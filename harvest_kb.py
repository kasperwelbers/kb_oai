from sickle import Sickle
from lxml import etree
import re, requests, argparse, os, csv, sys, logging

csv.field_size_limit(sys.maxsize)
XML_NAMESPACES = {'xsi': "http://www.w3.org/2001/XMLSchema-instance",
                  'didl': "urn:mpeg:mpeg21:2002:02-DIDL-NS",
                  'ddd': "http://www.kb.nl/namespaces/ddd",
                  'dc': "http://purl.org/dc/elements/1.1/",
                  'dcterms': "http://purl.org/dc/terms/",
                  'dcmitype': "http://purl.org/dc/dcmitype/",
                  'dcx': "http://krait.kb.nl/coop/tel/handbook/telterms.html",
                  'didmodel': "urn:mpeg:mpeg21:2002:02-DIDMODEL-NS",
                  'srw_dc': "info:srw/schema/1/dc-v1.1"}

def create_or_append_csv(filepath, colnames, done_field):
    if os.path.isfile(filepath):
        r = csv.DictReader(open(filepath, 'r'))
        done_urls = [l[done_field] for l in r]
        f = open(filepath, 'a')
        w = csv.writer(f)
    else:
        done_urls = []
        f = open(filepath, 'w')
        w = csv.writer(f)
        w.writerow(colnames)
    return w, f, done_urls

class kbScraper():
    def __init__(self, api_key, folder):
        self.oai_url = 'http://services.kb.nl/mdo/oai/'
        if api_key: self.oai_url = self.oai_url + api_key
        print(self.oai_url)
        self.folder = folder

    def parse_source_meta(self, elem):
        meta_node = elem.find('.//srw_dc:dcx', XML_NAMESPACES)
        d = {}
        for metafield in meta_node.getchildren():
            key = metafield.tag.split('}')[-1]
            if key == 'title':
                d['publisher'] = metafield.text
            if key == 'isVersionOf':
                d['publisher_alt'] = metafield.text
            if key == 'identifier':
                ## there are 2 keys named identifier, that can be distinguished by a messy namespace attribute
                xsi_type = '{' + XML_NAMESPACES['xsi'] + '}type'
                if len(metafield.attrib) == 0:
                    d['issue_url'] = metafield.text
            if key == 'volume':
                d['volume_int'] = int(metafield.text)
            if key == 'issuenumber':
                d['issuenumber_int'] = int(metafield.text)
            if key == 'date':
                d['date'] = metafield.text
            if key == 'language':
                if not metafield.text == 'nl': return(None)
        return d


    def parse_article_body(self, id):
        ocr_url = "http://resolver.kb.nl/resolve?urn=" + id + ":ocr"
        ocr_xml = etree.parse(ocr_url)

        title = ocr_xml.find('.//title').text

        text = ''
        for e in ocr_xml.findall('.//p'):
            text = text + e.text

        ## title is stored as ocr_title for testing whether this is identical to title in meta
        return {'ocr_title': title, 'text': text}

    def parse_article_meta(self, elem):
        d = {}
        meta_node = elem.find('.//srw_dc:dcx', XML_NAMESPACES)
        for metafield in meta_node.getchildren():
            key = metafield.tag.split('}')[-1]
            if key == 'title':
                d['title'] = metafield.text
            if key == 'identifier':
                d['url'] = metafield.text

        pagenr = elem.find('.//dcx:article-part', XML_NAMESPACES)
        d['page_int'] = int(pagenr.attrib['pageid'].split(':p')[-1])
        return d

    def get_records(self, set, from_date, download=True):
        fname = os.path.join(self.folder, 'KB_' + set + '_' + from_date + '_RAW_RECORDS.csv')
        if download: self.download_records(fname, set, from_date)

        with open(fname, 'r') as csvfile:
            for l in csv.DictReader(csvfile):
                record_xml = etree.fromstring(l['record_xml'])
                yield record_xml

    def download_records(self, fname, set, from_date):
        logging.info('Downloading records (this can take a long time)')

        w, f, done_ids = create_or_append_csv(fname, colnames=['id', 'record_xml'], done_field='id')
        sickle = Sickle(self.oai_url)
        headers = sickle.ListIdentifiers(metadataPrefix='didl', ignore_deleted=True, set=set, **{"from": from_date})
        i = 0
        for h in headers:
            i += 1
            if i == 10: break  ## only for testing
            if i % 100 == 0: logging.info('\t' + i)
            if h.identifier in done_ids: continue
            record = sickle.GetRecord(identifier=h.identifier, metadataPrefix='didl')
            w.writerow([id, record.raw])

        f.close()

    def get_articles(self, record_xml, done_urls):
        logging.info('Parsing articles and downloading OCR text')

        ## list with data for newspaper, pages and articles
        top_node = record_xml.find('.//didl:DIDL/didl:Item', XML_NAMESPACES)
        top_list = top_node.getchildren()

        ## the source meta is in the first item
        source_meta = self.parse_source_meta(top_list[0])

        for elem in top_list[1:]:
            dc_id_attrib = '{' + XML_NAMESPACES['dc'] + '}identifier'
            id = elem.attrib[dc_id_attrib]

            ## for articles the id ends with ':a' followed by the index
            if re.search('a[0-9]+$', id):
                art_meta = self.parse_article_meta(elem)

                ## the done urls is only used to prevent downloading the ocr data
                if art_meta['url'] in done_urls: continue
                art_body = self.parse_article_body(id)

                article = {**source_meta, **art_meta, **art_body}
                yield article

    def scrape(self, set, from_date, download=True, done_urls=[]):
        fname = os.path.join(self.folder, 'KB_' + set + '_' + from_date + '.csv')
        cols = ['publisher', 'publisher_alt', 'date', 'volume_int',
                'issuenumber_int', 'issue_url', 'url', 'title', 'text']
        w, f, done_urls = create_or_append_csv(fname, cols, done_field='url')

        for record_xml in self.get_records(set, from_date, download):
            for a in self.get_articles(record_xml, done_urls):
                w.writerow([a['publisher'], a['publisher_alt'], a['date'], a['volume_int'],
                            a['issuenumber_int'], a['issue_url'], a['url'], a['title'], a['text']])
        f.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--api_key',  type=str, help='The API key (needed for data after 1945)', default=None)
    parser.add_argument('--path',  type=str, help='The path for storing the output. default is current directory', default=os.getcwd())
    parser.add_argument('--set',  type=str, help='The set of the collection. Default is DDD', default="DDD")
    parser.add_argument('--from_date', type=str, help='Optionally, a start date (YYYY-MM-DD) for when the articles were added to the KB collection (so NOT the article publication date)', default='2000-01-01')
    args = parser.parse_args()

    ## for debugging. Set to FALSE to only parse articles for which the raw record xml is already downloaded,
    ## to limit traffic. note that the ocr data will still be downloaded from KB, so it does not stop all traffic
    download=True

    s = kbScraper(args.api_key, args.path)
    s.scrape(set=args.set, from_date=args.from_date, download=download)

