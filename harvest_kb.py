from sickle import Sickle
from lxml import etree
import re, requests, argparse, os, csv, sys, logging, hashlib, datetime

logging.basicConfig(level=logging.INFO)
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
        self.folder = folder

    def hash_id(self, set, from_date, to_date, publishers):
        hash_string = str(set) + str(from_date) + str(to_date) + '_'.join(publishers)
        return hashlib.sha1(hash_string.encode('UTF-8')).hexdigest()[:10]

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
                ## there are 2 keys named identifier, we need the one without attributes
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
            if e.text: text = text + e.text

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

    def get_records(self, set, from_date, to_date, publishers, download=True):
        fname_id = self.hash_id(set, from_date, to_date, publishers) ## use hash for unique file per query
        fname = os.path.join(self.folder, 'KB_' + fname_id + '_RAW_RECORDS.csv')

        if download:
            logging.info('Downloading meta data')
            self.download_records(fname, set, from_date, to_date, publishers)

        logging.info('Parsing metadata and downloading OCR texts')
        with open(fname, 'r') as csvfile:
            i = 0
            for l in csv.DictReader(csvfile):
                i += 1
                if i % 100 == 0: logging.info('\t' + str(i))
                if l['selected'] == 1:
                    record_xml = etree.fromstring(l['record_xml'])
                    yield record_xml




    def download_records(self, fname, set, from_date, to_date, publishers):
        w, f, done_ids = create_or_append_csv(fname, colnames=['id', 'date','publisher', 'selected', 'record_xml'], done_field='id')
        sickle = Sickle(self.oai_url)
        headers = sickle.ListIdentifiers(metadataPrefix='didl', ignore_deleted=True, set=set, **{"from": from_date})

        print(done_ids)

        i = 0
        for h in headers:
            i += 1
            #if i == 10: break  ## only for testing
            if i % 100 == 0: logging.info('\t' + str(i))
            if h.identifier in done_ids: continue
            record = sickle.GetRecord(identifier=h.identifier, metadataPrefix='didl')

            ## get newspaper / issue meta
            record_xml = etree.fromstring(record.raw)
            top_node = record_xml.find('.//didl:DIDL/didl:Item', XML_NAMESPACES)
            top_list = top_node.getchildren()
            source_meta = self.parse_source_meta(top_list[0])
            date = datetime.datetime.strptime(source_meta['date'], '%Y-%m-%d')
            publisher = source_meta.get('publisher_alt', 'MISSING')

            if date > from_date and date < to_date and publisher in publishers:
                w.writerow([h.identifier, date, publisher, 1, record.raw])
            else:
                w.writerow([h.identifier, date, publisher, 0, None])

        f.close()

    def get_articles(self, record_xml, done_urls):
        #logging.info('Parsing articles and downloading OCR text')

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

    def scrape(self, set, from_date, to_date, publishers, download=True, done_urls=[]):
        fname_id = self.hash_id(set, from_date, to_date, publishers) ## use hash for unique file per query
        fname = os.path.join(self.folder, 'KB_' + fname_id + '.csv')
        cols = ['publisher', 'publisher_alt', 'date', 'volume_int',
                'issuenumber_int', 'issue_url', 'page_int', 'url', 'title', 'text']
        w, f, done_urls = create_or_append_csv(fname, cols, done_field='url')

        for record_xml in self.get_records(set, from_date, to_date, publishers, download):
            for a in self.get_articles(record_xml, done_urls):
                w.writerow([a.get(col) for col in cols])
        f.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--api_key',  type=str, help='The API key (needed for data after 1945)', default=None)
    parser.add_argument('--path',  type=str, help='The path for storing the output. default is current directory', default=os.getcwd())
    parser.add_argument('--set',  type=str, help='The set of the collection. Default is DDD', default="DDD")
    parser.add_argument('--no_download', action='store_true', help='For debugging. If true, only parse articles for which raw record xml is already downloaded, to limit KB traffic')
    args = parser.parse_args()

    ## we can't search on publisher or date, but we will only store the raw data, collect the ocr texts and parse the metadata for
    ## the following publishers and date window
    publishers = ['NRC Handelsblad', 'De Telegraaf', 'Algemeen Dagblad', 'Trouw', 'De Volkskrant']
    from_date = datetime.datetime.strptime('1945-01-01', '%Y-%m-%d')
    to_date = datetime.datetime.strptime('1990-01-01', '%Y-%m-%d')

    s = kbScraper(args.api_key, args.path)
    s.scrape(set=args.set, from_date=from_date, to_date = to_date, publishers=publishers, download=not args.no_download)

