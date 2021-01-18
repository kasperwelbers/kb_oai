from sickle import Sickle
from lxml import etree
import re, requests, argparse, os, csv, sys, logging, datetime
#import hashlib

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

def create_or_append_csv(filepath, colnames, done_field, max_datestamp=False):
    ds = datetime.datetime.strptime('2000-01-01T20:00:00.847Z', '%Y-%m-%dT%H:%M:%S.%fZ')
    done_urls = []
    if os.path.isfile(filepath):
        for l in csv.DictReader(open(filepath, 'r')):
            done_urls.append(l[done_field])
            if max_datestamp:
                ds = max(ds, datetime.datetime.strptime(l['kb_date'], '%Y-%m-%dT%H:%M:%S.%fZ'))
        f = open(filepath, 'a')
        w = csv.writer(f)
    else:
        done_urls = []
        f = open(filepath, 'w')
        w = csv.writer(f)
        w.writerow(colnames)
    if max_datestamp:
        return w, f, done_urls, ds
    else:
        return w, f, done_urls

class kbScraper():
    def __init__(self, api_key, folder):
        self.oai_url = 'http://services.kb.nl/mdo/oai/'
        if api_key: self.oai_url = self.oai_url + api_key
        self.folder = folder

    #def hash_id(self, set, from_date, to_date, publishers):
    #    hash_string = str(set) + str(from_date) + str(to_date) + '_'.join(publishers)
    #    return hashlib.sha1(hash_string.encode('UTF-8')).hexdigest()[:10]

    def parse_source_meta(self, elem):
        meta_node = elem.find('.//srw_dc:dcx', XML_NAMESPACES)
        d = {}
        for metafield in meta_node.getchildren():
            key = metafield.tag.split('}')[-1]
            if metafield.text is None:
                text = ''
            else:
                text = metafield.text.strip()
            

            if key == 'title':
                d['publisher'] = text
            if key == 'isVersionOf':
                d['publisher_alt'] = text
            if key == 'identifier':
                ## there are 2 keys named identifier, we need the one without attributes
                if len(metafield.attrib) == 0:
                    d['issue_url'] = text
            if key == 'volume':
                volume = text.split()[0] if text != '' else ''
                volume = re.sub('[\D].*','', volume)
                d['volume_int'] = int(volume) if volume != '' else ''
            if key == 'issuenumber':
                issue = text.split()[0] if text != '' else ''
                issue = re.sub('[\D].*','', issue)
                d['issuenumber_int'] = int(issue) if issue != '' else ''
            if key == 'date':
                d['date'] = text
        return d

    def create_index(self, set):
        index_fname = os.path.join(self.folder, 'KB_INDEX_' + set + '.csv')
        d = dict()
        if os.path.isfile(index_fname):
            with open(index_fname, 'r') as index_file:
                for l in csv.DictReader(index_file):
                    pub = l['publisher']
                    if not l['publisher_alt'] == 'MISSING' and not pub == l['publisher_alt']:
                        pub = pub + ' | ' + l['publisher_alt']
                    date = datetime.datetime.strptime(l['date'], '%Y-%m-%d')
                    if pub in d.keys():
                        if date < d[pub][0]: d[pub][0] = date
                        if date > d[pub][1]: d[pub][1] = date
                        d[pub][2] += 1
                    else:
                        d[pub] = [date,date,1]
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

    def select_publishers(self, pub_regex):
        index = self.create_index(set)
        publishers = [x for x in index.keys() if re.search(pub_regex, x)]
        if len(publishers) == 0:
            logging.error('No newspapers matched np_regex. Use --show_index to see available newspapers')
        logging.info('Selected newspapers')
        for p in publishers:
            logging.info('\t' + p)
        return publishers

    def get_records(self, set, from_date, to_date, pub_regex, download=True):
        #fname_id = self.hash_id(set, from_date, to_date, publishers) ## use hash for unique file per query
        fname = os.path.join(self.folder, 'KB_META_' + set + '.csv')

        if download:
            publishers = self.select_publishers(pub_regex)
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


    def build_index(self, set):
        fname = os.path.join(self.folder, 'KB_INDEX_' + set + '.csv')
        w, f, in_index, from_date = create_or_append_csv(fname, colnames = ['identifier','date','publisher','publisher_alt','kb_date'], done_field='identifier', max_datestamp=True)

        sickle = Sickle(self.oai_url)
        headers = sickle.ListIdentifiers(metadataPrefix='didl', ignore_deleted=True, set=set, **{"from": from_date})
        #logging.info(from_date)

        i = 0
        for h in headers:
            i += 1

            if h.identifier in in_index:
                if i % 1000 == 0:
                    logging.info('\tscanning index: ' + str(i))
                    # For some reason, KB doesn't always start from the from date, but several hours earlier, so we also need to check on duplicate ids
                    #logging.info('date is ' + h.datestamp)
                continue

            if i % 100 == 0: logging.info('\tupdating index: ' + str(i))
            record = sickle.GetRecord(identifier=h.identifier, metadataPrefix='didl')
            record_xml = etree.fromstring(record.raw)
            top_node = record_xml.find('.//didl:DIDL/didl:Item', XML_NAMESPACES)
            top_list = top_node.getchildren()
            source_meta = self.parse_source_meta(top_list[0])
            w.writerow([h.identifier,
                        source_meta.get('date'),
                        source_meta.get('publisher', 'MISSING'),
                        source_meta.get('publisher_alt', 'MISSING'),
                        h.datestamp])
        f.close()

    def download_records(self, fname, set, from_date, to_date, publishers):
        w, f, done_ids = create_or_append_csv(fname, colnames=['id', 'date','publisher', 'record_xml'], done_field='id')

        index_fname = os.path.join(self.folder, 'KB_INDEX_' + set + '.csv')
        with open(index_fname, 'r') as csvfile:
            i = 0
            for l in csv.DictReader(csvfile):

                i += 1
                #if i == 10: break  ## only for testing
                if i % 100 == 0: logging.info('\t' + str(i))

                date = datetime.datetime.strptime(l['date'], '%Y-%m-%d')
                if date >= from_date and date <= to_date and l['publisher'] in publishers:
                    if l['identifier'] in done_ids: continue
                    done_ids.append(l['identifier'])
                    record = sickle.GetRecord(identifier=l['identifier'], metadataPrefix='didl')
                    w.writerow([l['identifier'], date, l['publisher'], record.raw])



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

    def scrape(self, set, from_date, to_date, pub_regex, download=True, done_urls=[], build_index=False, show_index=False):
        if build_index:
            logging.info('Updating index (this can be turned off with --dont_update_index')
            self.build_index(set)

        if show_index:
            index = self.create_index(set)
            print('Newspapers in index:')
            for k in sorted(index.keys()):
                #if index[k][2] < 400: continue
                print('\t' + k + ' (' + str(index[k][2]) +  ')')
            publishers = [x + ' (' + str(index[x][2]) +  ')' for x in index.keys() if re.search(pub_regex, x)]
            if len(publishers) == 0:
                logging.error('\n\nNo newspapers matched np_regex. Use --show_index to see available newspapers')
            print('\n\nnp_regex would match:')
            for p in publishers:
                print('\t' + p)
            return None


        fname = os.path.join(self.folder, 'KB_ARTICLES_' + set + '.csv')
        cols = ['publisher', 'publisher_alt', 'date', 'volume_int',
                'issuenumber_int', 'issue_url', 'page_int', 'url', 'title', 'text']
        w, f, done_urls = create_or_append_csv(fname, cols, done_field='url')

        for record_xml in self.get_records(set, from_date, to_date, pub_regex, download):
            for a in self.get_articles(record_xml, done_urls):
                w.writerow([a.get(col) for col in cols])
        f.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('np_regex', type=str, help='regular expression for selectiong which newspapers to collect. (use --show_index for overview of available newspapers)')
    parser.add_argument('from_date', type=str, help='The date (YYYY-MM-DD) from which to collect the articles')
    parser.add_argument('--to_date', type=str, help='Optionally, also the maximum date. If omitted, will get all untill most recent date')
    parser.add_argument('--api_key',  type=str, help='The API key (needed for data after 1945)', default=None)
    parser.add_argument('--path',  type=str, help='The path for storing the output. default is current directory', default=os.getcwd())
    parser.add_argument('--set',  type=str, help='The set of the collection. Default is DDD', default="DDD")
    parser.add_argument('--no_download', action='store_true', help='For debugging. If true, only parse articles for which raw record xml is already downloaded, to limit KB traffic')
    parser.add_argument('--show_index', action='store_true', help='Show all newspapers and dates in current index')
    parser.add_argument('--dont_update_index', action='store_true', help='Use current index, without checking whether it should be updated')
    args = parser.parse_args()


    from_date = datetime.datetime.strptime(args.from_date, '%Y-%m-%d')
    if args.to_date:
        to_date = datetime.datetime.strptime(args.to_date, '%Y-%m-%d')
    else:
        to_date = datetime.datetime.strptime('2050-01-01', '%Y-%m-%d')

    s = kbScraper(args.api_key, args.path)
    s.scrape(set=args.set, from_date=from_date, to_date = to_date, pub_regex=args.np_regex, download=not args.no_download, build_index=not args.dont_update_index, show_index=args.show_index)

