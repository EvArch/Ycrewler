import re
import urllib.request
import urllib.parse
import sqlite3
import math
import time
import bs4
from cffi.backend_ctypes import xrange
from nltk.stem import PorterStemmer
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from RULES import stopwords



tokens = 0
documents = 0
terms = 0
chars = re.compile(r'\W+')

class Term():
    term_id = 0
    term_freq = 0
    docs = 0
    docs_id = {}


def split_chars(line):
    return chars.split(line)


def parse_toke(db, line) -> object:
    global documents
    global tokens
    global terms

    p = PorterStemmer()

    line = line.replace('\t', ' ')
    line = line.strip()

    l = split_chars(line)

    for elmt in l:
        elmt = elmt.replace('\n', '')
        lowerElmt = elmt.lower().strip()
        tokens += 1
        if len(lowerElmt) < 2:
            continue
        if (lowerElmt in stopwords):
            continue
        try:
            dummy = int(lowerElmt)
        except ValueError:
            stemword = lowerElmt
        else:
            continue
        lowerElmt = p.stem(stemword)
        if not (lowerElmt in db.keys()):
            terms += 1
            db[lowerElmt] = Term()
            db[lowerElmt].term_id = terms
            db[lowerElmt].docs_id = dict()
            db[lowerElmt].docs = 0
        if not (isinstance(db[lowerElmt], str)):
            if not (documents in db[lowerElmt].docs_id.keys()):
                db[lowerElmt].docs += 1
                db[lowerElmt].docs_id[documents] = 0
            db[lowerElmt].docs_id[documents] += 1
    return l


def store_index(db,cur):
    for k in db.keys():
        if not (isinstance(db[k], str)):
            cur.execute('insert into TermDictionary values (?,?)', (k, db[k].term_id))
            docfreq = db[k].docs
            ratio = float(documents) / float(docfreq)
            idf = math.log10(ratio)

            for i in db[k].docs_id.keys():
                term_freq = db[k].docs_id[i]
                tfidf = float(term_freq) * float(idf)
                if tfidf > 0:
                    cur.execute('insert into Posting values (?, ?, ?, ?, ?)',
                                (db[k].term_id, i, tfidf, docfreq, term_freq))



def crewling(seed, table_namen):
    global documents
    # ------------------------- pages db connection ------------------------
    con_pages = sqlite3.connect("webcrawler.db")
    con_pages.isolation_level = None
    cur_pages = con_pages.cursor()
    cur_pages.execute("create table if not exists {} (content text, url text)".format(table_namen))
    # ------------------------- indexer db connection ----------------------
    con = sqlite3.connect("indexer_part2.db")
    con.isolation_level = None
    cur = con.cursor()
    cur.execute("drop table if exists DocumentDictionary")
    cur.execute("drop index if exists idxDocumentDictionary")
    cur.execute("create table if not exists DocumentDictionary (DocumentName text, DocId int)")
    cur.execute("create index if not exists idxDocumentDictionary on DocumentDictionary (DocId)")
    # Term Dictionary Table
    cur.execute("drop table if exists TermDictionary")
    cur.execute("drop index if exists idxTermDictionary")
    cur.execute("create table if not exists TermDictionary (Term text, term_id int)")
    cur.execute("create index if not exists idxTermDictionary on TermDictionary (term_id)")
    # Postings Table
    cur.execute("drop table if exists Posting")
    cur.execute("drop index if exists idxPosting1")
    cur.execute("drop index if exists idxPosting2")
    cur.execute("create table if not exists Posting (term_id int, DocId int, tfidf real, docfreq int, term_freq int)")
    cur.execute("create index if not exists idxPosting1 on Posting (term_id)")
    cur.execute("create index if not exists idxPosting2 on Posting (Docid)")
    db = {'keys': 'djjdjdjdd', 'term_id': 'bac21', 'term': 'community'}
    t2 = time.localtime()
    print('Start Time: %.2d:%.2d' % (t2.tm_hour, t2.tm_min))

    crawled = ([])  # contains the list of pages that have already been crawled
    tocrawl = [seed]  # contains the queue of url's that will be crawled
    links_queue = 0  # counts the number of links in the queue to limit the depth of the crawl
    crawlcomplete = True  # Flat that will exit the while loop when the craw is finished

    while crawlcomplete:

        try:
            crawling = tocrawl.pop()
        except:
            crawlcomplete = False
            continue

        l = len(crawling)
        #print("L:%.2d" % l)
        ext = crawling[l - 4:l]
        if ext in ['.pdf', '.png', '.jpg', '.gif', '.asp']:
            crawled.append(crawling)
            continue

        url = urllib.parse.urlparse(crawling)
        try:
            response = urllib.request.urlopen(crawling)
            print("[+] {}".format(crawling))
            if response.getheader('Content-Language') == 'en':
                response = response.read()
                cur_pages.execute('insert into {} values (?,?)'.format(table_namen), (response, crawling))
            else:
                raise  Exception("the page must be english")
        except:
            print("[-] {}".format(crawling))
            continue

        soup = bs4.BeautifulSoup(response, "html.parser")
        tok = soup.findAll("p", text=re.compile("."))
        f_tok = [t.text for t in tok]
        f_tok = " ".join(f_tok)
        parse_toke(db, f_tok)
        documents += 1

        cur.execute("insert into DocumentDictionary values (?, ?)", (documents, crawling))

        if links_queue < 30:
            links = re.findall('''href=["'](.[^"']+)["']''', response.decode('utf-8'), re.I)
            for link in (links.pop(0) for _ in xrange(len(links))):
                if links_queue > 30:
                    break
                if link.startswith('/'):
                    link = 'http://' + url[1] + link
                elif link.startswith('#'):
                    link = 'http://' + url[1] + url[2] + link
                elif not link.startswith('http'):
                    link = 'http://' + url[1] + '/' + link
                if link not in crawled:
                    links_queue += 1
                    tocrawl.append(link)
        crawled.append(crawling)

    # print("Links_queue %i" % links_queue)

    t2 = time.localtime()
    #print('Indexing Complete, write to disk: %.2d:%.2d' % (t2.tm_hour, t2.tm_min))
    store_index(db,cur)
    con.commit()
    con.close()

def fin(seed):
    print("Finished ==========> {}".format(seed))
    t2 = time.localtime()
    print('End Time: %.2d:%.2d' % (t2.tm_hour, t2.tm_min))




if __name__ == '__main__':
    pool = ThreadPoolExecutor(max_workers=20)
    seeds_que = Queue()
    seeds = input("Enter URL to crawl more than one using (,): ")
    seed_list = seeds.split(',')
    for seed in seed_list:
        job = pool.submit(crewling, seed, seed.split('//')[1].split('.')[1])