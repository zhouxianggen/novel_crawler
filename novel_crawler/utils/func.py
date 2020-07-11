import re
import json
import uuid
import logging
from lxml import html
from lxml import etree
from time import monotonic as now
import requests


def get_uuid(prefix=''):
    return prefix + str(uuid.uuid4()).replace('-', '')


def get_logger(name, level=logging.DEBUG):
    log = logging.getLogger(name)
    log.setLevel(level)
    log.propagate = False
    if not log.handlers:
        fmt = ('[%(name)-24s %(threadName)-14s %(levelname)-8s '
                '%(asctime)s] %(message)s')
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(fmt))
        log.addHandler(handler)
    return log


def open_url(url, headers={}, timeout=5):
    default_headers = {
            'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/81.0.4044.138 Safari/537.36'), 
            'Accept': 'text/html,application/xhtml+xml,application/xml;'
            }
    if not headers:
        headers = default_headers
    r = requests.get(url, headers=headers, timeout=timeout)
    assert r.status_code == 200
    return r.content
 

def post(url, params={}, headers={}, timeout=5):
    default_headers = {
            'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/81.0.4044.138 Safari/537.36'), 
            'Accept': 'text/html,application/xhtml+xml,application/xml;'
            }
    if not headers:
        headers = default_headers
    r = requests.post(url, params=params, headers=headers, 
            timeout=timeout)
    assert r.status_code == 200
    return r.content
    

def get_dom_tree(content, url=''):
    doc = html.fromstring(content)
    if url:
        doc.make_links_absolute(url)
    return doc.getroottree()


def get_xpath_text(doc, xpath, default=''):
    a = doc.xpath(xpath)
    text = a[0].text if a else None
    r = text or default
    return r.strip()


def took(func):
    def wrapper(*args, **kwargs):
        obj = args[0]
        start = now()
        r = func(*args, **kwargs)
        ms = (now() - start) * 1000
        obj.log.debug('%s.%s took %.2f ms' % ( 
                obj.__class__.__name__, func.__name__, ms)) 
        return r
    return wrapper


def parse_chapter_title(s):
    s = s.strip()
    m = re.match(r'第(.+?)章\s+(.+)$', s)
    if not m:
        return (0, s)
    seq = m.group(1).strip()
    title = m.group(2).strip()
    if seq.isdigit():
        return (int(seq), title)
    i, b, n = len(seq) - 1, 1, 0
    m = {'一': 1, '二': 2, '三': 3, '四': 4, '五': 5, 
            '六': 6, '七': 7, '八': 8, '九': 9}
    while i >= 0:
        c = seq[i]
        if c in m:
            n += m[c] * b
            b = 1
        elif c == '十':
            b = 10
        elif c == '百':
            b = 100
        elif c == '千':
            b = 1000
        elif c == '万':
            b = 10000
        elif c == '零':
            pass
        else:
            return (0, title) 
        i -= 1
    if b != 1:
        n += b
    return (n, title)

