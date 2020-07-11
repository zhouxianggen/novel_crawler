import re
import time
from datetime import datetime
from novel_crawler.base_job import BaseJob
from novel_crawler.utils.func import (get_uuid, open_url, get_dom_tree, 
        get_xpath_text, parse_chapter_title)
from novel_crawler.model.define import *


class Job(BaseJob):
    INTERVAL = 3600

    def run(self):
        self.log.info('get baidu novels')
        novels = set()
        with self.session_scope() as session:
            for r in session.execute("SELECt * FROM baidu_novel").fetchall():
                if r.url:
                    novels.add(r.url)
        self.log.info('get {} novels'.format(len(novels)))
        with self.session_scope() as session:
            stmt = ("SELECT uuid, url FROM biquga_novel "
                    "WHERE finished=0 ORDER BY scan_time ASC LIMIT 10000")
            for t in session.execute(stmt).fetchall():
                novel_id, url = t
                if url not in novels:
                    continue
                r = session.execute("SELECT max(seq) FROM biquga_chapter "
                        "WHERE novel_id='{}'".format(novel_id)).first()
                if r[0]:
                    continue
                self.log.info('scan chapters {}'.format(url))
                try:
                    self.process(session, novel_id, url)
                except Exception as e:
                    self.log.exception(e)
                time.sleep(0.5)
                

    def process(self, session, novel_id, url):
        content = open_url(url)
        doc = get_dom_tree(content, url)
        author = get_xpath_text(doc, '//*[@id="info"]/p[1]')
        author = author.strip('作者：').strip()
        category = get_xpath_text(doc, '//*[@id="info"]/p[2]')
        category = category.strip('分类：').strip()
        summary = get_xpath_text(doc, '//*[@id="intro"]')
        img = doc.xpath('//*[@id="fmimg"]/img')[0]
        cover = img.attrib.get('src', '')
        chapters = []
        for a in doc.xpath('.//div[@class="box_con"]/div/dl/dd/a'):
            href = a.attrib.get('href', '')
            seq = len(chapters) + 1
            title = a.text.strip()
            chapters.append((seq, title, href))
        self.log.info('update novel info')
        stmt = ("UPDATE biquga_novel SET author=:author, "
                "category=:category, summary=:summary, "
                "scan_time=:scan_time, cover=:cover "
                " WHERE uuid=:uuid")
        args = {'uuid': novel_id, 'author': author, 
                'category': category,  'scan_time': datetime.now(), 
                'summary': summary, 'cover': cover}
        session.execute(stmt, args)
        self.log.info('insert {} chapters'.format(len(chapters)))
        stmt = ("INSERT IGNORE INTO biquga_chapter(uuid, novel_id, seq, "
                "title, url, state) VALUES (:uuid, :novel_id, :seq, "
                ":title, :url, :state)")
        args = []
        for seq, title, url in chapters:
            args.append({'uuid': get_uuid('c'), 'novel_id': novel_id, 
                    'seq': seq, 'title': title, 'url': url, 'state': 0})
        session.execute(stmt, args)
        session.commit()


j = Job('/conf/zy/novel_crawler.conf')
j.run()
