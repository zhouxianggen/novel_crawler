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
        self.log.info('scan novel chapter')
        with self.session_scope() as session:
            stmt = ("SELECT uuid, url FROM luocs_novel "
                    "WHERE finished=0 ORDER BY scan_time ASC LIMIT 200")
            for t in session.execute(stmt):
                novel_id, url = t
                self.log.info('scan chapters {}'.format(url))
                try:
                    self.process(session, novel_id, url)
                except Exception as e:
                    self.log.exception(e)
                time.sleep(1.1)
                

    def process(self, session, novel_id, url):
        content = open_url(url)
        doc = get_dom_tree(content, url)
        author = get_xpath_text(doc, '//*[@id="maininfo"]/div[1]/h3/a')
        txt = get_xpath_text(doc, '//*[@id="maininfo"]/div[1]/p[1]')
        t = txt.split('：')
        category = t[1] if len(t) == 2 else ''
        txt = get_xpath_text(doc, '//*[@id="maininfo"]/div[1]/p[3]')
        t = txt.split('：')
        status = t[1] if len(t) == 2 else ''
        finished = 1 if status == '已完成' else 0
        summary = get_xpath_text(doc, '//*[@id="intro"]/p')
        chapters = []
        for a in doc.xpath('.//ul[@id="chapterlist"]/li/a'):
            href = a.attrib.get('href', '')
            seq, title = parse_chapter_title(a.text)
            chapters.append((seq, title, href))
        self.log.info('update novel info')
        stmt = ("UPDATE luocs_novel SET author=:author, "
                "category=:category, finished=:finished, "
                "summary=:summary, scan_time=:scan_time "
                " WHERE uuid=:uuid")
        args = {'uuid': novel_id, 'author': author, 
                'category': category, 'finished': finished, 
                'scan_time': datetime.now(), 'summary': summary}
        session.execute(stmt, args)
        self.log.info('insert {} chapters'.format(len(chapters)))
        stmt = ("INSERT IGNORE INTO luocs_chapter(uuid, novel_id, seq, "
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
