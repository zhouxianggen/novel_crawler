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
        self.log.info('scan book chapter')
        with self.session_scope() as session:
            stmt = ("SELECT uuid, url FROM hmba_cartoon "
                    "WHERE finished=0 ORDER BY scan_time ASC")
            for t in session.execute(stmt):
                novel_id, url = t
                self.log.info('scan chapters {}'.format(url))
                try:
                    self.process(session, novel_id, url)
                except Exception as e:
                    self.log.exception(e)
                time.sleep(0.5)
                

    def process(self, session, novel_id, url):
        content = open_url(url, timeout=10)
        doc = get_dom_tree(content, url)
        summary = get_xpath_text(doc, '//*[@id="intro"]')
        chapters = []
        for idx,a in enumerate(doc.xpath(
                './/div[@id="list"]/dl/dd/a')):
            href = a.attrib.get('href', '')
            title = a.text
            seq = idx+1 
            chapters.append((seq, title, href))
        self.log.info('update book info')
        stmt = ("UPDATE hmba_cartoon SET "
                "summary=:summary, scan_time=:scan_time "
                " WHERE uuid=:uuid")
        args = {'uuid': novel_id, 
                'scan_time': datetime.now(), 'summary': summary}
        session.execute(stmt, args)
        self.log.info('insert {} chapters'.format(len(chapters)))
        stmt = ("INSERT IGNORE INTO hmba_chapter(uuid, novel_id, seq, "
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
