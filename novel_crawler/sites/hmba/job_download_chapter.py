import re
import json
import time
from novel_crawler.base_job import BaseJob
from novel_crawler.utils.func import (open_url, get_dom_tree, get_xpath_text)
from novel_crawler.model.define import *


class Job(BaseJob):
    INTERVAL = 3600

    def run(self):
        self.log.info('scan download chapter')
        with self.session_scope() as session:
            stmt = ("SELECT uuid, url FROM hmba_chapter "
                    "WHERE state=0 ORDER BY id DESC")
            for t in session.execute(stmt):
                uuid, url = t
                self.log.info('download chapter {}'.format(url))
                try:
                    self.process(session, uuid, url)
                except Exception as e:
                    self.log.exception(e)
                time.sleep(1.1)
    

    def process(self, session, uuid, url):
        self.log.info('scan chapters {}'.format(url))
        content = open_url(url)
        doc = get_dom_tree(content, url)
        ps = []
        for img in doc.xpath('.//div[@id="content"]/p/img'):
            href = img.attrib['src']
            ps.append({"img": href})
        ps = json.dumps(ps, ensure_ascii=False)
        self.log.info('update chapter paragraphs')
        stmt = ("UPDATE hmba_chapter SET paragraphs=:paragraphs, "
                "state=:state WHERE uuid=:uuid")
        args = {'uuid': uuid, 'paragraphs': ps, 'state': 1}
        session.execute(stmt, args)
        session.commit()


j = Job('/conf/zy/novel_crawler.conf')
j.run()
