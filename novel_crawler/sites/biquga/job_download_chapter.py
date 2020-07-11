import re
import json
import time
from novel_crawler.base_job import BaseJob
from novel_crawler.utils.func import (open_url, get_dom_tree, get_xpath_text)
from novel_crawler.model.define import *


class Job(BaseJob):
    INTERVAL = 3600

    def run(self):
        with self.session_scope() as session:
            stmt = ("SELECT uuid, url FROM biquga_chapter "
                    "WHERE state=0 ORDER BY id DESC LIMIT 1000000")
            for t in session.execute(stmt).fetchall():
                uuid, url = t
                try:
                    self.process(session, uuid, url)
                except Exception as e:
                    self.log.exception(e)
                #time.sleep(0.1)
    

    def process(self, session, uuid, url):
        self.log.info('download chapter {}'.format(url))
        content = open_url(url)
        doc = get_dom_tree(content.decode('utf8'), url)
        ps = []
        for p in doc.xpath('.//div[@name="content"]/p'):
            if p.text.strip():
                ps.append({"text": p.text.strip()})
        ps = json.dumps(ps, ensure_ascii=False)
        self.log.info('update chapter paragraphs')
        stmt = ("UPDATE biquga_chapter SET paragraphs=:paragraphs, "
                "state=:state WHERE uuid=:uuid")
        args = {'uuid': uuid, 'paragraphs': ps, 'state': 1}
        session.execute(stmt, args)
        session.commit()


j = Job('/conf/zy/novel_crawler.conf')
j.run()
