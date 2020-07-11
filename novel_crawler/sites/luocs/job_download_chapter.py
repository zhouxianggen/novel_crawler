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
            stmt = ("SELECT uuid, url FROM luocs_chapter "
                    "WHERE state=0 ORDER BY id DESC LIMIT 100000")
            for t in session.execute(stmt):
                uuid, url = t
                try:
                    self.process(session, uuid, url)
                except Exception as e:
                    self.log.exception(e)
                time.sleep(0.4)
    

    def process(self, session, uuid, url):
        self.log.info('download chapter {}'.format(url))
        content = open_url(url).decode('gbk')
        m = re.search(r'<div id="book_text">(.+?)</div>', content, 
                re.I|re.S)
        if not m:
            self.log.error('can not find content')
            ps, state = '', -1
        else:
            state = 1
            text = m.group(1)
            text = text.replace('&nbsp;', ' ')
            ps = []
            for x in text.split('<br />'):
                x = x.strip()
                if x:
                    ps.append({"text": x})
            if ps:
                i = ps[-1]["text"].find('记住本站网址')
                if i != -1:
                    ps[-1]["text"] = ps[-1]["text"][:i]
        ps = json.dumps(ps, ensure_ascii=False)
        self.log.info('update chapter paragraphs')
        stmt = ("UPDATE luocs_chapter SET paragraphs=:paragraphs, "
                "state=:state WHERE uuid=:uuid")
        args = {'uuid': uuid, 'paragraphs': ps, 'state': state}
        session.execute(stmt, args)
        session.commit()


j = Job('/conf/zy/novel_crawler.conf')
j.run()
