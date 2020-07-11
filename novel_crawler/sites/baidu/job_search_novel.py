import re
import time
from datetime import datetime
from novel_crawler.base_job import BaseJob
from novel_crawler.utils.func import (get_uuid, open_url, post, 
        get_dom_tree, get_xpath_text, parse_chapter_title)
from novel_crawler.model.define import *


class Job(BaseJob):
    INTERVAL = 3600

    def run(self):
        with self.session_scope() as session:
            stmt = ("SELECT * FROM baidu_novel "
                    "WHERE state=0 LIMIT 100000")
            for n in session.execute(stmt):
                self.log.info('search novel {}.{}'.format(n.title, n.author))
                if n.category.find('女频') == -1:
                    continue
                novel = dict(n.items())
                try:
                    self.process(session, novel)
                except Exception as e:
                    self.log.exception(e)
                time.sleep(1.1)
                

    def process(self, session, novel):
        url = 'https://www.biquga.com/search/'
        params = {'searchkey': novel['title'], 'Submit': '搜 索'}
        content = post(url, params=params)
        doc = get_dom_tree(content, url)
        for li in doc.xpath('.//div[@class="novelslist"]/ul/li')[1:]:
            a = li.xpath('./span[@class="s2"]/a')[0]
            href = a.attrib.get('href', '')
            title = a.text.strip()
            span = li.xpath('./span[@class="s4"]')[0]
            author = span.text.strip()
            #if title == novel['title'] and author == novel['author']:
            if title == novel['title']:
                self.log.info('set novel url {}'.format(href))
                novel['url'] = href
                break
        if not novel['url']:
            self.log.warning('can not find novel')
            return
        self.log.info('update novel {}'.format(novel['title']))
        stmt = "UPDATE baidu_novel SET url=:url, state=1 WHERE uuid=:uuid"
        session.execute(stmt, {'url': novel['url'], 'uuid': novel['uuid']})
        session.commit()


j = Job('/conf/zy/novel_crawler.conf')
j.run()
