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
                if n.category.find('女频') == -1:
                    continue
                self.log.info('search novel {}.{}'.format(n.title, n.author))
                novel = dict(n.items())
                try:
                    url = self.process(session, novel)
                except Exception as e:
                    url = ''
                    self.log.exception(e)
                state = 1 if url else -1
                update_stmt = ("UPDATE baidu_novel SET state=:state, "
                        "url=:url WHERE uuid=:uuid")
                session.execute(update_stmt, {'state': state, 'url': url, 
                        'uuid': n.uuid})
                session.commit()
                time.sleep(0.5)
                

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
            if title == novel['title'] and author == novel['author']:
                self.log.info('set novel url {}'.format(href))
                novel['url'] = href
                break
        if not novel['url']:
            self.log.warning('can not find novel')
            return ''
        self.log.info('update novel {}'.format(novel['title']))
        novel['uuid'] = get_uuid()
        stmt = ("INSERT IGNORE INTO biquga_novel(uuid, title, "
                    "url, cover, summary) VALUES (:uuid, :title, "
                    ":url, :cover, :summary) ON DUPLICATE KEY UPDATE "
                    "cover=:cover, summary=:summary")
        session.execute(stmt, novel)
        session.commit()
        return novel['url']


j = Job('/conf/zy/novel_crawler.conf')
j.run()
