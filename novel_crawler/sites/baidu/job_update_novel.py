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
            stmt = "SELECT * FROM baidu_novel"
            for n in session.execute(stmt).fetchall():
                if not n.url:
                    continue
                self.log.info('update novel {}.{} {}'.format(n.title, 
                        n.author, n.url))
                novel = dict(n.items())
                novel['uuid'] = get_uuid()
                stmt = ("INSERT IGNORE INTO biquga_novel(uuid, title, "
                            "url, cover, summary) VALUES (:uuid, :title, "
                            ":url, :cover, :summary) ON DUPLICATE KEY UPDATE "
                            "cover=:cover, summary=:summary")
                session.execute(stmt, novel)
                session.commit()


j = Job('/conf/zy/novel_crawler.conf')
j.run()
