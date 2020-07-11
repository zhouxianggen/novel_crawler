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
            stmt = ("SELECT uuid, cover FROM biquga_novel")
            for t in session.execute(stmt):
                uuid, cover = t
                if cover.find('baidu.com') != -1:
                    self.log.info('update {}'.format(uuid))
                    update_stmt = ("UPDATE biquga_chapter SET state=10 "
                            "WHERE novel_id='{}' AND state=0").format(uuid)
                    session.execute(update_stmt)
    
   
j = Job('/conf/zy/novel_crawler.conf')
j.run()
