import time
import json
from novel_crawler.base_job import BaseJob
from novel_crawler.utils.func import get_uuid, open_url, get_dom_tree


class Job(BaseJob):
    INTERVAL = 3600

    def run(self):
        self.log.info('scan novels')
        url = 'http://dushu.baidu.com/api/getCateData'
        content = open_url(url)
        resp = json.loads(content)
        for data in resp['data']['categoryData']:
            for cate in data['cateList']:
                for page in range(1, 100):
                    cate_url = ('http://dushu.baidu.com/api/getCateDetail?'
                            'channel={}&cate2={}&query={}&count=20&page={}'
                            ).format(data['queryParams'][0]['channel'], 
                            cate['text'], cate['text'], page)
                    self.log.info(cate_url)
                    cate_resp = json.loads(open_url(cate_url))
                    novels = []
                    for novel in cate_resp['data']['novelList']:
                        novels.append({'uuid': novel['bookId'], 
                                'title': novel['title'], 
                                'author': novel['author'], 
                                'cover': novel['cover'], 
                                'summary': novel['description'], 
                                'finished': novel['status'] == '完结', 
                                'category': novel['category']})
                    self.log.info('insert {} novels'.format(len(novels)))
                    if not novels:
                        break
                    with self.session_scope() as session:
                        ks = ['uuid', 'title', 'author', 'cover', 'summary', 
                                'finished', 'category']
                        stmt = ("INSERT IGNORE INTO baidu_novel ({}) "
                                "VALUES ({})").format(', '.join(ks), 
                                ', '.join([':{}'.format(k) for k in ks]))
                        session.execute(stmt, novels)
                    if not cate_resp['data']['hasMore']:
                        break
                    time.sleep(1)


j = Job('/conf/zy/novel_crawler.conf')
j.run()
