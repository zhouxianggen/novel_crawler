import re
from novel_crawler.base_job import BaseJob
from novel_crawler.utils.func import get_uuid, open_url, get_dom_tree


class Job(BaseJob):
    INTERVAL = 3600

    def run(self):
        self.log.info('scan novels')
        reo = re.compile('https://www\.luocs\.cn/\d+/\d+/', re.I)
        url = 'https://www.biquga.com/sort/1/1.html'
        content = open_url(url)
        doc = get_dom_tree(content, url)
        for a in doc.xpath('.//div[@class="nav"]/ul/li/a')[1:-2]:
            novels = []
            cate_url = a.attrib.get('href', '')
            cate = a.text.strip()
            self.log.info('scan cate {}'.format(cate))
            cate_content = open_url(cate_url)
            cate_doc = get_dom_tree(cate_content, cate_url)
            for na in cate_doc.xpath('.//div[@class="ll"]/div/dl/dt/a'):
                novels.append({'url': na.attrib.get('href', ''), 
                        'title': na.text.strip(), 'uuid': get_uuid()})
            for na in cate_doc.xpath('.//div[@class="r"]/ul/li/span/a'):
                novels.append({'url': na.attrib.get('href', ''), 
                        'title': na.text.strip(), 'uuid': get_uuid()})
            for na in cate_doc.xpath('.//div[@class="l"]/ul/li/span[@class="s2"]/a'):
                novels.append({'url': na.attrib.get('href', ''), 
                        'title': na.text.strip(), 'uuid': get_uuid()})
            self.log.info('insert {} novels'.format(len(novels)))
            with self.session_scope() as session:
                stmt = ("INSERT IGNORE INTO biquga_novel(uuid, title, "
                        "url) VALUES (:uuid, :title, :url)")
                session.execute(stmt, novels)
        self.log.info('finished')


j = Job('/conf/zy/novel_crawler.conf')
j.run()
