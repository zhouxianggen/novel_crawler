import re
from novel_crawler.base_job import BaseJob
from novel_crawler.utils.func import get_uuid, open_url, get_dom_tree


class Job(BaseJob):
    INTERVAL = 3600

    def run(self):
        self.log.info('scan booklist')
        reo_bg = re.compile(r'background-image: url\((.+?)\)', re.I)
        for p in range(1, 22):
            url = 'https://www.hmba.vip/xiaoshuodaquan/page_{}.html'.format(p)
            self.log.info('scan page {}'.format(url))
            content = open_url(url)
            doc = get_dom_tree(content, url)
            books = []
            for a in doc.xpath('.//div[@class="bos1"]/ul/li/a'):
                href = a.attrib['href']
                img = a.xpath('./img')[0]
                cover = img.attrib['data-original']
                h3 = a.xpath('./h3')[0]
                title = h3.text
                self.log.info('get book {} {} {}'.format(title, href, cover))
                books.append((title, href, cover))
            self.log.info('insert {} books'.format(len(books)))
            with self.session_scope() as session:
                stmt = ("INSERT IGNORE INTO hmba_cartoon(uuid, title, "
                        "url, cover, state) VALUES (:uuid, :title, :url, "
                        ":cover, :state)")
                args = []
                for title, url, cover in books:
                    args.append({'uuid': get_uuid('n'), 'title': title, 
                            'url': url, 'cover': cover, 'state': 0})
                session.execute(stmt, args)
            self.log.info('finished')


j = Job('/conf/zy/novel_crawler.conf')
j.run()
