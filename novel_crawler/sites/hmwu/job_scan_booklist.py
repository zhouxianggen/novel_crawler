import re
from novel_crawler.base_job import BaseJob
from novel_crawler.utils.func import get_uuid, open_url, get_dom_tree


class Job(BaseJob):
    INTERVAL = 3600

    def run(self):
        self.log.info('scan booklist')
        reo_bg = re.compile(r'background-image: url\((.+?)\)', re.I)
        for p in range(1, 17):
            self.log.info('scan page {}'.format(p))
            url = 'https://www.hmwu.cc/booklist?page={}'.format(p)
            content = open_url(url)
            doc = get_dom_tree(content, url)
            books = []
            for div in doc.xpath('.//li/div[@class="mh-item"]'):
                a = div.xpath('./a')[0]
                href = a.attrib['href']
                title = a.attrib['title']
                p = a.xpath('./p')[0]
                m = reo_bg.search(p.attrib['style'])
                cover = m.group(1)
                self.log.info('get book {} {} {}'.format(title, href, cover))
                books.append((title, href, cover))
            self.log.info('insert {} books'.format(len(books)))
            with self.session_scope() as session:
                stmt = ("INSERT IGNORE INTO hmwu_cartoon(uuid, title, "
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
