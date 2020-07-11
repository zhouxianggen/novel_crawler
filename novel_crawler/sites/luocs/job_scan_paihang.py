import re
from novel_crawler.base_job import BaseJob
from novel_crawler.utils.func import get_uuid, open_url, get_dom_tree


class Job(BaseJob):
    INTERVAL = 3600

    def run(self):
        self.log.info('scan novels')
        reo = re.compile('https://www\.luocs\.cn/\d+/\d+/', re.I)
        url = 'https://www.luocs.cn/paihang.html'
        content = open_url(url)
        doc = get_dom_tree(content, url)
        novels = []
        for a in doc.xpath('.//ul/li/a'):
            href = a.attrib.get('href', '')
            title = a.text or ''
            if reo.match(href):
                novels.append((title.strip(), href))
        self.log.info('insert {} novels'.format(len(novels)))
        with self.session_scope() as session:
            stmt = ("INSERT IGNORE INTO luocs_novel(uuid, title, "
                    "url, state) VALUES (:uuid, :title, :url, :state)")
            args = []
            for title, url in novels:
                args.append({'uuid': get_uuid('n'), 'title': title, 
                        'url': url, 'state': 0})
            session.execute(stmt, args)
        self.log.info('finished')


j = Job('/conf/zy/novel_crawler.conf')
j.run()
