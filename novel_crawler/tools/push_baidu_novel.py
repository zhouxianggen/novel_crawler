import time
import json
import random
from datetime import datetime
from configparser import ConfigParser
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker, scoped_session


def main():
    cfg = ConfigParser()
    cfg.read('/conf/zy/novel_crawler.conf')
    CONN = cfg.get('mysql', 'conn')
    engine = create_engine(CONN, pool_size=100, pool_recycle=3600)
    Session = scoped_session(sessionmaker(bind=engine))
    session = Session()
    
    ONLINE_CONN = cfg.get('online', 'conn')
    online_engine = create_engine(ONLINE_CONN, pool_size=100, pool_recycle=3600)
    online_Session = scoped_session(sessionmaker(bind=online_engine))
    online_session = online_Session()
    r = online_session.execute("SELECT max(id) FROM dd_anime").first()
    anid = (r[0] or 0) + 1
    print('next anid is {}'.format(anid))

    anime_ks = ['id', 'btype', 'title', 'author', 'coverpic', 'infopic', 
            'remark', 'allchapter', 'create_time', 'areas', 'issex']
    chapter_ks = ['anid', 'title', 'chaps', 'info', 'create_time']
    for n in session.execute('SELECT * FROM baidu_novel WHERE state=1'
            ).fetchall():
        print('获取 {} 笔趣阁uuid'.format(n.title))
        bn = session.execute("SELECT * FROM biquga_novel WHERE url='{}'"
                .format(n.url)).first()
        if not bn:
            print('..没有对应的笔趣阁uuid')
            continue
        print('..获取小说章节')
        chapters = []
        max_seq = 0
        skip = 0
        for c in session.execute(
                'SELECT * FROM biquga_chapter WHERE novel_id="{}"'.format(
                bn.uuid)).fetchall():
            if not c.seq or c.state == 0 or not c.paragraphs:
                skip = 1
                break
            max_seq = max(max_seq, c.seq)
            chapters.append(c)
        if skip or not chapters:
            print('..没有找到小说章节')
            continue
        print('..插入小说')
        stmt = text("INSERT IGNORE INTO dd_anime ({}) VALUES ({})".format(
                ', '.join(anime_ks), 
                ', '.join([':{}'.format(k) for k in anime_ks])))
        areas = random.randint(1, 4)
        issex = int(n.category.find('女频') != -1)
        args = {'id':anid, 'btype': 2, 'title': n.title, 'author': n.author, 
                'coverpic': n.cover, 'infopic': n.cover, 'areas': areas, 
                'remark': n.summary, 'allchapter': max_seq, 'issex': issex,
                'create_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        online_session.execute(stmt, args)
        stmt = text("""INSERT IGNORE INTO dd_anime_chapter ({}) 
                VALUES ({})""".format(', '.join(chapter_ks), 
                ', '.join([':{}'.format(k) for k in chapter_ks])))
        for c in chapters:
            lst = json.loads(c.paragraphs)
            if lst and lst[0]['text'].find('天才一秒钟记住本网站') != -1:
                lst = lst[1:]
            info = ''.join(['<p style="text-indent: 2em;">{}</p>'.format(
                    p['text']) for p in lst])
            args = {'anid': anid, 'title': c.title, 'chaps': c.seq, 
                    'info': info, 'create_time': int(time.time())}
            online_session.execute(stmt, args)
        online_session.commit()
        anid += 1


if __name__ == '__main__':
    main()

