import time
import json
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
    anid = r[0] + 1
    print('next anid is {}'.format(anid))

    anime_ks = ['id', 'btype', 'title', 'author', 'coverpic', 'infopic', 
            'remark', 'allchapter', 'create_time']
    chapter_ks = ['anid', 'title', 'chaps', 'info', 'create_time']
    for n in session.execute('SELECT * FROM luocs_novel').fetchall():
        chapters = []
        max_seq = 0
        skip = 0
        for c in session.execute(
                'SELECT * FROM luocs_chapter WHERE novel_id="{}"'.format(
                n.uuid)).fetchall():
            if not c.seq or c.state == 0:
                skip = 1
                break
            max_seq = max(max_seq, c.seq)
            chapters.append(c)
        if skip:
            continue
        print('insert novel {}'.format(n.title))
        stmt = text("INSERT IGNORE INTO dd_anime ({}) VALUES ({})".format(
                ', '.join(anime_ks), 
                ', '.join([':{}'.format(k) for k in anime_ks])))
        args = {'id':anid, 'btype': 2, 'title': n.title, 'author': n.author, 
                'coverpic': n.cover, 'infopic': n.cover, 
                'remark': n.summary, 'allchapter': max_seq, 
                'create_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        online_session.execute(stmt, args)
        stmt = text("""INSERT IGNORE INTO dd_anime_chapter ({}) 
                VALUES ({})""".format(', '.join(chapter_ks), 
                ', '.join([':{}'.format(k) for k in chapter_ks])))
        for c in chapters:
            lst = json.loads(c.paragraphs)
            info = ''.join(['<p style="text-indent: 2em;">{}</p>'.format(
                    p['text']) for p in lst])
            args = {'anid': anid, 'title': c.title, 'chaps': c.seq, 
                    'info': info, 'create_time': int(time.time())}
            online_session.execute(stmt, args)
        online_session.commit()
        anid += 1


if __name__ == '__main__':
    main()

