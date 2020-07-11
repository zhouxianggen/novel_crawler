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
    r = online_session.execute("SELECT max(id) FROM vv_mh_list").first()
    _id = r[0] + 1
    print('next anid is {}'.format(_id))

    book_ks = ['id', 'title', 'mhcate', 'summary', 'cover_pic', 
            'detail_pic', 'sort', 'status', 'free_type', 'episodes', 
            'is_new', 'is_recomm', 'create_time', 'update_time', 
            'share_title', 'share_pic', 'share_desc']
    chapter_ks = ['mhid', 'title', 'ji_no', 'pics', 'before', 'next', 
            'create_time', 'update_time']
    for b in session.execute('SELECT * FROM hmba_cartoon').fetchall():
        chapters = []
        max_seq, skip = 0, 0
        for c in session.execute(
                'SELECT * FROM hmba_chapter WHERE novel_id="{}"'.format(
                b.uuid)).fetchall():
            if not c.seq or c.state == 0:
                skip = 1
                break
            max_seq = max(max_seq, c.seq)
            chapters.append(c)
        if skip:
            continue
        print('insert book {}'.format(b.title))
        stmt = text("REPLACE INTO vv_mh_list({}) VALUES ({})".format(
                ', '.join(book_ks), 
                ', '.join([':{}'.format(k) for k in book_ks])))
        now = int(time.time())
        summary = b.summary or ''
        args = {'id':_id, 'title': b.title, 'mhcate': '5', 'summary': summary, 
                'cover_pic': b.cover, 'detail_pic': b.cover, 'sort': 12, 
                'status': 2 if b.finished else 1, 'free_type': 1, 
                'episodes': max_seq, 'is_new': 1, 'is_recomm': 1, 
                'create_time': now, 'update_time': now, 'share_title': b.title, 
                'share_pic': b.cover, 'share_desc': summary}
        online_session.execute(stmt, args)
        stmt = text("""REPLACE INTO vv_mh_episodes({}) 
                VALUES ({})""".format(
                ', '.join(['`{}`'.format(k) for k in chapter_ks]), 
                ', '.join([':{}'.format(k) for k in chapter_ks])))
        for c in chapters:
            lst = json.loads(c.paragraphs)
            pics = ','.join([x['img'] for x in lst])
            args = {'mhid': _id, 'title': c.title, 'ji_no': c.seq, 
                    'pics': pics, 'before': c.seq-1, 'next': c.seq+1, 
                    'create_time': now, 'update_time': now}
            online_session.execute(stmt, args)
        online_session.commit()
        _id += 1


if __name__ == '__main__':
    main()

