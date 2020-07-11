from configparser import ConfigParser
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from novel_crawler.base import BaseObject


class BaseJob(BaseObject):
    def __init__(self, conf):
        BaseObject.__init__(self)
        self.cfg = ConfigParser()
        self.cfg.read(conf)
        CONN = self.cfg.get('mysql', 'conn')
        self.engine = create_engine(CONN, pool_size=100, pool_recycle=3600)
        self.Session = scoped_session(sessionmaker(bind=self.engine))


    def run(self):
        raise NotImplementedError

    
    @contextmanager
    def session_scope(self):
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()


    def session(self):
        return self.Session()

