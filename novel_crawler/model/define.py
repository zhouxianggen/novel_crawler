import re


class MyEnum(object):
    __reo_enum_value = re.compile(r'[A-Z_]+$')
    __domain__ = None

    @classmethod
    def domain(cls):
        if cls.__domain__ is None:
            cls.__domain__ = set()
            for k,v in cls.__dict__.items():
                if cls.__reo_enum_value.match(k):
                    cls.__domain__.add(v)
        return cls.__domain__


class NovelState(MyEnum):
    NEW = 0
    FINISHED = 5

