#!/usr/bin/python
# coding: utf-8

import thread


def singleton(cls):
    """ decorator for singleton.
    
    One singleton for one process. Threads in one process use one same singleton.
    """
    instances = {}

    def _singleton(*args, **kw):
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]

    return _singleton


def thread_singleton(cls):
    """ Thread specific singleton.
    
    One singleton for one thread. threads in one process use different singletons.
    """
    instances = {}

    def _singleton(*args, **kw):
        t_ident = thread.get_ident()
        if cls not in instances:
            instances[cls] = {}
            instances[cls][t_ident] = cls(*args, **kw)
        elif t_ident not in instances[cls]:
            instances[cls][t_ident] = cls(*args, **kw)
        return instances[cls][t_ident]

    return _singleton


class Singleton(object):
    """ Singleton helper. To use singleton, you need to inherit this class. """

    def __new__(cls, *args, **kw):
        if not hasattr(cls, '_instance'):
            cls._instance = super(Singleton, cls).__new__(cls, *args, **kw)
        return cls._instance
