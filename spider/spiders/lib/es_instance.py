__author__ = 'allanpan'
# !/usr/bin/python
# coding: utf-8

from elasticsearch import Elasticsearch
import singleton
from spider import settings


@singleton.thread_singleton
class EsIns(object):
    """ Elasticsearch instance, single model. """

    def __init__(self):
        """ Initialize elastic instance. """
        self.__es = Elasticsearch(settings.ES_HOST, port=settings.ES_PORT,
                                  sniff_on_start=True,
                                  sniff_on_connection_fail=True,
                                  timeout=120, sniffer_timeout=120, sniff_timeout=10, max_retries=3,
                                  retry_on_timeout=True)

    def instance(self):
        """ Return a es instance. """
        return self.__es


def es_instance():
    """ Get a instance."""
    return EsIns().instance()
