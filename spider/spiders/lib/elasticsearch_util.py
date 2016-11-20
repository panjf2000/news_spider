#!/usr/bin/python
# coding: utf-8
import sys

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.exceptions import *

reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append('..')
from spider import settings
import es_instance

INDEX = 'news'

if settings.ES_HOST is not None:
    es = es_instance.es_instance()
else:
    es = Elasticsearch()


def index_doc(_index=INDEX, _type=None, _id=None, _doc=None):
    if _type and _doc:
        if _id:
            res = es.index(index=_index, doc_type=_type, id=_id, body=_doc, refresh=True)
        else:
            res = es.index(index=_index, doc_type=_type, body=_doc, refresh=True)
        return res


def update_doc(_index=INDEX, _type=None, _id=None, _doc=None):
    if _type and _doc:
        if _id:
            res = es.update(index=_index, doc_type=_type, id=_id, body=_doc, refresh=True)
        else:
            return None
        return res


def delete_doc(_index=INDEX, _type=None, _id=None):
    if _type and _id:
        res = es.delete(index=_index, doc_type=_type, id=_id, refresh=True)
    else:
        return None
    return res


def streaming_bulk(actions, stat_only=False):
    try:
        success, failed = helpers.bulk(es, actions, stat_only)
    except TransportError as e:
        trys = 3
        while trys:
            try:
                success, failed = helpers.bulk(es, actions, stat_only)
                break
            except TransportError as e:
                trys -= 1
                continue
        if trys <= 0:
            raise e
    except ConnectionTimeout:
        trys = 3
        while trys:
            try:
                success, failed = helpers.bulk(es, actions, stat_only)
                break
            except ConnectionTimeout as e:
                trys -= 1
                continue
        if trys <= 0:
            raise e
    return success, failed


def parallel_bulk(actions, thread_size=5, doc_size=500, max_bytes=100 * 1024 * 1024):
    success, failed = 0, 0
    try:
        for ok, item in helpers.parallel_bulk(es, actions, thread_count=thread_size, chunk_size=doc_size,
                                              max_chunk_bytes=max_bytes):
            # go through request-reponse pairs and detect failures
            if not ok:
                failed += 1
            else:
                success += 1
    except TransportError as e:
        trys = 3
        while trys:
            try:
                for ok, item in helpers.parallel_bulk(es, actions, thread_count=thread_size, chunk_size=doc_size,
                                                      max_chunk_bytes=max_bytes):
                    # go through request-reponse pairs and detect failures
                    if not ok:
                        failed += 1
                    else:
                        success += 1
                break
            except TransportError as e:
                trys -= 1
                continue
        if trys <= 0:
            raise e
    except ConnectionTimeout:
        trys = 3
        while trys:
            try:
                for ok, item in helpers.parallel_bulk(es, actions, thread_count=thread_size, chunk_size=doc_size,
                                                      max_chunk_bytes=max_bytes):
                    # go through request-reponse pairs and detect failures
                    if not ok:
                        failed += 1
                    else:
                        success += 1
                break
            except ConnectionTimeout as e:
                trys -= 1
                continue
        if trys <= 0:
            raise e
    return success, failed


def get_doc(_index=INDEX, _type=None, _id=None):
    if _index and _id:
        try:
            res = es.get_source(index=_index, type=_type, id=_id)
            return res
        except ConnectionTimeout as e:
            raise e

        except Exception as e:
            raise e


def search_doc_struct_exact(_index=INDEX, _type=None, term={}, start=0, size=20):
    if _type and term:
        if len(term) == 1:
            _body = {
                "from": start,
                "size": size,
                "query": {
                    "filtered": {
                        "query": {
                            "match_all": {}
                        },
                        "filter": {
                            "term": {
                                term.keys()[0]: term.values()[0]
                            }
                        }
                    }
                }
            }
        else:
            must_list = []
            for k, v in term.items():
                must_list.append({"term": {k: v}})

            _body = {
                "from": start,
                "size": size,
                "query": {
                    "filtered": {
                        "query": {
                            "match_all": {}
                        },
                        "filter": {
                            "bool": {
                                "must": must_list
                            }
                        }
                    }
                }
            }
        try:
            res = es.search(index=_index, doc_type=_type, body=_body)
            return res
        except ConnectionTimeout as e:
            raise e

        except Exception as e:
            raise e


def search_doc_struct_mulexcat(_index=INDEX, _type=None, terms={}, start=0, size=20):
    if _type and terms:
        if len(terms) == 1:
            _body = {
                "from": start,
                "size": size,
                "query": {
                    "filtered": {
                        "filter": {
                            "terms": {
                                terms.keys()[0]: terms.values()[0]
                            }
                        }
                    }
                }
            }
        else:
            must_list = []
            for k, v in terms.items():
                must_list.append({"term": {k: v}})

            _body = {
                "from": start,
                "size": size,
                "query": {
                    "filtered": {
                        "query": {
                            "match_all": {}
                        },
                        "filter": {
                            "bool": {
                                "must": must_list
                            }
                        }
                    }
                }
            }
        try:
            res = es.search(index=_index, doc_type=_type, body=_body)
            return res
        except ConnectionTimeout as e:
            raise e

        except Exception as e:
            raise e


def search_doc_struct_range(_index=INDEX, _type=None, range={}, start=0, size=20):
    if _type and range:
        if len(range) == 1:
            _body = {
                "from": start,
                "size": size,
                "query": {
                    "filtered": {
                        "filter": {
                            "range": {
                                range.keys()[0]: range.values()[0]
                            }
                        }
                    }
                }
            }
        else:
            must_list = []
            for k, v in range.items():
                must_list.append({"range": {k: v}})

            _body = {
                "from": start,
                "size": size,
                "query": {
                    "filtered": {
                        "query": {
                            "match_all": {}
                        },
                        "filter": {
                            "bool": {
                                "must": must_list
                            }
                        }
                    }
                }
            }

        try:
            res = es.search(index=_index, doc_type=_type, body=_body)
            return res
        except ConnectionTimeout as e:
            raise e

        except Exception as e:
            raise e


def search_doc_fulltext_match(_index=INDEX, _type=None, match={}, start=0, size=20):
    if _type and match:
        if len(match) == 1:
            _body = {
                "from": start,
                "size": size,
                "query": {
                    "match": {
                        match.keys()[0]: match.values()[0]
                    }
                }
            }

        else:
            match_list = []
            for k, v in match:
                match_list.append({"match": {k: v}})

            _body = {
                "from": start,
                "size": size,
                "query": {
                    "bool": {
                        "must": match_list
                    }
                }
            }

        try:
            res = es.search(index=_index, doc_type=_type, body=_body)
            return res
        except ConnectionTimeout as e:
            raise e

        except Exception as e:
            raise e


def search_doc_fulltext_com(_index=INDEX, _type=None, keyword='', fields=[], terms={}, start=0, size=20):
    if _type and keyword and fields:
        if terms:
            must_list = []
            for k, v in terms.items():
                must_list.append({"term": {k: v}})
            _body = {
                "from": start,
                "size": size,
                "query": {
                    "filtered": {
                        "query": {
                            "multi_match": {
                                "query": keyword,
                                "type": "best_fields",
                                "fields": fields,
                                "tie_breaker": 0.3,
                                "minimum_should_match": "80%"
                            }
                        },
                        "filter": {
                            "bool": {
                                "must": must_list
                            }
                        }
                    }
                }
            }
        else:
            _body = {
                "from": start,
                "size": size,
                "query": {
                    "multi_match": {
                        "query": keyword,
                        "type": "best_fields",
                        "fields": fields,
                        "tie_breaker": 0.3,
                        "minimum_should_match": "80%"
                    }
                }
            }

        try:
            res = es.search(index=_index, doc_type=_type, body=_body)
            return res
        except ConnectionTimeout as e:
            raise e

        except Exception as e:
            raise e


def search_doc_complex(_index=INDEX, _type=None, query={}):
    if _type and query:
        _body = query

        try:
            res = es.search(index=_index, doc_type=_type, body=_body)
            return res
        except ConnectionTimeout as e:
            raise e

        except Exception as e:
            raise e
