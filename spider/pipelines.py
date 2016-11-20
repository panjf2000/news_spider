# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import elasticsearch
from spiders.lib import elasticsearch_util
from elasticsearch.exceptions import *
from spiders.lib import es_instance
from spiders.lib import filter_html


class FilterPipeline(object):
    """过滤网页正文中的html标签"""

    def process_item(self, item, spider):
        if item['title']:
            item['title'] = filter_html.filter_tags(item['title'])
        if item['text']:
            item['text'] = filter_html.filter_tags(item['text'])
        return item


class SpiderPipeline(object):
    index_name = 'news'
    doc_type = 'article'

    def __init__(self):
        self.es_client = es_instance.es_instance()

    def open_spider(self, spider):
        self.es_client = es_instance.es_instance()

    def close_spider(self, spider):
        pass

    def process_item(self, item, spider):
        # self.db[self.collection_name].insert(dict(item))
        action = {
            "_index": self.index_name,
            "_doc_type": self.doc_type,
            "_id": item['url'],
            "_source": dict(item)
        }
        try:
            self.es_client.index(index=self.index_name, doc_type=self.doc_type, id=item['url'], body=dict(item),
                                 refresh=True)
        except TransportError as e:
            raise e

        except ConnectionTimeout as e:
            raise e

        except Exception as e:
            raise e

        return item
