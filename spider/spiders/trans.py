# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

from spider.items import SpiderItem


class TransSpider(CrawlSpider):
    name = 'trans'
    allowed_domains = ['news.cn', 'news.xinhuanet.com']
    start_urls = ['http://www.news.cn/fortune/',
                  'http://www.news.cn/finance/',
                  'http://www.news.cn/finance/gsnds.htm',
                  'http://www.news.cn/fortune/caiyan.htm',
                  'http://www.news.cn/fortune/tanzhen.htm',
                  'http://www.news.cn/fortune/cfx.htm',
                  'http://www.news.cn/fortune/bcxc.htm',
                  'http://www.news.cn/finance/gsqbz.htm',
                  'http://www.news.cn/finance/tglj.htm']

    rules = (
        Rule(LinkExtractor(allow=r'http://www.news.cn/fortune/.*')),
        Rule(LinkExtractor(allow=r'http://www.news.cn/finance/.*')),
        Rule(LinkExtractor(allow=r'http://news.xinhuanet.com/fortune/\w{4}-\w{2}/\w+/.*'), callback='parse_item',
             follow=True),
        Rule(LinkExtractor(allow=r'http://news.xinhuanet.com/finance/\w{4}-\w{2}/\w+/.*'), callback='parse_item',
             follow=True),
    )

    def parse_item(self, response):
        self.logger.info('A response from %s just arrived!', response.url)
        item = SpiderItem()
        item['url'] = response.url
        title = response.xpath('//div[@id="article"]/h1[@id="title"]/text()').extract()[0]
        if title:
            item['title'] = title
        else:
            title['title'] = ''
        text = response.xpath('//div[@id="article"]/div[2]/p/text()').extract()
        if text:
            item['text'] = ' '.join(text)
        else:
            item['text'] = ''
        return item
