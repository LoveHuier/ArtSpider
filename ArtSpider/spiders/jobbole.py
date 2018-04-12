# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import Request
from urllib import parse

from ArtSpider.items import JobBoleArticleItem
from ArtSpider.utils.common import get_md5


class JobboleSpider(scrapy.Spider):
    name = 'jobbole'
    allowed_domains = ['blog.jobbole.com']
    start_urls = ['http://blog.jobbole.com/all-posts/']

    def parse(self, response):
        # 获取当前页面所有文章url并进行解析
        nodes = response.xpath('//div[@class="post floated-thumb"]/div[@class="post-thumb"]/a')
        for node in nodes:
            image_url = node.xpath('img/@src').extract_first("")
            article_url = node.xpath('@href').extract_first("")
            # yield关键字：把Request交给scrapy处理。parse.urljoin做字符串的拼接
            # meta将imageurl绑定到对应的Request上
            yield Request(url=parse.urljoin(response.url, article_url), meta={"front_image_url": image_url},
                          callback=self.parse_detail)

            # # 判断是否有下一页，并进行下载
            # next_url = response.xpath('//a[@class="next page-numbers"]/@href').extract()
            # if next_url:
            #     yield Request(url=parse.urljoin(response.url, next_url), callback=self.parse)

    def parse_detail(self, response):
        article_item = JobBoleArticleItem()

        # 解析当前文章页
        title = response.xpath('//div[@class="entry-header"]/h1/text()').extract_first()
        # 文章封面图
        front_image_url = response.meta.get("front_image_url", "")

        create_data = response.xpath('//p[@class="entry-meta-hide-on-mobile"]/text()').extract_first().strip()
        praise_nums = response.xpath('//span[contains(@class,"vote-post-up")]/h10/text()').extract_first("0")
        fav_nums = response.xpath('//span[contains(@class,"bookmark-btn")]/text()').extract_first("")
        comment_nums = response.xpath('//a[@href="#article-comment"]/span/text()').extract_first("")
        content = response.xpath('//div[@class="entry"]').extract_first("")
        tag_list = response.xpath('//p[@class="entry-meta-hide-on-mobile"]/a/text()').extract()
        tag_list = [tag for tag in tag_list if not tag.strip().endswith("评论")]
        tags = ",".join(tag_list)

        article_item['url_object_id'] = get_md5(response.url)
        article_item['title'] = title
        article_item['url'] = response.url
        article_item['create_date'] = create_data
        article_item['front_image_url'] = [front_image_url]
        article_item['praise_nums'] = praise_nums
        article_item['comment_nums'] = comment_nums
        article_item['fav_nums'] = fav_nums
        article_item['tags'] = tags
        article_item['content'] = content

        # 利用yield关键字将article_item传递到pipeline中
        yield article_item
