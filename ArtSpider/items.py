# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
import re
from scrapy.loader.processors import MapCompose, TakeFirst, Join
from scrapy.loader import ItemLoader


class ArtspiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class ArticleItemLoader(ItemLoader):
    """
    自定义itemloader
    """
    default_output_processor = TakeFirst()


def get_nums(value):
    match_re = re.findall("(\d+).*", value.strip())
    if match_re:
        nums = match_re[0]
    else:
        nums = '0'
    return nums


def return_value(value):
    return value


def remove_comment_tags(value):
    if "评论" in value:
        return ""
    else:
        return value


class JobBoleArticleItem(scrapy.Item):
    title = scrapy.Field(
        input_processor=MapCompose(lambda x: x + "-jobbole")
        # output_processor=TakeFirst()
    )
    create_date = scrapy.Field()
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    front_image_url = scrapy.Field(
        output_processor=MapCompose(return_value)
    )
    front_image_path = scrapy.Field()
    praise_nums = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    fav_nums = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    comment_nums = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    tags = scrapy.Field(
        input_processor=MapCompose(remove_comment_tags),
        output_processor=Join(",")
    )
    content = scrapy.Field()
