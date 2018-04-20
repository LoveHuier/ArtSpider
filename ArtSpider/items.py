# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
import re
from scrapy.loader.processors import MapCompose, TakeFirst, Join
from scrapy.loader import ItemLoader

# 用于删除提取的html中的tag
from w3lib.html import remove_tags


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


class LagouJobItemLoader(ItemLoader):
    # 自定义itemloader
    default_output_processor = TakeFirst()


def get_min_salary(value):
    salary_list = value.split("-")
    if salary_list:
        min_salary = salary_list[0]
        if "k" in min_salary:
            min_salary = min_salary.replace("k", "")
            return int(min_salary) * 1000
    else:
        return 0


def get_max_salary(value):
    salary_list = value.split("-")
    if salary_list:
        min_salary = salary_list[-1]
        if "k" in min_salary:
            min_salary = min_salary.replace("k", "")
            return int(min_salary) * 1000
    else:
        return 0


def get_min_wyear(value):
    # 经验3-5年 /
    wyear_list = value.replace("经验", "").replace("年", "").replace("/", "").strip().split("-")
    replace_char = ["经验", "年", "/"]
    for i in replace_char:
        if i in value:
            value = value.replace(i, "")
    wyear_list = value.split("-")
    if wyear_list:
        return int(wyear_list[0])
    else:
        return 0


def get_max_wyear(value):
    # 经验3-5年 /
    replace_char = ["经验", "年", "/"]
    for i in replace_char:
        if i in value:
            value = value.replace(i, "")
    wyear_list = value.split("-")
    if wyear_list:
        return int(wyear_list[-1])
    else:
        return 0


def remove_splash(value):
    if "/" in value:
        value = value.replace("/", "").strip()
    return value


def handle_jobaddr(value):
    addr_list = value.split("\n")
    addr_list = [item.strip() for item in addr_list if item.strip() != "查看地图"]
    return "".join(addr_list)


class LagouJobItem(scrapy.Item):
    """
    拉勾网职位信息
    """
    title = scrapy.Field()
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    salary_min = scrapy.Field(
        input_processor=MapCompose(get_min_salary),
        # output_processor=MapCompose(return_value)
    )
    salary_max = scrapy.Field(
        input_processor=MapCompose(get_max_salary),
        # output_processor=MapCompose(return_value)
    )
    job_city = scrapy.Field(
        input_processor=MapCompose(remove_splash),
    )
    work_year_min = scrapy.Field(
        input_processor=MapCompose(get_min_wyear),
    )
    work_year_max = scrapy.Field(
        input_processor=MapCompose(get_max_wyear),
    )
    degree_need = scrapy.Field(
        input_processor=MapCompose(remove_splash)
    )
    job_type = scrapy.Field()
    publish_time = scrapy.Field()
    tag = scrapy.Field(
        input_processor=Join(",")
    )
    job_advantage = scrapy.Field()
    job_desc = scrapy.Field(
        input_processor=MapCompose(remove_tags)
    )
    job_addr = scrapy.Field(
        input_processor=MapCompose(remove_tags, handle_jobaddr)
    )
    company_url = scrapy.Field()
    company_name = scrapy.Field()
    crawl_time = scrapy.Field()


"""
create table lagou_job(url varchar(300) not null,url_object_id varchar(50) not null primary key,
    title varchar(100) not null,salary_min int(10),salary_max int(10),
    job_city varchar(10),work_year_min int(10),work_year_max int(10),
    degree_need varchar(30),job_type varchar(20),publish_time varchar(20) not null,tag varchar(100),
    job_advantage varchar(1000),job_desc longtext not null,job_addr varchar(50),
    company_url varchar(300),company_name varchar(100),crawl_time datetime not null,
     crawl_update_time datetime);
"""
