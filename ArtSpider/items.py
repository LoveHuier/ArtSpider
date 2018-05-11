# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
import re
from scrapy.loader.processors import MapCompose, TakeFirst, Join
from scrapy.loader import ItemLoader
from ArtSpider.settings import SQL_DATE_FORMAT, SQL_DATETIME_FORMAT
from ArtSpider.models.es_types import LagouType,JobboleType
from elasticsearch_dsl.connections import connections

# 连接es
es = connections.create_connection(LagouType._doc_type.using)

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


def get_publish_time(value):
    value = value.replace("·", "").strip()
    return value


def get_content(value):
    return value.strip()


class JobBoleArticleItem(scrapy.Item):
    title = scrapy.Field(
        # input_processor=MapCompose(lambda x: x + "-jobbole")
        # output_processor=TakeFirst()
    )
    create_date = scrapy.Field(
        input_processor=MapCompose(get_publish_time)
    )
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
    content = scrapy.Field(
        input_processor=MapCompose(remove_tags, get_content)
    )

    def get_insert_sql(self):
        insert_sql = """
                        insert into jobbole_article(title,url,create_date,fav_nums,url_object_id,front_image_url,
                                front_image_path,praise_nums,comment_nums,tags,content) 
                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        on duplicate key update fav_nums=values(fav_nums),comment_nums=values(comment_nums);
                        """
        parsms = (self['title'], self['url'], self['create_date'], self['fav_nums'], self['url_object_id']
                  , self['front_image_url'], self['front_image_path'], self['praise_nums'], self['comment_nums']
                  , self['tags'], self['content'])
        return insert_sql, parsms

    def save_to_es(self):
        # 将item转化为es的数据
        jobbole = JobboleType()
        jobbole.title = self['title']
        jobbole.url = self['url']
        jobbole.create_date = self['create_date']
        jobbole.tags = self['tags']
        jobbole.content=self['content']
        jobbole.meta.id = self['url_object_id']
        jobbole.suggest = gen_suggests(LagouType._doc_type.index, ((jobbole.title, 10), (jobbole.tags, 7)))

        jobbole.save()

        return


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
    replace_char = ["经验", "年", "/", "以下", "以上"]
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


def gen_suggests(index, info_tuple):
    # 根据字符串生成搜索建议数组
    used_words = set()
    suggests = []
    for text, weight in info_tuple:
        if text:
            # 调用es的analyze接口分析字符串,得到分词
            words = es.indices.analyze(index=index, body={"text": text, 'analyzer': "ik_max_word"},
                                       params={"filter": ["lowercase"]})
            analyzed_words = set([w["token"] for w in words["tokens"] if len(w["token"]) > 1])
            # 获取新增加的词
            new_words = analyzed_words - used_words
        else:
            new_words = set()
        if new_words:
            suggests.append({
                "input": list(new_words),
                "weight": weight
            })

    return suggests


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

    def get_insert_sql(self):
        insert_sql = """
                        insert into lagou_job(title,url,url_object_id,salary_min,salary_max,job_city,work_year_min,work_year_max,
                                              degree_need,job_type,publish_time,job_advantage,job_desc,
                                              job_addr,company_name,company_url,crawl_time,tag
                                              ) 
                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        on duplicate key update job_desc=values(job_desc),tag=values(tag);
                        """
        parsms = (
            self['title'], self['url'], self['url_object_id'], self['salary_min'], self['salary_max'], self['job_city']
            , self['work_year_min'], self['work_year_max'], self['degree_need'], self['job_type']
            , self['publish_time'], self['job_advantage'], self['job_desc'], self['job_addr'],
            self['company_name'], self['company_url'], self['crawl_time'].strftime(SQL_DATETIME_FORMAT), self['tag']
        )
        return insert_sql, parsms

    def save_to_es(self):
        # 将item转化为es的数据
        lagou = LagouType()
        lagou.title = self['title']
        lagou.url = self['url']
        lagou.salary_min = self['salary_min']
        lagou.salary_max = self['salary_max']
        lagou.job_city = self['job_city']
        lagou.work_year_min = self['work_year_min']
        lagou.work_year_max = self['work_year_max']
        lagou.degree_need = self['degree_need']
        lagou.job_type = self['job_type']
        lagou.publish_time = self['publish_time']
        lagou.tag = self['tag']
        lagou.job_advantage = self['job_advantage']
        lagou.job_desc = self['job_desc']
        lagou.job_addr = self['job_addr']
        lagou.company_url = self['company_url']
        lagou.company_name = self['company_name']
        lagou.crawl_time = self['crawl_time']
        lagou.meta.id = self['url_object_id']
        lagou.suggest = gen_suggests(LagouType._doc_type.index, ((lagou.title, 10), (lagou.tag, 7)))

        lagou.save()

        return


"""
create table lagou_job(url varchar(300) not null,url_object_id varchar(50) not null primary key,
    title varchar(100) not null,salary_min int(10),salary_max int(10),
    job_city varchar(10),work_year_min int(10),work_year_max int(10),
    degree_need varchar(30),job_type varchar(20),publish_time varchar(20) not null,tag varchar(100),
    job_advantage varchar(1000),job_desc longtext not null,job_addr varchar(50),
    company_url varchar(300),company_name varchar(100),crawl_time datetime not null,
     crawl_update_time datetime);
"""
