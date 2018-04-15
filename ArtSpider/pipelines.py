# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import codecs
import json
import pymysql

from scrapy.pipelines.images import ImagesPipeline
from scrapy.exporters import JsonItemExporter
from twisted.enterprise import adbapi


class ArtspiderPipeline(object):
    def process_item(self, item, spider):
        return item


class MysqlPipeline(object):
    """
    采用同步的机制写入mysql
    """

    def __init__(self):
        self.conn = pymysql.connect(host="127.0.0.1", user="root", password="ts123456", db="art_schema", port=3306)
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        insert_sql = """
            insert into jobbole_article(title,url,create_date,fav_nums) 
            values({title},{url},{create_date},{fav_nums});
        """.format(title=item['title'], url=item['url'], create_date=item['create_date'], fav_nums=item['fav_nums'])
        self.cursor.execute(insert_sql)
        self.conn.commit()
        return item


class MysqlTwistedPipeline(object):
    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        dbparms = dict(
            host=settings['MYSQL_HOST'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            passwd=settings['MYSQL_PASSWORD'],
            port=settings['MYSQL_PORT'],
            charset='utf8',
            cursorclass=pymysql.cursors.DictCursor,
            use_unicode=True
        )

        dbpool = adbapi.ConnectionPool('pymysql', **dbparms)
        return cls(dbpool)

    def process_item(self, item, spider):
        """
        使用twisted将mysql插入变成异步执行，采用异步的机制写入mysql
        :param item:
        :param spider:
        :return:
        """
        query = self.dbpool.runInteraction(self.do_insert, item)
        # 处理异常
        query.addErrback(self.handle_error)

    def handle_error(self, failure):
        # 处理异步插入的异常
        print(failure)

    def do_insert(self, cursor, item):
        insert_sql = """
                    insert into jobbole_article(title,url,create_date,fav_nums,url_object_id,front_image_url,
                    front_image_path,praise_nums,comment_nums,tags,content) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                """
        cursor.execute(insert_sql,
                       (item['title'], item['url'], item['create_date'], item['fav_nums'], item['url_object_id']
                        , item['front_image_url'], item['front_image_path'], item['praise_nums'], item['comment_nums']
                        , item['tags'], item['content']))


class ArticleImagePipeline(ImagesPipeline):
    """
    自定义pipeline方法，继承scrapy.pipelines.images.ImagesPipeline，并重载item_completed
    """

    def item_completed(self, results, item, info):
        if "front_image_path" in item:
            for ok, value in results:
                front_image_path = value['path']
            item['front_image_path'] = front_image_path
        # 返回item以方便后面的pipeline对item进行处理
        return item


class JsonWithEncodingPipeline(object):
    """
    自定义JsonWithEncodingPipeline，用于存放json数据
    """

    def __init__(self):
        self.file = codecs.open("article.json", "w", encoding='utf-8')

    def process_item(self, item, spider):
        # 设置ensure_ascii=False防止存入中文时出现乱码
        lines = json.dumps(dict(item), ensure_ascii=False) + "\n"
        self.file.write(lines)
        return item

    def spider_closed(self, spider):
        self.file.close()


class JsonExporterPipleline(object):
    """
    利用scrapy定义的JsonItemExporter导出json数据
    """

    def __init__(self):
        self.file = open('articleexport.json', "wb")  # wb以二进制的方式打开
        self.exporter = JsonItemExporter(self.file, encoding='utf-8', ensure_ascii=False)
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def close_spider(self):
        self.exporter.finish_exporting()
        self.file.close()
