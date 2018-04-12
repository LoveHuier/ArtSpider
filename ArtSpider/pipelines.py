# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import codecs
import json

from scrapy.pipelines.images import ImagesPipeline
from scrapy.exporters import JsonItemExporter


class ArtspiderPipeline(object):
    def process_item(self, item, spider):
        return item


class ArticleImagePipeline(ImagesPipeline):
    """
    自定义pipeline方法，继承scrapy.pipelines.images.ImagesPipeline，并重载item_completed
    """

    def item_completed(self, results, item, info):
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
