# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.pipelines.images import ImagesPipeline


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
