from datetime import datetime
from elasticsearch_dsl import DocType, Date, Nested, Boolean, \
    analyzer, Completion, Keyword, Text, Integer

from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.analysis import CustomAnalyzer as _CustomAnalyzer

# Define a default Elasticsearch client
connections.create_connection(hosts=["127.0.0.1"])


class CustomAnalyzer(_CustomAnalyzer):
    # 重写get_analysis_definition方法
    def get_analysis_definition(self):
        return {}


ik_analyzer = CustomAnalyzer("ik_max_word", filter=["lowercase"])  # 加入filter做大小写的转换


class LagouType(DocType):
    # 拉勾职位类型
    # 在这里定义映射
    # suggest = Completion(analyzer="ik_max_work") 理论上可以这样，但是elasticsearch_dsl源码有问题
    # 添加suggest字段，为了完成补全功能
    suggest = Completion(analyzer=ik_analyzer)
    title = Text(analyzer="ik_max_word")
    url = Keyword()
    url_object_id = Keyword()
    salary_min = Integer()
    salary_max = Integer()
    job_city = Keyword()
    work_year_min = Integer()
    work_year_max = Integer()
    degree_need = Keyword()
    job_type = Keyword()
    publish_time = Keyword()
    tag = Text(analyzer="ik_max_word")
    job_advantage = Text(analyzer="ik_max_word")
    job_desc = Text(analyzer="ik_max_word")
    job_addr = Keyword()
    company_url = Keyword()
    company_name = Keyword()
    crawl_time = Date()

    class Meta:
        index = "lagou"
        doc_type = "job"


class JobboleType(DocType):
    suggest = Completion(analyzer=ik_analyzer)
    title = Text(analyzer="ik_max_word")
    create_date = Date()
    url = Keyword()
    comment_nums = Integer()
    fav_nums = Integer()
    praise_nums = Integer()
    tags = Text(analyzer="ik_max_word")
    content = Text(analyzer="ik_max_word")

    class Meta:
        index = "jobbole"
        doc_type = "article"


if __name__ == "__main__":
    # LagouType.init()
    JobboleType.init()
