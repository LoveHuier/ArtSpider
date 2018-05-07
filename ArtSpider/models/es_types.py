from datetime import datetime
from elasticsearch_dsl import DocType, Date, Nested, Boolean, \
    analyzer, InnerDoc, Completion, Keyword, Text, Integer

from elasticsearch_dsl.connections import connections
connections.create_connection(hosts=["127.0.0.1"])

class LagouType(DocType):
    # 拉勾职位类型
    # 在这里定义映射
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


if __name__ == "__main__":
    LagouType.init()
