"""
本地es调用数据
"""

from elasticsearch import Elasticsearch
import logging
import traceback

logger = logging.getLogger('main.es_local')


# 聚合查询结果
def merge_result_list(final_result, es_result):
    hits = es_result.get('hits').get('hits')
    for item in hits:
        final_result.append(item['_source'])


# 以游标轮询的方式扫描所有数据
def scan(es_node, index_es, doc_type_es, query_json, max_records, scroll='5m'):
    final_result = []
    es = Elasticsearch(es_node)
    result = es.search(index=index_es, doc_type=doc_type_es, body=query_json, search_type="query_then_fetch", scroll=scroll, track_scores=False)
    # 总记录数
    total = int(result.get('hits').get('total'))
    # 取出第一个10条数据
    merge_result_list(final_result, result)
    # 默认每次从es中取出10条记录，要循环多少次才能将结果记录取完, 设置最大返回记录数为3000
    limit_total = total if total < max_records else max_records
    loops = (limit_total - 1) // 10
    scroll_id = result['_scroll_id']
    for i in range(0, loops):
        scroll_result = es.scroll(scroll_id=scroll_id, scroll="5m")
        merge_result_list(final_result, scroll_result)
    return final_result, total


# 从es查询符合条件的所有数据
def search(es_node, index_es, doc_type_es, key_words_must, key_words_must_not, max_records=3000):
    query_json = {
        "query": {
            "bool": {
                "must": generate_key_words_json(key_words_must),
                "must_not": generate_key_words_json(key_words_must_not)
            }
        },
        "sort": [
            {
                "date": {
                    "order": "asc"
                }
            }
        ]
    }
    result, total = [], 0
    try:
        result, total = scan(es_node, index_es, doc_type_es, query_json, max_records)
    except Exception as e:
        logger.debug('查询es出错，query_json：{}'.format(query_json))
        logger.debug(traceback.format_exc())
    finally:
        return result, total


# 生成关键字的j片段
def generate_key_words_json(key_words):
    key_words_json = []
    for word in key_words:
        word_json = {
                        "multi_match": {
                            "query": word,
                            "fields": [
                                "title",
                                "content"
                            ],
                            "type": "phrase",
                            "slop": 0
                        }
                    }
        key_words_json.append(word_json)
    return key_words_json


if __name__ == '__main__':
    es_node1 = ['localhost:9200/']
    index_es1 = ['articles-2018-2']
    doc_type_es1 = ['article']
    result1, total1 = search(es_node1, index_es1, doc_type_es1, ['旅游'], [], 100)
    print('共{}条记录，返回{}条记录：'.format(total1, len(result1)))
    for article in result1[:122]:
        print(article)

