"""
性能评估
"""
import pandas as pd
import es_local
import time
import match
import corpus


def read_rules():
    file = r'E:\project\poc\rule_matching\SELECT_NAME__KEYWORD__NOTWORD__titleKeyW.csv'
    rules_cvs = pd.read_csv(file, sep=',', encoding='utf-8', header=None)
    rules = []
    for index, row in rules_cvs.iterrows():
        r0, r1, r2 = str(row[0]), str(row[1]), str(row[2])
        if r0 == 'nan' or r1 == 'nan':
            continue
        if r2 != 'nan':
            rule = '({})-({})'.format(r1, r2)
        else:
            rule = r1
        rules.append((rule, r0))
    return rules


def read_news(keywords):
    es_node1 = ['localhost:9200/']
    index_es1 = None
    doc_type_es1 = None
    result1, total1 = es_local.search(es_node1, index_es1, doc_type_es1, keywords, [], 100000)
    print('共{}条记录，返回{}条记录：'.format(total1, len(result1)))
    return [new['content'] for new in result1]


def read_corpus_news(file_path):
    df = corpus.read_hdfs_file(file_path)
    return df['recno'].values, df['content'].values


def evaluate():
    recnos, news = read_corpus_news(r'E:\项目\移动在线本部\网罗天下\商机规则匹配\20180102093031_webspider\20180102093031_webspider.out')
    rules = read_rules()
    start_time = time.time()
    rules_set1 = match.RulesSet(rules)
    rules_time = time.time()
    res = []
    pre_time = time.time()
    for index, new in enumerate(news):
        json = match.mark_text(new, rules_set1)
        res.append(json)
        if (index+1) % 100 == 0:
            print('共{}条，已完成{}条,耗时{}s'.format(len(news), index+1, time.time()-pre_time))
            pre_time = time.time()
    res_time = time.time()
    print('新闻数：{}条 ， 规则数{}条'.format(len(news), len(rules)))
    print('耗费总时间：{}s , 规则解析时间:{}s , 规则匹配时间{}s '.format(res_time-start_time, rules_time-start_time, res_time-rules_time))
    dataframe = pd.DataFrame({'recno': recnos, 'res': res})
    dataframe.to_csv(r"E:\项目\移动在线本部\网罗天下\商机规则匹配\res.csv", index=False, sep=',')


# 测试多进程
def evaluate_parallel():
    recnos, news = read_corpus_news(r'E:\项目\移动在线本部\网罗天下\商机规则匹配\20180102093031_webspider\20180102093031_webspider.out')
    rules = read_rules()
    start_time = time.time()
    rules_set1 = match.RulesSet(rules)
    rules_time = time.time()
    res = match.mark_parallel_process(news, rules_set1)
    res_time = time.time()
    print('新闻数：{}条 ， 规则数{}条'.format(len(news), len(rules)))
    print('耗费总时间：{}s , 规则解析时间:{}s , 规则匹配时间{}s '.format(res_time-start_time, rules_time-start_time, res_time-rules_time))
    dataframe = pd.DataFrame({'recno': recnos, 'res': res})
    dataframe.to_csv(r"E:\项目\移动在线本部\网罗天下\商机规则匹配\res.csv", index=False, sep=',')


if __name__ == '__main__':
    evaluate()




