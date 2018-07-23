import pandas as pd
import pymysql
from datetime import datetime


# 将data frame转为json, 并写入文件
def df_to_json(df, file_name):
    """
    将dataframe存为json格式的文件
    :param df:
    :param file_name:
    :return:
    """
    with open(file_name, "w", encoding='utf-8') as f:
        i = 0
        for index, row in df.iterrows():
            if i > 0:
                f.write('\n')  # 换行符
            j_s = row.to_json(force_ascii=False)
            f.write(j_s)
            i += 1


def combine_news_type_query(news_type_set):
    """拼接关于信源的查询sql部分"""
    news_type_query = ''
    for news_type in news_type_set:
        news_type_query = "{}or NEWS_TYPE LIKE '%{}%' ".format(news_type_query, str(news_type))
    news_type_query = news_type_query.lstrip('or')
    return news_type_query


def read_word_config(db, word_config_file, word_config_table, news_type_set):
    """
    从mysql中读取最新的词条配置
    :return:
    """
    db_conn = pymysql.connect(**db)
    news_type_query = combine_news_type_query(news_type_set)
    # news_type_query = "NEWS_TYPE LIKE '%5%' or NEWS_TYPE LIKE '%3%'"
    sql_cmd = """SELECT ID, `NAME`, NEWS_TYPE, APPLY_TYPE, TITLE_KEYWORD, TITLE_NOT_KEYWORD, CONTENT_KEYWORD, CONTENT_NOTKEYWORD, MATCH_TYPE, TENANT_ID, START_TIME, END_TIME
            FROM  {}   WHERE   `STATUS` <> 0  AND
            ({})""".format(word_config_table, news_type_query)
    record = pd.read_sql(sql_cmd, db_conn)
    if len(record) > 0:
        record = record.dropna()  # 有问题
        record['START_TIME'] = record.apply(lambda row: row['START_TIME'].strftime("%Y-%m-%d %H:%M:%S"), axis=1)
        record['END_TIME'] = record.apply(lambda row: row['END_TIME'].strftime("%Y-%m-%d %H:%M:%S"), axis=1)
    df_to_json(record, word_config_file)


def read_word_config_by_txt(word_config_file, word_config_txt):
    """从txt读取词条配置"""
    df = pd.read_csv(word_config_txt, sep=':,:', encoding='utf-8')
    df_to_json(df, word_config_file)


if __name__=="__main__":
    from config import configs
    read_word_config_by_txt(configs['path']['word_config'], configs['path']['word_config_txt'])
