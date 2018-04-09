"""
语料加载相关的函数集合
"""
import pandas as pd


# 读取新闻爬虫爬取得文件
def read_hdfs_file(file_path):
    df = pd.DataFrame()
    ls = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            ls.append(line.replace('\n', ' ').split('|')[:-1])
    df = pd.DataFrame.from_records(ls, columns=["recno", "id", "rectype", "thesource","reference", "date", "ffdcreate", "languagetype",
                                           "dresource", "title", "content", "abstract", "recemotional" ,"area", "frequencyword",
                                           "likeinfo", "likeinfocount", "screen_name", "comments", "reportcount", "readcount",
                                           "weibotype", "weixintype", "hotvalue", "mediatype", "alarmlevel", "keyword", "businesstype"])
    return df


if __name__=='__main__':
    df = read_hdfs_file(r'E:\项目\移动在线本部\网罗天下\商机规则匹配\20180102093031_webspider\20180102093031_webspider.out')
    pass
