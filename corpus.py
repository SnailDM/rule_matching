"""
语料加载相关的函数集合
"""
import pandas as pd
from queue import Queue
import logging
import time
from config import configs
import common as cm
import os
import traceback
logger = logging.getLogger('main.corpus')

files_queue = Queue()   # 内存中的待处理文件缓存


class Folder:
    """
    文件夹类，
    """
    def __init__(self, name):
        self.name = name
        try:
            self.date = cm.string2date(name)
        except Exception as e:
            self.date = cm.string2date('1900-01-01')

    def __eq__(self, other):
        self.date == other.date

    def __lt__(self, other):
        self.date < other.date


class File:
    """
    文件类
    """
    def __init__(self, name, full_path, parent_folder):
        self.name = name
        self.full_path = full_path
        self.parent_folder = parent_folder

    def __hash__(self):
        return hash(self.full_path)

    def __eq__(self, other):
        return self.full_path == other.full_path


def clean_empty_folder(dir_monitor, clean_date):
    """
    清理给定日期之前的空文件夹
    :return:
    """
    # 遍历监控的目录
    for line in os.listdir(dir_monitor):
        file_path_1 = os.path.join(dir_monitor, line)
        # 如果子目录不是文件夹，则继续遍历
        if not os.path.isdir(file_path_1):
            continue
        folder = Folder(line)
        # 如果文件夹于清理日期之前，则删除该文件夹
        if (folder.date - clean_date).days < 0:
            if not os.listdir(file_path_1):
                os.rmdir(file_path_1)


def update_files_queue(dir_monitor, monitor_date, processed_files):
    """
    根据当前已处理到的文件缓存记录，获取待处理的文件列表
    :param dir_monitor: 监测目录
    :param monitor_date: 监测日期
    :param processed_files: 待处理文件
    """
    # 遍历监控的目录
    sub_dir_list = os.listdir(dir_monitor)
    sub_dir_list.sort()
    for line in sub_dir_list:
        file_path_1 = os.path.join(dir_monitor, line)
        # 如果子目录不是文件夹，则继续遍历
        if not os.path.isdir(file_path_1):
            continue
        folder = Folder(line)
        # 如果文件夹于监控日期之前，则继续遍历
        if (folder.date - monitor_date).days < 0:
            continue
        # 遍历符合条件的子目录
        sub_file_list = os.listdir(file_path_1)
        sub_file_list.sort()
        for li in sub_file_list:
            file_path_2 = os.path.join(file_path_1, li)
            file = File(li, file_path_2, folder)
            #  如果不是文件则继续遍历
            if os.path.isfile(file_path_2) and file not in processed_files:
                files_queue.put(file)


# 动态加载爬虫数据
def load_spider_df(dir_monitor, monitor_date, processed_files):
    """
    根据已处理的文件记录，依时间次序加载待处理文件
    :return: dataframe
    """
    while True:
        # 如果当前没有待处理的文件，则休眠一定时间
        if not files_queue.empty():
            file = files_queue.get()
            # 尝试三次读取文件，如果失败则换另一个文件读取
            for i in range(3):
                try:
                    # 测试爬虫数据
                    # spider_df = read_hdfs_file(file.full_path)
                    # 实际部署时读取nlp处理的中间结果文件
                    spider_df = read_hdfs_file_with_heads(file.full_path)
                    return spider_df, file
                except Exception as e:
                    logger.error('读取文件{}出错'.format(file.name))
                    logger.error(traceback.format_exc())
                    if i < 2:
                        logger.debug('等待{}秒后，再次尝试。'.format(configs['date']['sleep_time']))
                        time.sleep(configs['date']['sleep_time'])
                    else:
                        logger.debug('尝试三次均失败，跳过该文件{}的读取。'.format(file.name))

        else:
            update_files_queue(dir_monitor, monitor_date, processed_files)
            # 测试打印队列
            # [print(files_queue.queue[i].full_path) for i in range(len(files_queue.queue))]
            if files_queue.empty():
                logger.info('当前没有检索到新文件。等待{}秒后再尝试检索。'.format(configs['date']['sleep_time']))
                time.sleep(configs['date']['sleep_time'])
                continue


# 读取新闻爬虫爬取得文件
def read_hdfs_file(file_path):
    ls = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            ls.append(line.replace('\n', ' ').split('|')[:-1])
    df = pd.DataFrame.from_records(ls, columns=["recno", "id", "recType", "thesource","reference", "date", "ffdCreate", "languagetype",
                                           "dresource", "title", "content", "abstract", "recemotional" ,"area", "frequencyword",
                                           "likeinfo", "likeinfocount", "screen_name", "comments", "reportcount", "readcount",
                                           "weibotype", "weixintype", "hotvalue", "mediatype", "alarmlevel", "keyword", "businesstype"])
    return df


# 读取带标题的文件名
def read_hdfs_file_with_heads(file_path):
    df = pd.read_csv(file_path, sep='|')
    return df


if __name__ == '__main__':
    # df = read_hdfs_file(r'E:\项目\移动在线本部\网罗天下\商机规则匹配\20180102093031_webspider\20180102093031_webspider.out')
    folder1 = Folder('2018-02-3')
    file2 = File('20180620193031_webspider.out', r'E:\data\webspider\2018-6-20\20180620193031_webspider.out', folder1)
    file1 = File('20180620193031_webspider.out', r'E:\data\webspider\2018-6-20\20180620193031_webspider.out', folder1)
    s = set()
    s.add(file2)
    print(s)
    pass
