"""
一些通用方法
"""
from collections import defaultdict
import os
import datetime as dt
from datetime import datetime
import numpy as np
import pickle
import time
import hashlib
import json

# 极小值
EPS = 1e-100


# 获取当前目录
_get_abs_path = lambda path: os.path.normpath(os.path.join(os.getcwd(), path))


# 安全计算log
def cal_log(x):
    return np.log2(x + EPS)


# 遍历文件夹下的所有文件，包括子文件下的文件
def ergodic_file(path):
    file_list = []
    g = os.walk(path)
    for f_path, d, files in g:
        for filename in files:
            file_list.append(os.path.join(f_path, filename))
    return file_list


def mk_dir(path):
    """
    若目录不存在，则多层创建该目录
    :param path:目录
    """
    # 去除首位空格， 去除尾部 \ 符号
    path = path.strip().rstrip("\\").rstrip("/")
    # 判断路径是否存在
    is_exist = os.path.exists(path)
    # 判断结果
    if not is_exist:
        # 创建目录操作函数
        os.makedirs(path)


# 读取语料中的文本内容
def read_all_content(file):
    with open(file, encoding='utf8', errors='ignore') as f:
        return f.read()


# 初始化耗时初始时刻和耗时记录
def time_init():
    start_time = datetime.now()
    time_takes = {'start_time': start_time}
    return time_takes


# 记录时间段名称和耗时时间
def note_time_takes(key, time_takes):
    time_temp = datetime.now()
    if 'start_time' not in time_takes.keys():
        time_takes['start_time'] = time_temp
    t_stamp = (time_temp - time_takes['start_time']).total_seconds()
    time_takes[key] = time_takes.get(key, 0) + t_stamp
    time_takes['start_time'] = time_temp


# 加载二进制文件
def load_cache(cache_file_name):
    with open(cache_file_name, 'rb') as f:
        ds = pickle.load(f)
        return ds


# 持久化为二进制文件
def save_cache(cache, cache_file_name):
    with open(cache_file_name, 'wb') as f:
        pickle.dump(cache, f)


# 获取今天日期
def get_today():
    today = dt.date.today()
    return today


# 获取昨天日期
def get_yesterday():
    today = dt.date.today()
    one_day = dt.timedelta(days=1)
    yesterday = today-one_day
    return yesterday


# 返回当前的时间戳
def time_stamp():
    stamp = time.strftime("%Y%m%d%H%M%S", time.localtime())
    return stamp


def string2date(string, f='%Y-%m-%d'):
    """把字符串转成date"""
    return datetime.strptime(str(string), f).date()


def string2datetime(string, f='%Y-%m-%d %H:%M:%S'):
    """把字符串转成datetime"""
    return datetime.strptime(str(string), f)


def string2timestamp(str_time, f='%Y-%m-%d %H:%M:%S'):
    """把字符串转成时间戳形式"""
    return time.mktime(string2datetime(str_time, f).timetuple())


def timestamp2string(stamp, f='%Y-%m-%d %H:%M:%S'):
    """把时间戳转成字符串形式"""
    return time.strftime(f, time.localtime(stamp))


# 从指定文件拷贝部分文件到目标文件夹
def copy_files(files, sorce_path, targe_path):
    for file in files:
        if not os.path.isdir(targe_path):
            print('目标文件夹不存在')
        sorce_file = os.path.join(sorce_path, file)
        targe_file = os.path.join(targe_path, file)
        if os.path.isfile(sorce_file):
            open(targe_file, "wb").write(open(sorce_file, "rb").read())


def str_q2b(ustring):
    """全角转半角"""
    rstring = ""
    ustring = str(ustring)
    for uchar in ustring:
        inside_code = ord(uchar)
        if inside_code == 12288:
            inside_code = 32
        elif 65281 <= inside_code <= 65374:
            inside_code -= 65248

        rstring += chr(inside_code)
    return rstring


def read_file_md5(file_name):
    with open(file_name, 'r', encoding='utf-8') as f:
        text = f.readlines()
        md5 = hashlib.md5(str(text).encode('utf-8')).hexdigest()
        return text, md5


def json2dict(s):
    """
    json字符串转字典
    :param s: json字符串
    :return: 字典
    """
    d = json.loads(s)
    return d


def str_clean(ustring):
    rstring = ""
    ustring = str(ustring)
    rstring = ustring.replace('(<[^>]+>)', ' ')
    rstring = rstring.replace('(<.*?/>)', ' ')
    rstring = rstring.replace('&#110;', ' ')
    rstring = rstring.replace('&nbsp;', ' ')
    rstring = rstring.replace('  ', '')
    rstring = str_q2b(rstring)
    rstring = rstring.replace('  ', '')
    return rstring


# 最里层的字典
def dd1_float():
    return defaultdict(float)


# 最里层的字典
def dd1_list():
    return defaultdict(list)


# 最里层的字典
def dd1_set():
    return defaultdict(set)


# 第二层字典
def dd2(dd1):
    return defaultdict(dd1)


if __name__ == '__main__':
    ts = string2timestamp('2018-06-25 00:00:00')
    file_name = r'E:\项目\移动在线本部\词条匹配\rules\word_config.txt'
    text1, md51 = read_file_md5(file_name)