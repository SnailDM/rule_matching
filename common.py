"""
一些通用方法
"""
from collections import defaultdict
import os
from datetime import datetime
import numpy as np
import pickle
import time

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


# 返回当前的时间戳
def time_stamp():
    stamp = time.strftime("%Y%m%d%H%M%S", time.localtime())
    return stamp


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

