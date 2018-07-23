"""
脚本：舆情数据的实时标注程序
主要功能描述：规则集合的实时更新（每加载一次文本数据，就更新一次规则集合）。
"""
import corpus as cp
from Logger import Logger
import logging
import os
import common as cm
from config import configs
from rule import Processor
import cache
import traceback
import datetime as dt
import csv
import shutil

# 初始化日志类
Logger(logname=os.path.join(cm._get_abs_path('log'), 'match.log'), loglevel=1, logger="main")
logger = logging.getLogger('main')


def main(p_id):
    logger.info('程序启动。')
    """
    实时标注的主函数
    """
    # 获取启动日期
    start_date = cm.string2date(configs['date']['start_date'])
    # 初始化全局缓存
    global_cache = cache.load_global_cache(p_id)
    # 初始化文本处理器
    pro = Processor(configs['type']['news_type_set'])
    while True:
        try:
            # step0: 如果在新的一天开始时，清理一些缓存数据
            # 获取待处理文件
            processed_files = global_cache.get('processed_files', set())
            # 获取监控日期
            monitor_date = global_cache.get('monitor_date', start_date)

            # step1: 加载爬虫文件
            spider_df, spider_file = cp.load_spider_df(configs['path']['monitor'], monitor_date, processed_files)

            # step2: 加载规则库
            # 每处理一个文件前，先加载最新规则
            pro.load_total_rules(global_cache)

            # step3: 对文件每行进行打标签
            logger.info('开始处理文件{}。'.format(spider_file.full_path))
            # 对文件中的每一条数据进行打标签
            # 测试直接爬虫文件时需要增加的代码

            # 测试直接爬虫文件时需要增加的代码
            # spider_df['date'] = spider_df.apply(lambda row: str(dt.datetime.fromtimestamp(int(row['date']))), axis=1)
            # spider_df['ffdCreate'] = spider_df.apply(lambda row: str(dt.datetime.fromtimestamp(int(row['ffdCreate']))), axis=1)
            # spider_df.loc[i, 'keyword'], spider_df.loc[i, 'businessType'] = pro.mark(spider_df.iloc[i])  # 要修改
            # 数据和规则都不为空的情况下，才处理数据
            # 如果数据不为空
            if len(spider_df) > 0:
                # 如果规则不为空
                if len(pro.title_lfreq) > 0 or len(pro.content_lfreq) > 0:
                    spider_df[['keyword', 'businessType']] = \
                        spider_df.apply(lambda row: pro.mark(row['title'], row['content'],
                                                             row['recType'], row['ffdCreate']), axis=1)
                else:
                    spider_df['keyword'] = ''
                    spider_df['businessType'] = ''

            # step4: 保存结果文件
            out_temp_file_path = configs['path']['temp_out']
            out_file_path = os.path.join(configs['path']['out'], spider_file.parent_folder.name)
            # 创建临时输出目录
            cm.mk_dir(out_temp_file_path)  # 创建临时文件所在的目录
            spider_df.to_csv(os.path.join(out_temp_file_path, spider_file.name), sep='|', header=True, index=False,
                             encoding='utf-8', quoting=csv.QUOTE_NONE, escapechar='\\')
            # 将临时输出文件移到正式输出文件夹中
            cm.mk_dir(out_file_path)  # 创建正式文件所在的目录
            shutil.move(os.path.join(out_temp_file_path, spider_file.name), os.path.join(out_file_path, spider_file.name))
            # 测试单个文件效率
            # break

            # 当前的处理的文件加入缓存记录
            processed_files.add(spider_file)
            # 如果已经开始处理比较近期的文件，则将历史的一些缓存删除
            expected_monitor_date = spider_file.parent_folder.date - dt.timedelta(days=configs['date']['monitor_days'])
            if expected_monitor_date > monitor_date:
                processed_files = {file for file in processed_files if file.parent_folder.date >= expected_monitor_date}
                global_cache.monitor_date = expected_monitor_date
                # 清理监控日期之前的空文件夹
                cp.clean_empty_folder(configs['path']['monitor'], expected_monitor_date)
            global_cache.processed_files = processed_files
            # step5: 移除处理完成的文件
            # 测试效率
            # sys.exit(0)
            os.remove(spider_file.full_path)
            logger.info('共处理{}条数据，已生成结果文件{}，并将临时文件删除成功。'.format(len(spider_df),
                                                                os.path.join(out_file_path, spider_file.name)))
            # step6: 释放资源
            # 内存监测
        except Exception as e:
            logger.error('舆情数据流式处理出错。')
            logger.error(traceback.format_exc())


if __name__ == '__main__':
    try:
        main('20180716_2')
    except Exception as e:
        logger.error(traceback.format_exc())


