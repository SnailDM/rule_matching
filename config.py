"""
配置文件
"""


configs = {
    'path': {
        'monitor': r'E:\data\webspider',  # 监测新文件产生的路径
        'temp_out': r'E:\data\webspider_temp_out',
        'out': r'E:\data\webspider_out',  # 结果文件的保存路径
        'word_config': r'E:\项目\移动在线本部\词条匹配\rules\test_config.json',  # 词条配置文件的位置
        'word_config_txt': r'E:\项目\移动在线本部\词条匹配\rules\client_config_entry.txt',  # 前端系统同步过来的文件路径
    },
    'date': {
        'start_date': '2018-7-12',   # 程序启动日期
        'sleep_time': 2,  # 程序在没有新文件产生时的休眠时间 秒
        'monitor_days': 2  # 监控多少天内的爬虫文件
    },
    'type': {
        'news_type_set': {'1', '6'}  # 程序要处理的新闻类型，主要用来筛选对应的词条。1：新闻，2：微博，3：微信，4：app，5：论坛，6：报刊，7：视频，8：博客
    },
    'db_conn': {
        'host': '192.168.100.181',
        'database': 'netpomdb_dev',
        'user': 'netpomadmin',
        'password': '8rQSql27odWWG0K',
        'port': 41000,
        'charset': 'utf8'
    },
    'db_table': {
        'word_config_table': 'client_config_entry'
    },
    'debug': {
        'word_config_debug': 0  # 0:采用写死的json测试文件，作为词条配置。 1： 采用生产环境中的mysql读取此调配配置
    }


}
