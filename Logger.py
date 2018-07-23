"""
日志管理类，可以将信息同时打印到控制台和日志文件中。
"""
import logging
from logging.handlers import TimedRotatingFileHandler


# 用字典保存日志级别
format_dict = {
    1: logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
    2: logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
    3: logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
    4: logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
    5: logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
}


# 开发一个日志系统， 既要把日志输出到控制台， 还要写入日志文件
class Logger:
    def __init__(self, logname, loglevel, logger):
        """
           指定保存日志的文件路径，日志级别，以及调用文件
           将日志存入到指定的文件中
        """

        # 创建一个logger
        self.logger = logging.getLogger(logger)
        self.logger.setLevel(logging.DEBUG)

        # 创建一个handler，用于写入日志文件
        # 每秒写一个日志文件
        # rf_handler = logging.handlers.TimedRotatingFileHandler(filename=logname, when="S", interval=1, backupCount=20)
        # rf_handler.suffix = "%Y-%m-%d_%H-%M-%S.log"

        # 每天写一个日志文件
        rf_handler = TimedRotatingFileHandler(filename=logname, when="D", interval=1, backupCount=20)
        rf_handler.suffix = "%Y-%m-%d.log"
        rf_handler.setLevel(logging.DEBUG)
        # fh = logging.FileHandler(logname)
        # fh.setLevel(logging.DEBUG)

        # 再创建一个handler，用于输出到控制台
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # 定义handler的输出格式
        # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        formatter = format_dict[int(loglevel)]
        rf_handler.setFormatter(formatter)
        ch.setFormatter(formatter)

        # 给logger添加handler
        self.logger.addHandler(rf_handler)
        self.logger.addHandler(ch)

    def get_log(self):
        return self.logger

