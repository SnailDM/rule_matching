"""
定义基础规则的结构（一级二级），规则的解析
"""
import match
import common as cm
import re
import logging
import traceback
from config import configs
import json
import pandas as pd
import db_handle

logger = logging.getLogger('main.rule')


def parse_word_config(word_config, news_type_set):
    """
    解析词条配置文件
    :return: 一级词条，二级业务分类
    """
    rule_1 = []
    rule_2 = []
    for i, rule_str in enumerate(word_config):
        try:
            rule_dict = cm.json2dict(rule_str)
            rule_news_type_set = set(re.split(r'[\,，\s]', str(rule_dict['NEWS_TYPE'])))
            if len(rule_news_type_set & news_type_set) <= 0:
                continue
            rule_tmp = Rule(rule_dict['ID'], rule_dict['NAME'], rule_dict['NEWS_TYPE'],
                            rule_dict['APPLY_TYPE'], rule_dict['TITLE_KEYWORD'], rule_dict['TITLE_NOT_KEYWORD'],
                            rule_dict['CONTENT_KEYWORD'], rule_dict['CONTENT_NOTKEYWORD'], rule_dict['MATCH_TYPE'],
                            rule_dict['TENANT_ID'], rule_dict['START_TIME'], rule_dict['END_TIME'])
            # 实时监测和预警都作为一级词条
            if str(rule_tmp.apply_type) == str(0) or str(rule_tmp.apply_type) == str(1):
                rule_1.append(rule_tmp)
            if str(rule_tmp.apply_type) == str(2):
                rule_2.append(rule_tmp)
        except Exception as e:
            logger.error('第{}条词条规则解析出错。其内容为：{}'.format(i+1, rule_str))
            logger.error(traceback.format_exc())
    return rule_1, rule_2


def split_rules_str(rule_list, field):
    keywords = set()
    for rule in rule_list:
        words_set = split_str(rule.__dict__[field])
        keywords |= words_set
    return keywords


def split_str(rule_str):
    words = re.split('[\+\-\(\)\|]', rule_str)
    words_set = set(words) - set([''])
    return words_set


def compute_hit_key(title_r, content_r, rule):
    """计算命中关键字"""
    return list((title_r.keys() & rule.title_keyword_set) | (content_r.keys() & rule.content_keyword_set))


def eval_opt(dict_r, keyword_set, rule_precompile, is_exist, title_r, content_r):
    """
    优化eval，在前提不满足时直接返回false
    """
    if set(dict_r) & keyword_set:
        return eval(rule_precompile)
    else:
        return False


def format_result(result):
    """格式化词条匹配结果字符串"""
    result_str = ''
    if len(result) > 0:
        result_str = json.dumps({'keywords': result}, ensure_ascii=False)
        result_str = result_str.strip('{').strip('}')
        # result_str = '"keywords":{}'.format(json.dumps(result, ensure_ascii=False))
    return result_str


def combine_key_words_rule(keyword, not_key_word):
    """
    合并关键字和排除关键，组合成完整的规则表达式
    :param keyword:关键字
    :param not_key_word:排除关键字
    :return:规则表达式
    """
    rule_str = ''
    if str(not_key_word).strip() != '':
        rule_str += '-({})'.format(str(not_key_word).strip())
    if str(keyword).strip() != '':
        rule_str += '+({})'.format(str(keyword).strip())
    if rule_str == '':
        rule_str = 'False'
    return rule_str


class Rule:
    """
    规则类，用于存储具体的规则信息，及底层的规则操作
    """
    def __init__(self, id, name, news_type, apply_type, title_keyword, title_not_key_word,
                 content_keyword, content_not_key_word, match_type, tenant_id, start_time, end_time):
        # 词条id
        self.id = id
        # 词条标签
        self.name = name
        # 信源类型：1、新闻，2微博....
        # 需要确认是否和爬虫爬下的类型一一对应
        # 是否可以存在多个类型:可以，以，隔开
        self.news_type = news_type
        self.news_type_set = set(re.split(r'[\,，\s]', str(news_type)))
        # 0:实时监控，1：预警，2：业务类型
        self.apply_type = apply_type
        # 标题关键字规则
        self.title_keyword = '' if title_keyword is None else title_keyword
        # 标题排除关键字规则
        self.title_not_key_word = '' if title_not_key_word is None else title_not_key_word
        # 内容关键字规则
        self.content_keyword = '' if content_keyword is None else content_keyword
        # 内容排除关键字规则
        self.content_not_key_word = '' if content_not_key_word is None else content_not_key_word
        # 匹配类型，0：标题匹配，1：内容匹配，2：标题或者内容匹配
        self.match_type = str(match_type)
        # 租户id
        self.tenant_id = tenant_id
        # start_time
        self.start_time = str(start_time)
        # end_time
        self.end_time = str(end_time)
        # 标题关键字集合，用来计算命中关键字
        self.title_keyword_set = split_str(self.title_keyword)
        # 标题组合关键字规则和排除关键字规则 # '-({})+({})'.format(title_not_key_word, title_keyword)
        self.title_key_words_rule = combine_key_words_rule(self.title_keyword, self.title_not_key_word)
        # 标题关键字集合，用来计算命中关键字
        self.content_keyword_set = split_str(self.content_keyword)
        # 内容组合关键字规则和排除关键字规则 # '-({})+({})'.format(content_not_key_word, content_keyword)
        self.content_key_words_rule = combine_key_words_rule(self.content_keyword, self.content_not_key_word)
        # 预编译标题关键字规则
        self.title_key_words_rule_precompile = match.precompile(self.title_key_words_rule, 'title_r')
        # 预编译内容关键字规则
        self.content_key_words_rule_precompile = match.precompile(self.content_key_words_rule, 'content_r')

    # 先决条件判断,主要判断信源类型和发布时间和词条的配置是否一致
    def precondition(self, rectype, ffdcreate):
        if not str(rectype).strip() in self.news_type_set:
            return False
        if not self.end_time >= ffdcreate >= self.start_time:
            return False

        return True

    def entries_match(self, title_r, content_r, is_exist):
        """
        词条匹配：包括标题和内容的组合匹配
        :return:
        """
        if self.match_type == '0':
            return eval_opt(title_r, self.title_keyword_set, self.title_key_words_rule_precompile,
                            is_exist, title_r, content_r)
        elif self.match_type == '1':
            return eval_opt(content_r, self.content_keyword_set, self.content_key_words_rule_precompile,
                            is_exist, title_r, content_r)
        elif self.match_type == '2':
            return eval_opt(title_r, self.title_keyword_set, self.title_key_words_rule_precompile,
                            is_exist, title_r, content_r) or eval_opt(content_r, self.content_keyword_set,
                                                                      self.content_key_words_rule_precompile,
                                                                      is_exist, title_r, content_r)
        else:
            return False


class RulesManage:
    """
    规则的管理类，加载、更新、清除（）
    """
    def __init__(self, news_type_set):
        """
        初始化：保存规则的变量
        """
        # 处理的新闻类型，用来筛选加载的规则类型
        self.news_type_set = news_type_set
        # 标题关键字集合
        self.title_key_words_set = set()
        # 内容关键字集合
        self.content_key_words_set = set()
        # 标题关键字前缀词典
        self.title_lfreq = dict()
        # 标题关键字前缀词典
        self.content_lfreq = dict()
        # 一级规则数组
        self.level_1_rule_list = []
        # 二级规则字典，key为租户id
        self.level_2_rule_dict = cm.dd1_list()
        # 是否已加载规则
        self.is_loaded = False

    def load_total_rules(self, word_config):
        """
        每天加载一次规则
        :return:
        """
        # 加载1级词条规则，和2级业务规则
        self.level_1_rule_list, r_2_list = parse_word_config(word_config, self.news_type_set)
        self._add_level2_rules(r_2_list)
        # 添加标题关键字字典
        self.title_key_words_set |= split_rules_str(self.level_1_rule_list + r_2_list, 'title_key_words_rule')
        # 添加内容关键字字典
        self.content_key_words_set |= split_rules_str(self.level_1_rule_list + r_2_list, 'content_key_words_rule')
        # 将关键字集合加入到前缀字典中
        match.index_structure(self.title_lfreq, self.title_key_words_set)
        match.index_structure(self.content_lfreq, self.content_key_words_set)

    def _add_level2_rules(self, level2_rules):
        """
        添加二级规则，支持追加
        :param level2_rules:
        :return:
        """
        for rule in level2_rules:
            self.level_2_rule_dict[rule.tenant_id].append(rule)


class Processor(RulesManage):
    """
    处理器类：计算文本流的标签符合情况
    """

    def mark(self, title, content, rectype, ffdcreate):
        """
        给当前记录进行标记
        :return: 标签值
        """
        # 文本多模式匹配，默认微博、新闻内容字段均为'content'。
        try:
            label1_list = []
            label2_list = []
            # s_r、is_exist 变量名是写死在预编译代码中的，不能更改其名称
            title_r = match.text_search(title, self.title_lfreq)
            content_r = match.text_search(content, self.content_lfreq)
            if len(title_r) == 0 and len(content_r) == 0:
                return pd.Series({'a': format_result([]), 'b': format_result([])})
            is_exist = match.is_exist
            # 已经遍历过的租户id，不再重复遍历
            tenant_id_set_matched = set()
            # 规则解析
            for rule1 in self.level_1_rule_list:
                if rule1.precondition(rectype, ffdcreate) and rule1.entries_match(title_r, content_r, is_exist):
                    # 命中关键字
                    rule1_hit_key = compute_hit_key(title_r, content_r, rule1)
                    label1_list.append({'eid': rule1.id, 'tenantid': rule1.tenant_id, 'words': rule1_hit_key})
                    if rule1.tenant_id in tenant_id_set_matched:
                        continue
                    else:
                        tenant_id_set_matched.add(rule1.tenant_id)
                    # 判断同一租户下的业务分类标签规则是否符合
                    for rule2 in self.level_2_rule_dict.get(rule1.tenant_id, []):
                        if rule2.precondition(rectype, ffdcreate) and rule2.entries_match(title_r, content_r, is_exist):
                            # 命中关键字
                            rule2_hit_key = compute_hit_key(title_r, content_r, rule2)
                            label2_list.append({'eid': rule2.id, 'tenantid': rule2.tenant_id, 'words': rule2_hit_key})

            #  通过'a', 'b' 来进行列排序
            return pd.Series({'a': format_result(label1_list), 'b': format_result(label2_list)})
        except Exception as e:
            logger.error('标题为{}的记录, 词条匹配失败'.format(title))
            logger.error(traceback.format_exc())
            return pd.Series({'a': format_result([]), 'b': format_result([])})

    def load_total_rules(self, global_cache):
        """
        重写继承的方法。
        每天重新加载一次规则
        :param global_cache: 加载日期，记录到缓存中
        :return:
        """
        # 从mysql中读取最新的word_config
        if configs['debug']['word_config_debug']:
            try:
                db_handle.read_word_config(configs['db_conn'], configs['path']['word_config'],
                                           configs['db_table']['word_config_table'], configs['type']['news_type_set'])
            except Exception as e:
                logger.error('读取mysql中的词条配置信息错误.')
                logger.error(traceback.format_exc())

        #  读取规则测试文件，后续正式上线要改成从mysql中读取
        word_config, md5 = cm.read_file_md5(configs['path']['word_config'])
        # 判断是否加载过配置文件和是否加载
        if not self.is_loaded or md5 != global_cache.get('word_config_md5', ''):
            RulesManage.load_total_rules(self, word_config)
            self.is_loaded = True
            # 缓存记录加载日期
            global_cache.word_config_md5 = md5
            logger.info('成功加载最近规则。')


if __name__ == '__main__':
    # l1, l2 = build_rule_example()
    pass


