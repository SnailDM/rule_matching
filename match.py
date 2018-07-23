# -*- coding:utf-8 -*-
"""
复杂规则下的文本快速匹配
"""
import common as cm
import re
import json
import time


# 规则集合类
class RulesSet:
    def __init__(self, rules):
        self.rules = []
        self.labels = []
        self.keywords = []
        self.total_words = set()
        self.parse_rules(rules)
        self.lfreq = dict()
        index_structure(self.lfreq, self.total_words)

    def parse_rules(self, rules):
        for rule, rule_label in rules:
            words = re.split('[\+\-\(\)\|]', rule)
            keywords = set(words) - set([''])
            new_rule_compile = precompile(rule)
            self.rules.append(new_rule_compile)
            self.labels.append(rule_label)
            self.keywords.append(keywords)
            self.total_words |= keywords


# 根据是否符合规则来标记文本
def mark_text(text, rules_set):
    s_r = text_search(text, rules_set.lfreq)
    result = compute_rules(rules_set, s_r)
    return json.dumps(result, ensure_ascii=False)


# 从文本集合中检索关键字
def retrieval_texts(texts, keywords):
    res = []
    lfreq = index_structure(keywords)
    for text in texts:
        r = text_search(text, lfreq)
        res.append(list(r.keys()))
    return res


# 根据关键字检索结果计算规则表达式、
def compute_rules(rules_set, s_r):
    result = dict()
    for index, rule in enumerate(rules_set.rules):
        is_conform = eval(rule)
        if is_conform:
            hit_keys = [key for key in rules_set.keywords[index] if key in s_r]
            result[rules_set.labels[index]] = ' '.join(hit_keys)
    return result


# 从文本中检索存在的关键字
def text_search(content, lfreq):
    content = str(content)
    search_result = dict()
    N = len(content)
    k = 0
    while k < N:
        i = k + 1
        while i <= N:
            word = content[k:i]
            tag = lfreq.get(word, -1)
            if tag > 0:
                search_result[word] = True
            if tag < 0 or i == N:
                k += 1
                break
            i += 1
    return search_result


# 解析关键字集合成适合检索的结构  FREQ
def index_structure(lfreq, keywords):
    for word in keywords:
        lfreq[word] = 1
        for ch in range(len(word) - 1):
            wfrag = word[:ch + 1]
            if wfrag not in lfreq:
                lfreq[wfrag] = 0
    return lfreq


# 判断检索结果是否含有关键字
def is_exist(word, search_result):
    if word in search_result:
        return True
    else:
        return False
    # return search_result.get(word, False)


# 将关键字检索结果函数增加到规则字符串中
def transform_rule(rule, var_name):
    new_rule = ''
    N = len(rule)
    k = 0
    op_set = set(['+', '-', '|', '(', ')'])
    if rule[0] not in op_set:
        new_rule += 'is_exist("'
    while k < N:
        before_k = k - 1
        if before_k >= 0 and rule[before_k] in op_set and rule[k] not in op_set:
            new_rule += 'is_exist("'
        if before_k >= 0 and rule[before_k] not in op_set and rule[k] in op_set:
            new_rule += '", {})'.format(var_name)
        new_rule += rule[k]
        k += 1
    if rule[N-1] not in op_set:
        new_rule += '", {})'.format(var_name)
    return new_rule


def precompile(key_words_rule, var_name='s_r'):
    """
    预编译关键字规则，其中s_r为写死的关键字匹配结果变量名称
    :param key_words_rule:
    :param var_name:
    :return:
    """
    new_rule = transform_rule(key_words_rule, var_name)
    new_rule = new_rule.replace('+', ' and ').replace('-', ' and not ').replace('|', ' or ').strip().strip('and').strip()
    key_words_rule_compile = compile(new_rule, '', 'eval')
    return key_words_rule_compile


if __name__ == '__main__':
    # 测试数据
    start = time
    rules1 = [('(招标|投标|采购|集采|中标|应标|租赁|升级|改造|租用)+((远程监控系统|无线集群通讯系统|通讯总线系统|通讯调度子系统|通讯指挥调度|应急通信设备|无线电通信系统|实时通信系统|通信传输系统|无线集群通信系统|无线语音集群|通信调度子系统|通信指挥调度|电信级设备管理和维护|云计算|移动办公|外接存储设备|系统集成|板卡配置)|((IDC|数据)+(专业级机房|数据存放|服务器托管|租赁|服务器代维|服务系状态远程监控|IP-KVM|宽带出租|公网IP地址出租|主机托管|带宽出租|VIP机房租赁|IP地址出租及云计算服务|体系建设|数据平台|网建设|网交|网络共享|信息平台|信息网|信息网络|虚拟|oa|运维|电子政务|管线|互联互通|互通|监测网|监测网络|监控室|拘留所|门户网|门户网站|周界|综合布线|指挥|全防|中国有线)))-((中国移动采购与招标网|中国联通)+电信)', 'rule4'),
              ('(招标|投标|采购|集采|中标|应标|租赁|升级|改造|租用)+((远程监控系统|无线集群通讯系统|通讯总线系统|通讯调度子系统|通讯指挥调度|应急通信设备|无线电通信系统|实时通信系统|通信传输系统|无线集群通信系统|无线语音集群|通信调度子系统|通信指挥调度|电信级设备管理和维护|云计算|移动办公|外接存储设备|系统集成|板卡配置)|((IDC|数据)+(专业级机房|数据存放|服务器托管|租赁|服务器代维|服务系状态远程监控|IP-KVM|宽带出租|公网IP地址出租|主机托管|带宽出租|VIP机房租赁|IP地址出租及云计算服务|体系建设|数据平台|网建设|网交|网络共享|信息平台|信息网|信息网络|虚拟|oa|运维|电子政务|管线|互联互通|互通|监测网|监测网络|监控室|拘留所|门户网|门户网站|周界|综合布线|指挥|全防|中国有线)))-((中国移动采购与招标网|中国联通)+电信)', 'rule2')]
    text1 = cm.read_all_content(r'E:\项目\移动在线本部\网罗天下\商机规则匹配\文本.txt')
    rules_set1 = RulesSet(rules1)
    # json1 = mark_text(text1, rules_set1)
    res1 = retrieval_texts([text1]*10, rules_set1.total_words)
    print(res1)










