"""
词条匹配：针对舆情分析中标签的复杂文本匹配
特点：增加中心词汇、距离、前配置项、后配置项、前不配置项、后不配置项的概念
问题：如何将二级标签（业务标签）和词条标签融合在一起
"""


class Condition:
    """
    条件类超类，多个条件组合可成为规则
    """
    def __init__(self):
        """
        条件初始化
        """
        pass

    def verify(self):
        """
        非条件验证
        :return: True or False
        """
        pass


# 从文本中检索存在的关键字
def text_search(content, lfreq):
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
