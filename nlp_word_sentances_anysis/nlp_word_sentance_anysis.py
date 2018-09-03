# -*- coding: utf-8 -*-
'''
Created on 2018年8月14日

@author: Zhukun Luo
Jiangxi university of finance and economics
'''
import os
LTP_DATA_DIR = 'D:\LTP\MODEL\ltp_data'  # ltp模型目录的路径
cws_model_path = os.path.join(LTP_DATA_DIR, 'cws.model')  # 分词模型路径，模型名称为`cws.model`
pos_model_path = os.path.join(LTP_DATA_DIR, 'pos.model')  # 词性标注模型路径，模型名称为`pos.model`
ner_model_path = os.path.join(LTP_DATA_DIR, 'ner.model')  # 命名实体识别模型路径，模型名称为`pos.model`
par_model_path = os.path.join(LTP_DATA_DIR, 'parser.model')  # 依存句法分析模型路径，模型名称为`parser.model`
srl_model_path = os.path.join(LTP_DATA_DIR,'pisrl_win.model')  # 语义角色标注模型目录路径，模型目录为`srl`。注意该模型路径是一个目录，而不是一个文件。
import pandas as pd
import numpy as np
import re
import nltk
import gensim 
import pyltp
from pyltp import SentenceSplitter
from pyltp import Segmentor
from pyltp import Postagger
from pyltp import NamedEntityRecognizer
from pyltp import Parser
from pyltp import SementicRoleLabeller
#分句，也就是将一片文本分割为独立的句子
def sentence_splitter(sentence):
    sents = SentenceSplitter.split(sentence)  # 分句
    sent=''
    for i in sents:
        i=re.sub("[\s+\.\!\/_,$%^*(+\"\')]+|[+——()?【】“”！，。？、~@#￥%……&*（）]+", "",str(i))
        sent+=i+' '
    #print(sent)
    return sent
 
#分词
def segmentor(sentence):
    segmentor = Segmentor()  # 初始化实例
    segmentor.load(cws_model_path)  # 加载模型
    words = segmentor.segment(sentence)  # 分词
    #默认可以这样输出
    #print ('\t'.join(words))
    # 可以转换成List 输出
    words_list = list(words)
    segmentor.release()  # 释放模型
    return words_list
#词性标注 
def posttagger(words):
    postagger = Postagger() # 初始化实例
    postagger.load(pos_model_path)  # 加载模型
    postags = postagger.postag(words)  # 词性标注
    word_postag=[]
    for word,tag in zip(words,postags):
        word_postag.append((word,tag))
    #postags=list(postags) 
    postagger.release()  # 释放模型
    return postags,word_postag
 
#命名实体识别
def ner(words, postags):
    recognizer = NamedEntityRecognizer() # 初始化实例
    recognizer.load(ner_model_path)  # 加载模型
    netags = recognizer.recognize(words, postags)  # 命名实体识别
    word_neg=[]
    for word,tag in zip(words,netags):
        word_neg.append((word,tag))
    recognizer.release()  # 释放模型
    return netags,word_neg
 
#依存语义分析
def parse(words, postags):
    parser = Parser() # 初始化实例
    parser.load(par_model_path)  # 加载模型
    arcs = parser.parse(words, postags)  # 句法分析
    print ("\t".join("%d:%s" % (arc.head, arc.relation) for arc in arcs))
    parser.release()  # 释放模型
    return arcs
 
#角色标注
def role_label(words, postags, netags, arcs):
    labeller = SementicRoleLabeller() # 初始化实例
    labeller.load(srl_model_path)  # 加载模型
    roles = labeller.label(words, postags, netags, arcs)  # 语义角色标注
    for role in roles:
        print (role.index, "".join(
            ["%s:(%d,%d)" % (arg.name, arg.range.start, arg.range.end) for arg in role.arguments]))
    labeller.release()  # 释放模型
    return roles
    
if __name__ == '__main__':
    f=open('word_flag_100+.txt','a')
    f.write('word'+','+'flag'+'\n')
    #words_flag = pd.DataFrame(columns=['word','flag'])
    mblog_text=pd.read_csv('all_company_weibo_2_101+.csv')
    for i in mblog_text['mblog_text']:
        contents=''
        #contents=''
        i=str(i)
        contents+=str(sentence_splitter(i))
        #print(contents)
        #print(contents)
        word_list=segmentor(contents)#分词
        postag,words_tag=posttagger(word_list)
        #word_list=pd.DataFrame(word_list)
        for word, tag in words_tag:
            f.write(str(word)+','+str(tag)+'\n')            
    f.close
    
    
    