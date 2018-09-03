# -*- coding: utf-8 -*-
'''
Created on 2018年8月13日

@author: Zhukun Luo
Jiangxi university of finance and economics
'''
import pandas as pd
import numpy as np
import re
import nltk
import gensim 
import pyltp
def preprocessText():
    data=pd.read_csv('all_company_weibo_1.csv')
    list=[]
    for i in data['mblog_text']:
        reg = re.compile('<[^>]*>')
        if reg is True:  
            str_1= reg.sub(' ',str(i))+' ，这是转发或图片'          
            str_2 = re.sub(r"[\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）]+【】?",'',str_1)
            list.append(str_2)
        else:
            str_1= reg.sub(' ',str(i))        
            str_2 = re.sub(r"[\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）]+【】?",'',str(str_1))
            list.append(str_2)
    data['mblog_text']=list 
    print(data['mblog_text']) 
    data.to_csv('../all_company_weibo_2.csv')     
if __name__ == '__main__':
    
    preprocessText()
    
        