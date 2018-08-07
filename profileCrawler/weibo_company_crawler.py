# -*- coding: utf-8 -*-
'''
Created on 2018年8月3日

@author: Zhukun Luo
Jiangxi university of finance and economics
'''
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import threading
import requests
import logging 
import urllib.request;
import time
import random
import json
import re
import redis
import pymysql 
from pymongo import MongoClient
from http import cookiejar
comInfo=pd.read_csv('comInfo.csv')
#print(comInfo)
user_agent = 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64; rv:61.0) Gecko/20100101 Firefox/61.0'
headers={'User-Agent':user_agent}
requests.adapters.DEFAULT_RETRIES = 5
import re 
def compId(collection):
    for i in comInfo['Id']:
        #print(i)
        i= str(i).strip()
        compUrl='https://m.weibo.cn/api/container/getIndex?type=uid&value='+i
        logging.captureWarnings(True)
        r = requests.get(compUrl,headers=headers,verify=False)
        _cookies=r.cookies#获取cookie
        html=requests.get(compUrl,cookies=_cookies,headers=headers,verify=False);
        long_data=json.loads(html.text)#json的数据格式
        id=i
        screen_name=str(long_data['data']['userInfo']['screen_name'])#微博名
        profile_url=str(long_data['data']['userInfo']['profile_url'])#个人主页
        state_count=str(long_data['data']['userInfo']['statuses_count'])#状态统计
        description=str(long_data['data']['userInfo']['description'])#description
        company_gender=str(long_data['data']['userInfo']['gender'])#gender
        followers_count=str(long_data['data']['userInfo']['followers_count'])#followers_count
        follow_count=str(long_data['data']['userInfo']['follow_count'])#follow_count
        fans_scheme=str(long_data['data']['fans_scheme'])#fans_scheme
        follow_scheme=str(long_data['data']['follow_scheme'])#follow_scheme
        article_scheme=str(long_data['data']['userInfo']['toolbar_menus'][2]['scheme'])#article_scheme
        company_profile_containerId=str(long_data['data']['tabsInfo']['tabs'][0]['containerid'])#profile_containerId
        company_weibo_containerId=str(long_data['data']['tabsInfo']['tabs'][1]['containerid'])#weibo_containerId
        company_video_containerId=str(long_data['data']['tabsInfo']['tabs'][2]['containerid'])#video_containerId
#         print(company_video_containerId)
#         print(article_scheme)
        companyDetail={'companyId':id,'weiboName':screen_name,'profile_url':profile_url,'state_count':state_count,'company_description':description,'company_gender':company_gender,'followers_count':followers_count,'follow_count':follow_count,'fans_scheme':fans_scheme,'follow_scheme':follow_scheme,'article_scheme':article_scheme,'company_profile_containerId':company_profile_containerId,'company_weibo_containerId':company_weibo_containerId,'company_video_containerId':company_video_containerId}
        collection.insert(companyDetail)
def collect_weibo_detail(companyDetail,collection):
     for index,i in companyDetail.iterrows():
        #print(i)
        weiboname=str(i['weiboName']).strip()
        companyNo= str(i['companyId']).strip()
        company_weibo_containerId=str(i['company_weibo_containerId'])
        compUrl='https://m.weibo.cn/api/container/getIndex?type=uid&value='+companyNo+'&containerid='+company_weibo_containerId+'&page=1'
        logging.captureWarnings(True)
        r = requests.get(compUrl,headers=headers,verify=False)
        _cookies=r.cookies#获取cookie
        html=requests.get(compUrl,cookies=_cookies,headers=headers,verify=False);
        long_data=json.loads(html.text)#json的数据格式
        #print(long_data['data'])
        weibo_count=int(long_data['data']['cardlistInfo']['total'])#每个用户总微博数量
        if(weibo_count<50):
            page_count=int(weibo_count/10+1)
        else:
            page_count=int(weibo_count/40)
        for j in range(page_count):
            url='https://m.weibo.cn/api/container/getIndex?type=uid&value='+companyNo+'&containerid='+company_weibo_containerId+'&page='+str(j)
            logging.captureWarnings(True)
            r = requests.get(url,headers=headers,verify=False)
            _cookies=r.cookies#获取cookie
            html1=requests.get(url,cookies=_cookies,headers=headers,verify=False);
            long_long_data=json.loads(html1.text)#json的数据格式
            for i in  long_long_data['data']['cards']:
                weiboName=weiboname#weiboname
                #weibo_count=weibo_count#微博总数
                page=j#多少页
                itemid=i['itemid']#itemid
                weibo_scheme=i['scheme']#weibo URL
                mblog_id=i['mblog']['id']#mblog Id
                mblog_create_time=i['mblog']['created_at']#创建时间
                mblog_text=i['mblog']['text']#weibo 文本内容
                mblog_source=i['mblog']['source']#微博来源
                repost_count=i['mblog']['reposts_count']#微博转发数量
                comment_count=i['mblog']['comments_count']#weibo评论数量
                attitude_count=i['mblog']['attitudes_count']#weibo点赞数量
                mblog_bid=i['mblog']['bid']#微博 Bid
                company_weibo_detail={'weiboName':weiboName,'companyNo':companyNo,'company_weibo_containerId':company_weibo_containerId,'page':page,'itemid':itemid,'weibo_scheme':weibo_scheme,'mblog_id':mblog_id,'mblog_create_time':mblog_create_time,'mblog_text':mblog_text,'mblog_source':mblog_source,'repost_count':repost_count,'comment_count':comment_count,'attitude_count':attitude_count,' mblog_bid': mblog_bid}
                collection.insert(company_weibo_detail)
if __name__ == '__main__':
    mongo_con=MongoClient('localhost', 27017)
    db=mongo_con.weiboCompany
#     collection=db.companyDetail    
#     compId(collection)
    companyDetail=pd.read_csv('companyDetail.csv')
    collection=db.company_weibo_detail
    collect_weibo_detail(companyDetail,collection)
