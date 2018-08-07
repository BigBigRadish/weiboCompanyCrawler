# -*- coding: utf-8 -*-
'''
Created on 2018年8月5日

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
import itertools
from pymongo import MongoClient
from http import cookiejar
comInfo=pd.read_csv('comInfo.csv')
#print(comInfo)
user_agent = 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64; rv:61.0) Gecko/20100101 Firefox/61.0'
headers={'User-Agent':user_agent}
requests.adapters.DEFAULT_RETRIES = 5
import re 
def weibo_repost(all_company_weibo,con,m):#获取每条微博的转发用户信息
    for i in all_company_weibo:
        companyNo=str(i['companyNo'])
        mblog_id=str(i['mblog_id'])
        weiboName=str(i['weiboName'])
        firstURL='https://m.weibo.cn/statuses/extend?id='+str(mblog_id)
        Url='https://m.weibo.cn/api/statuses/repostTimeline?id='+str(mblog_id)+'&page=1'
        logging.captureWarnings(True)
        r = requests.get(Url,headers=headers,verify=False)
        #_cookies=r.cookies#获取cookie
        #html=requests.get(Url,cookies=_cookies,headers=headers,verify=False);
        long_data=json.loads(r.text)#json的数据格式
        #print(long_data)
        if long_data['ok']==1:#‘ok’表示有数据
            create_time=str(long_data['data']['data'][0]['retweeted_status']['created_at'])
            total_num=int(long_data['data']['total_number'])
            page=int(total_num/10)
            if page==0:
                page=1
            if page>20:#最多抓取500个
                page=20
                for j in range(1,page):
                
                    Url='https://m.weibo.cn/api/statuses/repostTimeline?id='+str(mblog_id)+'&page='+str(j)
                    logging.captureWarnings(True)
                    html = requests.get(Url,headers=headers,verify=False)
        #            _cookies=r.cookies#获取cookie
        #             html=requests.get(Url,cookies=_cookies,headers=headers,verify=False);
                    long_long_data=json.loads(html.text)#json的数据格式 
                    try :
                        for k in long_long_data['data']['data']:
                            total_Num=str(total_num)
                            user_Id=str(k['user']['id'])
                            user_follow_count=str(k['user']['follow_count'])
                            user_follower_count=str(k['user']['followers_count'])
                            user_Gender=str(k['user']['gender'])
                            #following=str(k['user']['following'])
                            #print(following)
                            profile_url=str(k['user']['profile_url'])
                            user_screen_name=str(k['user']['screen_name'])
                            user_statuses_count=str(k['user']['statuses_count'])
                            connectMysql(con, weiboName,create_time, companyNo, mblog_id, user_screen_name, user_Id, user_follow_count, user_follower_count, user_Gender, total_Num, profile_url, user_statuses_count)
                    except KeyError:
                        #print(m)
                        break
        m+=1
                   
def connectMysql(connection,weiboName,create_time,companyNo,mblog_id,user_screen_name,user_Id,user_follow_count,user_follower_count,user_Gender,total_Num,profile_url,user_statuses_count):#连接数据库并插入数据
#获取会话指针
    with connection.cursor() as cursor:
#创建sql语句
        sql = "insert into `company_weibo_repost` (`weiboName`,`create_time`,`companyNo`,`mblog_id`,`user_screen_name`,`user_Id`,`user_follow_count`,`user_follower_count`,`user_Gender`,`total_Num`,`profile_url`,`user_statuses_count`) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
#执行sql语句
        cursor.execute(sql,(weiboName,create_time,companyNo,mblog_id,user_screen_name,user_Id,user_follow_count,user_follower_count,user_Gender,total_Num,profile_url,user_statuses_count))
#提交数据库
        connection.commit()    
if __name__ == '__main__':
    conn = pymysql.connect(host='127.0.0.1', user='root', password='147258', db='weibo_company', charset='utf8mb4')
    mongo_con=MongoClient('localhost', 27017)
    db=mongo_con.weiboCompany
    all_company_weibo=db.company_weibo_detail.find()
    m=1#监听爬取的进度
    try:
        weibo_repost(all_company_weibo,conn,m)
    except Exception:
        print(m)
    mongo_con.close()
    conn.close()