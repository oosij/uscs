## import load !
import pandas as pd
import schedule
import openpyxl
import re
import requests
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service
#from webdriver_manager.chrome import ChromeDriverManager

import datetime
from datetime import datetime as dtime
from dateutil.parser import parse
from time import strptime
import pytz

import nltk
from nltk.corpus import stopwords
from nltk.tag import pos_tag
from nltk.tokenize import word_tokenize
from nltk.tokenize import WordPunctTokenizer
from nltk.stem import WordNetLemmatizer
from collections import Counter
from nltk import sent_tokenize

import time
import pymysql

#from Press_List import *
from Google_News_Search import google_etf_news_crawler_main


# 아마존 DB TEST 접속
#db = pymysql.connect(host='mymastertestdb.cwkjjkurliut.ap-northeast-2.rds.amazonaws.com',  port = 3306 , user = 'contest_manager',
#                           password = "thinkmanager6387", db = 'invest', charset = 'utf8')


# 로컬 DB TEST 접속
db = pymysql.connect(host='127.0.0.1',  port = 3306 , user = 'root',
                           password = "1234", db = 'jisoo', charset = 'utf8')


headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
        'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}


## 자동화 테스트
def etf_news_main():
        # 로컬 DB TEST 접속
        db = pymysql.connect(host='127.0.0.1', port=3306, user='root',
                             password="1234", db='jisoo', charset='utf8')
        # 시간 재기용
        start = time.time()
        try:
                google_etf_news_crawler_main(db)
        except:
                print('error!')
                time.sleep(10)
        print("WorkingTime: {} sec".format(time.time() - start))  # 현재시각 - 시작시간 = 실행 시간


print('===== 미국 ETF 뉴스 크롤링 中 =====')
google_etf_news_crawler_main(db)

# 10분에 한번씩 함수 실행
schedule.every(60).minutes.do(etf_news_main)


# 무한 반복
while True:
    schedule.run_pending()
    time.sleep(1)