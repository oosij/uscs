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
from webdriver_manager.chrome import ChromeDriverManager

import datetime
from datetime import datetime as dtime
from dateutil.parser import parse
from time import strptime

import nltk
from nltk.corpus import stopwords
from nltk.tag import pos_tag
from nltk.tokenize import word_tokenize
from nltk.tokenize import WordPunctTokenizer
from nltk.stem import WordNetLemmatizer
from collections import Counter
from nltk import sent_tokenize
#nltk.download('punkt')
#nltk.download('stopwords')
#nltk.download('wordnet')
#nltk.download('omw-1.4')
#nltk.download('averaged_perceptron_tagger')
import time
import pymysql

## module !! US_News_func
import US_News_func
from US_News_func import *

# 로컬 DB TEST 접속
#db = pymysql.connect(host='127.0.0.1',  port = 3306 , user = 'root',
#                           password = "1234", db = 'jisoo', charset = 'utf8')

# 아마존 DB TEST 접속
db = pymysql.connect(host='mymastertestdb.cwkjjkurliut.ap-northeast-2.rds.amazonaws.com',  port = 3306 , user = 'contest_manager',
                           password = "thinkmanager6387", db = 'invest', charset = 'utf8')


headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
        'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}



##

stock_like_path = '../data/한국인이 좋아하는 미국 50 종목.xlsx'
df = pd.read_excel(stock_like_path)


'''
stock_ids, stock_news, stock_names  = korean_like_data_split(df)


for i in range(10):
    s_ids, s_news, s_names = stock_ids[i], stock_news[i], stock_names[i]
    result = investing_stock_news_crawler(s_ids, s_news, s_names)
'''


def main(df):  # 차후에  개장전 / 개장후 같이 커스텀해서 main 구성
    db = pymysql.connect(host='mymastertestdb.cwkjjkurliut.ap-northeast-2.rds.amazonaws.com', port=3306,
                         user='contest_manager',
                         password="thinkmanager6387", db='invest', charset='utf8')
    #now = dtime.now()
    stock_ids, stock_news, stock_names = korean_like_data_split(df)
    for i in range(10):
        s_ids, s_news, s_names = stock_ids[i], stock_news[i], stock_names[i]
        result = investing_stock_news_crawler(db, s_ids, s_news, s_names)

    #print('=== 종목 뉴스 테스트 중...',now.date()  ,'===')


main(df)
print('===== 미국 종목 뉴스 매시간 크롤링 中 =====')
# 매 시간 실행
schedule.every().hour.do(main, df)

while True:
    schedule.run_pending()
    time.sleep(1)