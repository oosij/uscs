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
from datetime import timedelta
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
import random

from US_News_other_func import barchart_commdities_table_info, barchart_news_crawler

from retrying import retry
from pyvirtualdisplay import Display

@retry(stop_max_attempt_number=7, wait_random_min=600, wait_random_max=1200)
def never_give_up_never_surrender():
    print('retry')
    #raise ConnectionError, raise MaxRetryError, rasie TimeoutException
never_give_up_never_surrender()

# 로컬 DB TEST 접속
db = pymysql.connect(host='127.0.0.1',  port = 3306 , user = 'root',
                           password = "1234", db = 'jisoo', charset = 'utf8')


headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
        'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}


def barchat_news_crawler_main(db):
    now = dtime.now()
    print(now)
    start = time.time()
    select = 'etf'
    news_list = barchart_news_crawler(select)  # 실질 running
    #print(len(news_list))
    #print(news_list[-1])
    print("WorkingTime: {} sec".format(time.time() - start))  # 현재시각 - 시작시간 = 실행 시간
    time.sleep(3600)

    return news_list

print('===== 미국 ETF 뉴스 크롤링 中 =====')
# select = 'etf'
# barchart_news_crawler(select)
#barchat_news_crawler_main(db)

# 60분에 한번씩 함수 실행
schedule.every(1).hour.do(barchat_news_crawler_main, db)

# 무한 반복
while True:
    schedule.run_pending()
    time.sleep(1)
