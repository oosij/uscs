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

import datetime
from datetime import datetime as dtime
import calendar
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
from US_News_func import stock_info_load, finviz_news_crawler, notfind_symbol_load, date_zero_padding

import time
import pymysql

db = pymysql.connect(host='127.0.0.1',  port = 3306 , user = 'root',
                           password = "1234", db = 'jisoo', charset = 'utf8')

# 시간 재기용
start = time.time()

#select_ticker = ['AAPL', 'MSFT', 'GOOG', 'AMZN', 'TSLA', 'NVDA', 'PEP', 'COST', 'ASML', 'AVGO']
select_ticker = 0  ## 선택없이 전체
ticker_list = stock_info_load(db, select_ticker) # 0 이면 전체 할수있도록
nofind_symbol_list = notfind_symbol_load()

select_stock = []

cnt = 0
for n in range(len(ticker_list)):
    symbol = ticker_list[n][0]
    if symbol in nofind_symbol_list:
        cnt += 1
        continue
    select_stock.append(ticker_list[n])

print( len(nofind_symbol_list) , '/',len(ticker_list) , '=', len(select_stock))

#excel_df = pd.DataFrame(select_stock, columns = ['symbol', 'name', 'ISIN'])
#excel_df.to_excel('./data/now_symbol_list(4868).xlsx', index = False)

def stock_news_main(select_stock):
    # 아마존 DB TEST 접속
    db = pymysql.connect(host='127.0.0.1', port=3306, user='root',
                         password="1234", db='jisoo', charset='utf8')

    usa_est = datetime.datetime.now(pytz.timezone('America/New_York'))

    month = date_zero_padding(usa_est.month)
    days = date_zero_padding(usa_est.day)
    ymd_date = str(usa_est.year) + '-' + str(month) + '-' + str(days)

    category = 'stock_news'

    # 시간 재기용
    start = time.time()
    #finviz_news_crawler(db, category, select_stock, ymd_date)
    try:
        finviz_news_crawler(db, category, select_stock, ymd_date)
    except:
        print('Error!')
        time.sleep(60)
    print("WorkingTime: {} sec".format(time.time() - start))  # 현재시각 - 시작시간 = 실행 시간
    time.sleep(3600) # 1시간 후딜

print('===== 미국 종목 뉴스 크롤링 中 =====')
#stock_news_main(select_stock)

# 1시간에 한번씩 함수 실행
schedule.every(1).hour.do(stock_news_main, select_stock)

# 무한 반복
while True:
    schedule.run_pending()
    time.sleep(1)
