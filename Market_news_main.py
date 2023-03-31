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
db = pymysql.connect(host='127.0.0.1',  port = 3306 , user = 'root',
                           password = "1234", db = 'jisoo', charset = 'utf8')

# 아마존 DB TEST 접속
#db = pymysql.connect(host='mymastertestdb.cwkjjkurliut.ap-northeast-2.rds.amazonaws.com',  port = 3306 , user = 'contest_manager',
#                           password = "thinkmanager6387", db = 'invest', charset = 'utf8')



headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
        'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}

## 미국 주식 거래시간
## BMO  : before market open  / AMO  : after market open  / AMC  : after market close
## UST  :    표준시           / DST : 서머타임

##
us_market_time_dic = {'BMO(UST)': ['07:30:00', '09:30:00'], 'AMO(UST)': ['10:30:00', '11:30:00'],
                          'AMC(UST)': ['17:00:00', '18:30:00'], 'BMO(DST)': ['06:30:00', '09:00:00'],
                          'AMO(DST)': ['09:30:00', '10:30:00'], 'AMC(DST)': ['16:00:00', '17:30:00']}


# 매일 특정 HH:MM 및 다음 HH:MM:SS에 작업 실행




def main(m_class):  # 차후에  개장전 / 개장후 같이 커스텀해서 main 구성
    now = dtime.now()
    db = pymysql.connect(host='127.0.0.1', port=3306, user='root',
                         password="1234", db='jisoo', charset='utf8')

    #market = 'AMC'
    market = m_class
    daily = 'DST'  # 서머타임 DST, 표준시면 UST로
    market_time = market + '(' + daily + ')'
    # market_time = us_market_time_dic[name]
    #print('미국 시황 장마감 수집 시작')
    #invert_news_crawler_v2(db, market_time)
    #print('인베스팅 뉴스 종료')
    yahoo_crawler(db, market_time)
    print('야후 파이낸스 뉴스 종료')
    print('=== '+ market_time  + ' (한국)',now.date()  ,'===')


print('===== 미국 시황 뉴스 크롤링 中 =====')
## 현재는 장마감 이후만 캐치하지만, 차후엔 위와 동일하게 개장전 /개장후/ 장마감 3개의 시간대를 스케줄러화 해야함
#schedule.every().day.at("05:30").do(main, db) # 05:



## 미국 서머 타임
# 연도   시작     종료
# 2022	3/13	11/6
# 2023	3/12

# 장 시작은 22:30, ~ 마감은 05:00
start_pre = "22:00"
start_time = "23:30"
end_time = "06:30" # 06:30 , 07:30
#market_class = 'AMC'  # 테스트는 이대로하되, 추후에는 각각을 리스트화 시켜서 돌아갈수 있도록 설정해야함 : 해당 부분 스케줄 모듈로 어케하는지

market_bmo = 'BMO'
market_amo = 'AMO'
market_amc = 'AMC'

#main(market_bmo)
#main(market_amo)
#main(market_amc)
#print('끝')

#url = 'https://finance.yahoo.com/news/emerging-markets-latam-fx-stocks-204328673.html'
#ttt = ['16:00:00', '17:00:00']
#fyahoo_news_info_extract(url, headers, ttt)
#print('끝')


#url = 'https://www.investing.com/news/stock-market-news/stock-market-today-dow-racks-up-gains-amid-plunging-treasury-yields-2901187'
#data = investing_news_info_extract(url, headers)
#print(data)

# 월요일  : 실패, 매개변수를 착각함;;;  그러나, 장마감 뉴스 때 6:00 에도 뉴스 하나가 잡히지 않음! 30분 증가  하고 다음날 시도 !
#schedule.every().monday.at(start_pre).do(main, market_bmo) # 월요일 -> 미국시각 월요일
#schedule.every().monday.at(start_time).do(main, market_amo) # 월요일 -> 미국시각 월요일
schedule.every().tuesday.at(end_time).do(main, market_amc) # 화요일 -> 미국시각 월요일

# 화요일
#schedule.every().tuesday.at(start_pre ).do(main, market_bmo) # 화요일 -> 미국시각 월요일
#schedule.every().tuesday.at(start_time).do(main, market_amo) # 화요일 -> 미국시각 월요일
schedule.every().wednesday.at(end_time).do(main, market_amc) # 수요일 -> 미국시각 화요일

# 수요일
#schedule.every().wednesday.at(start_pre).do(main, market_bmo) # 수요일 -> 미국시각 화요일
#schedule.every().wednesday.at(start_time).do(main, market_amo) # 수요일 -> 미국시각 화요일
schedule.every().thursday.at(end_time).do(main, market_amc) # 목요일 -> 미국시각 수요일

# 목요일
#schedule.every().thursday.at(start_pre).do(main, market_bmo) # 목요일 -> 미국시각 화요일
#schedule.every().thursday.at(start_time).do(main, market_amo) # 목요일 -> 미국시각 화요일
schedule.every().friday.at(end_time).do(main, market_amc) # 금요일 -> 미국시각 수요일

# 금요일
#schedule.every().friday.at(start_pre).do(main, market_bmo) # 금요일 -> 미국시각 수요일
#schedule.every().friday.at(start_time).do(main, market_amo) # 금요일 -> 미국시각 목요일
schedule.every().saturday.at(end_time).do(main, market_amc) # 토요일 -> 미국시각 금요일


while True:
    schedule.run_pending()
    time.sleep(1)


## 키워드 기반 보다 최종적으로 분류 모델로써 활용해서 수집하는게 차후의 방법