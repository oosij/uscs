## import load !
import pandas as pd
import schedule
import openpyxl
import re

import datetime
from datetime import datetime as dtime
from datetime import timedelta
from dateutil.parser import parse
from time import strptime
import pytz

import time
import pymysql
import random

from Summary_realtime import summary_realtime_main
from transformers import logging
import warnings
warnings.filterwarnings('ignore')
logging.set_verbosity_warning()

def summary_se_realtime_main():
    db = pymysql.connect(host='127.0.0.1', port=3306, user='root',
                         password="1234", db='jisoo', charset='utf8')
    page_num = 2
    category_list = ['stock_news', 'etf_news']
    #category = 'stock_news'
    sub_category = ''
    # 추후에 수정할지 정하기 지금은 비워두자 :
    # 그냥 근본적인걸 건들이지말고, 마지막 입력때 종목을 기반으로 서브카테고리 검색하는 함수하나 만들기
    #keyword = 'stock'
    key_one_list = ['stock', 'etf']
    keyword2 = 'price' #'price'

    for run in range(2):
        start = time.time()
        ## 실제 실행
        category = category_list[run]
        keyword = key_one_list[run]
        summary_realtime_main(db,  page_num, category, sub_category, keyword, keyword2 )

        print("WorkingTime: {} sec".format(time.time() - start))
        #if run == 0:
        time.sleep(3600) # 1시간 쿨

print('===== 미국 종목/ETF 뉴스 요약 자동화 中 =====')
summary_se_realtime_main()

# 2시간에 한번씩 함수 실행
schedule.every(1).hour.do(summary_se_realtime_main)

# 스캐쥴 시작
while True:
    schedule.run_pending()
    time.sleep(1)
