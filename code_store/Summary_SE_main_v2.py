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



db = pymysql.connect(host='127.0.0.1',  port = 3306 , user = 'root',
                           password = "1234", db = 'jisoo', charset = 'utf8')
def asdasd():
 start = time.time()

#start_date_input = 20221125
#end_date_input = 20221201

page_num = 1
category = 'stock_news'
sub_category = ''
# 추후에 수정할지 정하기 지금은 비워두자 :
# 그냥 근본적인걸 건들이지말고, 마지막 입력때 종목을 기반으로 서브카테고리 검색하는 함수하나 만들기
keyword = 'stock'
keyword2 = 'price'

summary_realtime_main(db,  page_num, category, sub_category, keyword, keyword2 )
time.sleep()
# 2시간에 한번씩 함수 실행
schedule.every(2).hour.do(함수)

print("WorkingTime: {} sec".format(time.time() - start))  # 현재시각 - 시작시간 = 실행 시간

