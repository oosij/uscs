from Summary_news import today_tomorrow, symbol_summary_run, summary_insert, sef_search_sql_news

from datetime import datetime as dtime
import datetime
import time
import pymysql
import schedule

from transformers import logging
import warnings
warnings.filterwarnings('ignore')
logging.set_verbosity_warning()

db = pymysql.connect(host='127.0.0.1',  port = 3306 , user = 'root',
                           password = "1234", db = 'jisoo', charset = 'utf8')



def summary_main(category, symbol):
    db = pymysql.connect(host='127.0.0.1', port=3306, user='root',
                         password="1234", db='jisoo', charset='utf8')
    now = dtime.now()
    base_, base_end = today_tomorrow(now)
    base_, base_end = '2022-12-16' , '2022-12-22' # '오늘', (>=)  '내일'
    print(base_, base_end)

    title, bert, bart, date = symbol_summary_run(db, base_, base_end , category, symbol)

    # DB 넣는 함수,
    #result = summary_insert(db, title, bert, bart, category, date)
    print(symbol)
    print(bert)
    print(bart)
    print('=== '+ category + ' (미국) 요약 완료',date ,'===')

    #return result # title, bert, bart, category, date


# markey_news는 심볼이 없어서 따로 지정
category = 'etf_news'  # 카테고리명 ['stock_news',  'etf_news' ]
symbol = 'IWM'  # 종목 심볼
print('===== 미국 종목 뉴스 요약 자동화 中 =====')
summary_main(category, symbol)
