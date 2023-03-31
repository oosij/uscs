from Summary_news import today_tomorrow, symbol_summary_run, summary_se_insert, sef_search_sql_news

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
    base_, base_end = '2022-12-05', '2022-12-07'
    print(base_, base_end)
    # 추후 매개변수 sybmol 을 symbol_info 리스트 값으로 받고, 각각의 인덱스를 symbol, category, sub_category = symbol_info[0] , symbol_info[1], symbol_info[2] 로 지정
    bert, bart, date = symbol_summary_run(db, base_, base_end , category, symbol)
    #result = summary_se_insert(db, title, bert, bart, category, sub_category, symbol, date)
    print(symbol)
    print(bert)
    print(bart)
    print('=== '+ category + ' (미국) 요약 완료',date ,'===')

    #return result # title, bert, bart, category, date


category = 'stock_news'
symbol = 'TSLA'
print('===== 미국 종목 뉴스 요약 자동화 中 =====')
summary_main(category, symbol)

