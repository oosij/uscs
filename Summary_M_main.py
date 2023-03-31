from Summary_news import today_tomorrow, summary_run, summary_m_insert, search_sql_news

from datetime import datetime as dtime
import datetime
import time
import pymysql
import schedule

from transformers import logging
import warnings
warnings.filterwarnings('ignore')
logging.set_verbosity_warning()

# 로컬
#db = pymysql.connect(host='127.0.0.1',  port = 3306 , user = 'root',
#                           password = "1234", db = 'jisoo', charset = 'utf8')

# 아마존 RDS
#db = pymysql.connect(host='mymastertestdb.cwkjjkurliut.ap-northeast-2.rds.amazonaws.com', port=3306,
#                         user='contest_manager',
#                         password="thinkmanager6387", db='invest', charset='utf8')

def summary_m_run(category, sub_category):
    db = pymysql.connect(host='127.0.0.1',  port = 3306 , user = 'root',
                               password = "1234", db = 'jisoo', charset = 'utf8')

    now = dtime.now()
    base_, base_end = today_tomorrow(now)
    #base_, base_end = '2022-12-23' , '2022-12-24'  # 테스트용
    bert, bart, date = summary_run(db, base_, base_end , category)
    result = summary_m_insert(bert, bart, category, sub_category,  date)
    print('=== '+ category + ' (미국) 요약 완료',date ,'===')

    return result # title, bert, bart, category, date

def summary_market_main():
    category = 'market_news'
    sub_category = 'AMC(UST)'
    summary_m_run(category, sub_category)

    return 1



#category = 'market_news'
#sub_category = 'AMC(UST)'
print('===== 미국 시황 뉴스 요약 자동화 中 =====')
#summary_main(category, sub_category)

# 장마감 시간 기준
end_time = "06:40" # 06:40 , 07:40

#summary_market_main()


# 월요일
schedule.every().tuesday.at(end_time).do(summary_market_main) # 화요일 -> 미국시각 월요일
# 화요일
schedule.every().wednesday.at(end_time).do(summary_market_main) # 수요일 -> 미국시각 화요일
# 수요일
schedule.every().thursday.at(end_time).do(summary_market_main) # 목요일 -> 미국시각 수요일
# 목요일
schedule.every().friday.at(end_time).do(summary_market_main) # 금요일 -> 미국시각 수요일
# 금요일
schedule.every().saturday.at(end_time).do(summary_market_main) # 토요일 -> 미국시각 금요일


while True:
    schedule.run_pending()
    time.sleep(1)
