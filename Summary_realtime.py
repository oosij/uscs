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
from nltk import sent_tokenize
from nltk.tokenize import WordPunctTokenizer
from nltk.stem import WordNetLemmatizer
from collections import Counter
from nltk import sent_tokenize

import time
import pymysql
import random
import schedule

from Summary_news import today_tomorrow, summary_run, bart_generate, bart_summarize
from Summary_news import summary_se_insert
from US_News_other_func import cnbc_etf_list
from summarizer import Summarizer
from transformers import logging
import warnings
warnings.filterwarnings('ignore')
logging.set_verbosity_warning()

headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
        'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}

### 메인용 함수
#######################################################################################
def summary_realtime_main(db, page_num, category, sub_category, keyword, keyword2):
    start_date_input, end_date_input, w_date = realtime_usa_time()
    ### 테스트용 : 나중엔 삭제
    #start_date_input = 20221125
    #end_date_input = 20221201
    ###
    if category == 'stock_news':
        stock_list = stock_list_load(25)
    if category == 'etf_news':
        stock_list = cnbc_eft_df_to_list(10) # 개당 10개 X 2
    press_list = google_press_list(category)

    # 확인용
    print('target category news =',category, ' stock symbol count =',len(stock_list))

    search_dic = google_search_title_news_extract(start_date_input, end_date_input, page_num,
                                                  stock_list, keyword, keyword2, press_list)
    symbol_dic = google_matching_news_extract(stock_list, search_dic, category)
    result_dic = news_body_three_extract(db, symbol_dic)
    bert_bart_news_run(result_dic, category, sub_category, w_date)
    print(category, 'done')




#######################################################################################
### 0. 미국 시가총액 기반 상위 종목 심볼 추출 -> 리스트화 (순위 제한)
###   + ETF 추출
#######################################################################################
def stock_list_load(num):
    stock_market_cap_path = './data/USA_Market_cap.xlsx'
    stock_df = pd.read_excel(stock_market_cap_path)

    stock_total_list = []

    for s in range(len(stock_df)):
        # 데이터 미싱 'nan' 이라면
        if type(stock_df['symbol'][s]) is float:
            continue
        if stock_df['symbol'][s].find('_') >= 0:
            continue
        stock_data = [stock_df['symbol'][s], stock_df['name'][s], stock_df['industry'][s]]
        stock_total_list.append(stock_data)

    stock_list = stock_total_list[:num]

    return stock_list

## etf 용 데이터 리스트화
def cnbc_eft_df_to_list(num):
    stock_df = cnbc_etf_list()

    stock_list = []
    symbol_list = stock_df['Symbol'].tolist()
    name_list = stock_df['Name'].tolist()
    class_list = stock_df['Class'].tolist()

    # 갯수 제한 용
    class_ind_cnt = 0
    class_comm_cnt = 0
    for snc in range(len(stock_df)):
        symbol_ = symbol_list[snc]
        name_ = name_list[snc]
        class_ = class_list[snc]
        # 각각 10개까지만.
        if class_ == 'index':
            if class_ind_cnt >= num:
                continue
            else:
                class_ind_cnt += 1
        if class_ == 'commodity':
            if class_comm_cnt >= num:
                continue
            else:
                class_comm_cnt += 1
        stock_data = [symbol_, name_, class_]

        stock_list.append(stock_data)
    return stock_list


## 미국 실시간 타임 추출

def realtime_usa_time():
    usa_previous, usa_now = usa_week_to_now()
    start_date_input = int(str(usa_previous.year) + date_zero_padding(usa_previous.month) + date_zero_padding(usa_previous.day))
    end_date_input = int(str(usa_now.year) + date_zero_padding(usa_now.month) + date_zero_padding(usa_now.day))
    w_date = str(usa_now.date())

    return start_date_input, end_date_input, w_date

### 1 - 2. 구글 뉴스 매칭 모듈 및 날짜 정렬 뉴스 중복 체크 모듈
#######################################################################################

def search_title_sql(title, category):
    db = pymysql.connect(host='127.0.0.1', port=3306, user='root',
                         password="1234", db='jisoo', charset='utf8')

    # summary_check = 1 이면 이미 사용한 뉴스임을 의미하므로 제외해야함
    search_sql = 'select url, title, body, w_date, press, category, symbol from US_Stock_Market_News where category ="' + category + '" and title like "%' + title + '%" and summary_check is Null'
    cursor = db.cursor()
    cursor.execute(search_sql)
    rows = cursor.fetchall()
    data_list = list(rows)

    return data_list


def summary_check_update(db, url, symbol, summary_check):
    cur = db.cursor()  # ISIN
    sql = """UPDATE US_Stock_Market_news SET summary_check = %s WHERE url = %s and symbol = %s """
    try:
        cur.execute(sql, (summary_check, url, symbol))
        db.commit()
    except pymysql.Error as msg:
        print(msg)
        pass
#######################################################################################

# 시간 관련 라이브러리에서 종종 1자리수 숫자에 0 안붙이는거 해결...
def date_zero_padding(m):
    if m < 10:
        m = '0' + str(m)
    else:
        m = str(m)
    return m


def date_sorted(numbers):
    for i in range(len(numbers), 1, -1):
        for j in range(i - 1):
            if numbers[j][3] > numbers[j + 1][3]:
                numbers[j], numbers[j + 1] = numbers[j + 1], numbers[j]
    return numbers


def usa_week_to_now():  # today 포함 7일 간 즉, 오늘부터  6일전까지
    usa_est = datetime.datetime.now(pytz.timezone('America/New_York'))
    yesterday = usa_est + timedelta(days=-7)   # -6 = 7일,  -4 = 5일,   -2 = 3일

    usa_mon_str = date_zero_padding(usa_est.month)
    usa_day_str = date_zero_padding(usa_est.day)

    yes_mon_str = date_zero_padding(yesterday.month)
    yes_day_str = date_zero_padding(yesterday.day)

    now_str = str(usa_est.year) + '-' + usa_mon_str + '-' + usa_day_str
    yesterday_str = str(yesterday.year) + '-' + yes_mon_str + '-' + yes_day_str

    usa_now = dtime.strptime(now_str, '%Y-%m-%d')
    usa_yesterday = dtime.strptime(yesterday_str, '%Y-%m-%d')

    return usa_yesterday, usa_now


def google_date_process(date_input):
    years, month, days = str(date_input)[:4], str(date_input)[4:6], str(date_input)[6:]
    str_date = month + '/' + days + '/' + years

    return str_date

## url 중복 체크
def ticker_url_check(ticker_info):
    # ticker_info is symbol_dic['symbol']
    check_list = []
    title_list = [] # 2중 확인

    for ti in range(len(ticker_info)):
        ticker_url = ticker_info[ti][0]
        ticker_title = ticker_info[ti][1]
        check_list.append(ticker_url)
        title_list.append(ticker_title)

    check_cnt = len(set(check_list))
    title_cnt = len(set(title_list))

    return check_cnt, title_cnt


### 구글 크롤러 매칭 함수 ##
def google_news_crawler_v2(query, start_date_input, end_date_input, page_num, press_list):
    start_date = google_date_process(start_date_input)  # 시작 날짜   월 일 년도
    end_date = google_date_process(end_date_input)  # 마지막 날짜   월 일 년도
    avoid_ip_cnt = 0

    for p in range(page_num):
        random_time = int(random.uniform(30, 60))
        page_cnt = p * 10
        url = "https://www.google.com/search?q=" + query + "&tbs=cdr:1,cd_min:" + start_date + ",cd_max:" + end_date + "&tbm=nws&start=" + str(
            page_cnt) + "&gl=us"
        # print(url)
        purl, no_ = google_crawling_v2(url, headers, press_list)
        # print(len(purl))
        if no_ == 1:
            break  # 더이상 페이지 없음
        # 각 뉴스 언론사/주소를 통해 아웃링크 함수 입력 -> 아래엔 임의의 예시
        # parser_runing = outlink_parsing(db, purl, symbol, category)
        avoid_ip_cnt += 1
        # print('소요시간',random_time, '회피 탐지 중...', avoid_ip_cnt)
        time.sleep(random_time)  # 10 ~ 60 에서 랜덤한 숫자만큼 쉬기 : 페이지당
    return purl


# rso > div > div > div:nth-child(1) > div > div > a > div > div.iRPxbe > div.mCBkyc.ynAwRc.MBeuO.nDgy9d
def google_crawling_v2(url, headers, c_press_list):
    purl_list = []
    break_cnt = 0
    response = requests.get(url, headers=headers)
    # print(response)
    soup = BeautifulSoup(response.text, 'html.parser')
    # print(soup)

    for i in range(10):  # 기본 1페이지당 10 개의 글
        find_select_url = '#rso > div > div > div:nth-child(' + str(i + 1) + ') > div > div > a'
        find_select_press = find_select_url + ' > div > div > div.CEMjEf.NUnG9d'
        find_select_title = find_select_url + ' > div > div.iRPxbe > div.mCBkyc.ynAwRc.MBeuO.nDgy9d'

        date_find_url = soup.select(find_select_url)
        date_find_press = soup.select(find_select_press)
        date_find_title = soup.select(find_select_title)

        if len(date_find_url) == 0:  # 더 이상 뉴스 게시글이 없을 때,
            break_cnt = 1
            break
        news_link = date_find_url[0]['href']
        news_press = date_find_press[0].text
        news_title = date_find_title[0].text

        if news_press not in c_press_list:
            continue

        press_data = [news_press, news_link, news_title]
        purl_list.append(press_data)
    return purl_list, break_cnt

#######################################################################################
# - 첫째 :  가능한 언론사 구별하기
def google_press_list(category):
    db = pymysql.connect(host='127.0.0.1', port=3306, user='root',
                         password="1234", db='jisoo', charset='utf8')

    search_sql = """select press from US_Stock_Market_News where category = '""" + category + """'"""

    cursor = db.cursor()
    cursor.execute(search_sql)
    rows = cursor.fetchall()
    data_list = list(rows)
    press_list = []

    for d in range(len(data_list)):
        press = data_list[d][0]
        press_list.append(press)

    press_list = list(set(press_list))

    return press_list


# - 둘째 : 상위 종목 10개를 기반으로 for 문으로 돌리겠금 진행  [제목, url, 언론사, 심볼,  키워드] 로 쭉 추출

def google_search_title_news_extract(start_date_input, end_date_input, page_num,  stock_list, keyword,
                                     keyword2, press_list):
    # start_date_input = 20221125
    # end_date_input = 20221201
    # page_num = 1
    # category = 'stock_news'
    # symbol = 'MSFT'
    # keyword = 'stock'
    # keyword2 = 'price'

    result_list = {}
    for run in range(len(stock_list)):
        symbol = stock_list[run][0]
        query = symbol + ' ' + keyword + ' ' + keyword2  # 검색어
        info_extract = google_news_crawler_v2(query, start_date_input, end_date_input, page_num, press_list)
        result_list[symbol] = info_extract

    return result_list


# - 셋째 : 구글에서 서치한 해당 언론사 뉴스의 제목과 db 의 수집된 뉴스와 매칭 _version 2
##  날짜 정렬 수정 파트 !

def google_matching_news_extract(stock_list, result_list, category):
    symbol_dic = {}

    for t in range(len(stock_list)):

        symbol = stock_list[t][0]
        title_dic = result_list[symbol]
        if len(title_dic) == 0:  # 해당 종목의 데이터가 없으면 스킵
            continue
        symbol_dic[symbol] = []
        for td in range(len(title_dic)):
            title = title_dic[td][2]
            title = title.replace('...', '')
            title = title.replace('\n', '')
            try:
                searching = search_title_sql(title, category)
            except:
                continue
            if len(searching) == 0:
                continue
            ## 이 부분을 수정, symbol 과 마지막 -1 인덱스가 같아야함
            for s in range(len(searching)):
                ninfo = searching[s]
                body, ticker = searching[s][2], searching[s][-1]
                if ticker != symbol:
                    continue
                ## 맞다면, 해당 update sql 필요
                url = searching[s][0]
                # summary_check = 1 # 1 여부에 따라 사용여부 확인
                # summary_check_update(db,  url, symbol, summary_check)
                data = list(ninfo)
                symbol_dic[symbol].append(data)
                # data_list.append(data)

    return symbol_dic


# - 넷째 : 뉴스 중복 체크 및 3 미만의 횟수는 스킵 + 3 이상인 것은 스킵
##  어디까지 할가... 본문만 끄집어내도록?

def news_body_three_extract(db, symbol_dic):
    symbol_keys = list(symbol_dic.keys())
    result_dic = {}

    for k in range(len(symbol_keys)):
        tick = symbol_keys[k]
        tick_list = symbol_dic[tick]
        if len(tick_list) < 3:
            continue
        url_count, title_cnt = ticker_url_check(tick_list)
        if url_count < 3: # url 중복 체크
            continue
        if title_cnt < 3:  # 제목 중복 체크
            continue
        result_dic[tick] = []
        dnum_list = date_sorted(tick_list)
        for tk in range(len(dnum_list)):
            url, smb = dnum_list[tk][0], dnum_list[tk][-1]
            w_date = dnum_list[tk][3]  # .date()
            body = dnum_list[tk][2]
            ## 먼저 체크해두고, 데이터 형태 바꾸기
            summary_check = 1  # 1 여부에 따라 사용여부 확인
            summary_check_update(db, url, tick, summary_check)
            result_dic[smb].append([body, w_date])

            if tk == 2:  # 3개까지만 처리
                break

    return result_dic
#######################################################################################
### 4. 본문 기반으로 진행
def summary_extract(news_list):
    bert_summary = Summarizer()

    body_cons = body_connect(news_list)

    bsum = bert_summary(body_cons, ratio=0.2, num_sentences=3)  # ratio=0.2
    bart = bart_generate(bsum)

    summary_data = [bsum, bart]

    return summary_data


def body_connect(data_list):
    contents = ''

    for i in range(len(data_list)):
        body = data_list[i]  # body 1
        body_split = sent_tokenize(body)

        for b in range(len(body_split)):
            if len(body_split) <= 5:
                break
            sentence = body_split[b]
            if b == 0:
                content = sentence
            else:
                content = content + ' ' + sentence

        if i == 0:
            contents = content
        else:
            contents = contents + ' ' + content
    return contents

#######################################################################################
### 3. 본문 입력진행할 전처리 & 기타
def bert_bart_news_run(result_dic, category, sub_category, w_date):
    symbol_keys = list(result_dic.keys())
    news_dic = {}

    for s in range(len(symbol_keys)):
        ticker = symbol_keys[s]
        if len(result_dic[ticker]) == 0:
            continue
        news_dic[ticker] = result_dic[ticker]

    symbol_target_list = list(news_dic.keys())

    for t in range(len(symbol_target_list)):
        ticker = symbol_target_list[t]
        body_list = []
        for n in range(len(news_dic[ticker])):
            body, b_date = news_dic[ticker][n][0], news_dic[ticker][n][1]
            # 여기에 입력 함수
            if n == 0:  # 시작 날짜
                s_date = b_date
            if n == 2:  # 끝 날짜
                e_date = b_date
            body_list.append(body)

        ## summary model
        summary_input = summary_extract(body_list)
        ## db 인풋   :
        bert_output, bart_output = summary_input[0], summary_input[1]
        summary_se_insert(bert_output, bart_output, category, sub_category, w_date, s_date, e_date, ticker)

#######################################################################################

#######################################################################################

#######################################################################################

#######################################################################################
# 전체 메인 프로세스  메모용 : 실제 구동 X
'''
## 샘플 10 개 -> 추후에 db에서 가져올시, 아래와 같은 형태로 하는게 편할 듯함.
stock_list = stock_list_load(30)
# 미국 시각 기준 어제, 오늘 날짜
usa_previous, usa_now = usa_week_to_now()

start_date_input = int(str(usa_previous.year) + date_zero_padding(usa_previous.month) + date_zero_padding(usa_previous.day))
end_date_input = int(str(usa_now.year) + date_zero_padding(usa_now.month) + date_zero_padding(usa_now.day))
# start_date_input, end_date_input 는 int형으로 변환하는데, 이는 아래 예시와 같이 구글 url parameter

start = time.time()

start_date_input = 20221125
end_date_input = 20221201
page_num = 1
category = 'stock_news'
sub_category = ''
# 추후에 수정할지 정하기 지금은 비워두자 :
## 그냥 근본적인걸 건들이지말고, 마지막 입력때 종목을 기반으로 서브카테고리 검색하는 함수하나 만들기
keyword = 'stock'
keyword2 = 'price'
# 구글 검색에서 매칭되는 언론사 선정
press_list = google_press_list(category)
# 구글 검색에서 db 내 언론사와 일치하는 언론사의 url, title, press 추출
search_dic = google_search_title_news_extract(start_date_input,end_date_input, page_num,
                                              stock_list, keyword, keyword2, press_list  )
# 위의 search_dic 에서 매칭되는 db 뉴스 본문 추출하기
symbol_dic = google_matching_news_extract(stock_list, search_dic, category)
# 위의 symbol_dic 에서 기간 동안 정렬해서 가장 오래된 것부터 3개 자르기 : 3개 미만인 종목의 뉴스는 아에 스킵함.
result_dic = news_body_three_extract(db, symbol_dic)
# w_date는 입력 상, str으로 바꾸기 위해 진행 : 시황 뉴스와의 어울림을 위해
w_date = str(usa_now.date())
# bert 추출 요약과 bart 추상 요약을 사용하고, db에 저장 : 추후 모델 학습 필요
bert_bart_news_run(result_dic, category, sub_category, w_date)
'''