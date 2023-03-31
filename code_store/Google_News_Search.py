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

from US_News_func import *
from Press_List import nasdaq_news_parser, etf_trands_news_parser, etf_daily_news_parser, the_montley_fool_news_parser, benzinga_news_parser
from Press_List import equities_news_parser, moneyshow_news_parser, fxstreet_news_parser, entrepreneur_news_parser, compound_advisors_news_parser
from random import randint

MAX_SLEEP_TIME = 10

headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
        'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}



def outlink_parsing(db, purl, symbol,category):
    news_info_list =[]

    for i in range(len(purl)):
        press_t = purl[i][0]
        url_t =  purl[i][1]
        news_checking = news_duplicate_check(db, url_t, symbol)
        if news_checking == 1:
            continue  # 중복된 url + 종목의 뉴스가 있다면 스킵
        ## 아웃링크 파서 함수
        if press_t == 'Yahoo Finance':
            news_info = fyahoo_stock_news_inlink_extract(url_t, headers)

        if press_t == 'Nasdaq':
            news_info = nasdaq_news_parser(url_t, headers, press_t)

        if press_t == 'ETF Trends':
            news_info = etf_trands_news_parser(url_t, headers, press_t)

        if press_t == 'ETF Daily News':
            news_info = etf_daily_news_parser(url_t, headers, press_t)

        if press_t =='The Motley Fool':
            news_info = the_montley_fool_news_parser(url_t, headers, press_t)

        if press_t == 'Benzinga':
            news_info = benzinga_news_parser(url_t, headers, press_t)

        if press_t == 'Equities News':
            news_info = equities_news_parser(url_t, headers, press_t)

        if press_t == 'MoneyShow':
            news_info = moneyshow_news_parser(url_t, headers, press_t)

        if press_t == 'Fxstreet':
            news_info = fxstreet_news_parser(url_t, headers, press_t)

        if press_t == 'Entrepreneur':
            news_info = entrepreneur_news_parser(url_t, headers, press_t)

        if press_t == 'Compound Advisors':
            news_info = compound_advisors_news_parser(url_t, headers, press_t)
        nurl, title, body, w_date, n_date, press = news_info[0], news_info[1], news_info[2], news_info[3], news_info[4], news_info[5]
        news_info_input = stock_news_insert(db, nurl, title, body, w_date, n_date, press, category, symbol)
        news_info_list.append(news_info)
    return news_info_list


def google_etf_news_crawler_main(db):
    symbol_list = symbols_load()
    usa_est = dtime.now(pytz.timezone('America/New_York'))
    yesterday = timedelta(days=1)
    usa_est_yesterday = usa_est - yesterday

    start_date_input = google_date_time_str(usa_est_yesterday)
    end_date_input = google_date_time_str(usa_est)
    print(start_date_input, end_date_input)

    page_num = 1

    category = 'etf_news'

    for s in range(len(symbol_list)):
        symbol = symbol_list[s]
        query = symbol + ' ' + 'etf'  # 검색어
        google_news_crawler(db, symbol, query, start_date_input, end_date_input, page_num, category)


def symbols_load():
    etf_path = '../data/ETF_top100_sample.xlsx'
    df = pd.read_excel(etf_path)

    symbol_list = []

    for d in range(len(df['Symbol'])):
        ticker = df['Symbol'].iloc[d]
        symbol_list.append(ticker)
    return symbol_list


def google_date_time_str(time_pre):
    month = date_zero_padding(time_pre.month)
    days = date_zero_padding(time_pre.day)
    year = date_zero_padding(time_pre.year)
    ymd_date = str(year) + str(month) + str(days)

    return ymd_date



def google_news_crawler(db, symbol, query, start_date_input, end_date_input, page_num, category):
    start_date = google_date_process(start_date_input)  # 시작 날짜   월 일 년도
    end_date = google_date_process(end_date_input)  # 마지막 날짜   월 일 년도
    page_cnt = (page_num - 1) * 10

    MAX_SLEEP_TIME = 10  # 3 ~ 10
    rand_value = randint(3, MAX_SLEEP_TIME)  # 랜덤으로 time sleep 조정, 차단 회피 방안 중 하나
    
    for p in range(page_num):
        url = "https://www.google.com/search?q=" + query + "&tbs=cdr:1,cd_min:" + start_date + ",cd_max:" + end_date + "&tbm=nws&start=" + str(
            page_cnt) + "&gl=us"
        purl, no_ = google_crawling(url, headers)
        if no_ == 1:
            break  # 더이상 페이지 없음
        # 각 뉴스 언론사/주소를 통해 아웃링크 함수 입력 -> 아래엔 임의의 예시
        parser_runing = outlink_parsing(db, purl, symbol, category)
        time.sleep(rand_value)
    return parser_runing


### 추가되는 함수들 ###

def google_date_process(date_input):
    years, month, days = str(date_input)[:4], str(date_input)[4:6], str(date_input)[6:]
    str_date = month + '/' + days + '/' + years

    return str_date


def google_crawling(url, headers):
    c_press_list = ['Nasdaq', 'ETF Trends', 'FXStreet', 'ETF Daily News', 'Compound Advisors', 'Benzinga',
                    'Equities News', 'MoneyShow',
                    'The Motley Fool', 'Entrepreneur', 'Yahoo Finance']
    purl_list = []
    break_cnt = 0
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    for i in range(10):  # 기본 1페이지당 10 개의 글
        find_select_url = '#rso > div > div > div:nth-child(' + str(i + 1) + ') > div > div > a'
        find_select_press = find_select_url + ' > div > div > div.CEMjEf.NUnG9d'

        date_find_url = soup.select(find_select_url)
        date_find_press = soup.select(find_select_press)
        if len(date_find_url) == 0:  # 더 이상 뉴스 게시글이 없을 때,
            break_cnt = 1
            break
        news_link = date_find_url[0]['href']
        news_press = date_find_press[0].text

        if news_press not in c_press_list:
            continue

        press_data = [news_press, news_link]
        purl_list.append(press_data)
    return purl_list, break_cnt

def news_duplicate_check(db, url, ticker):
    # db에 url 가 있다면 break! ]
    checking_sql = "SELECT * FROM US_Stock_Market_News WHERE url = '" + url + "' and symbol ='" + ticker + "'"

    cur = db.cursor()
    cur.execute(checking_sql)
    result = cur.fetchall()

    if len(result) >= 1:
        result = 1  # 1이면 contine
    else:
        result = 0
    return result

def newsdate_preprocessing(dt):
    dt = dt.replace('T', ' ')
    dt = dt.replace('+09:00', '')
    dt = datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
    return dt


