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

# 최초 크롤링 관련 함수 백업버전 v1 : 220601 ~ 221027 까지


# 로컬 DB TEST 접속
#db = pymysql.connect(host='127.0.0.1',  port = 3306 , user = 'root',
#                           password = "1234", db = 'jisoo', charset = 'utf8')

#  아마존 DB 접속
db = pymysql.connect(host='mymastertestdb.cwkjjkurliut.ap-northeast-2.rds.amazonaws.com', port=3306,
                         user='contest_manager',
                         password="thinkmanager6387", db='invest', charset='utf8')

headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
        'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}

## 미국 주식 거래시간
## BMO  : before market open  / AMO  : after market open  / AMC  : after market close
## UST  :    표준시           / DST : 서머타임

##
us_market_time_dic = {'BMO(UST)': ['07:30:00', '09:30:00'], 'AMO(UST)': ['10:30:00', '11:30:00'],
                      'AMC(UST)': ['17:00:00', '18:30:00'], 'BMO(DST)': ['06:30:00', '09:00:00'],
                      'AMO(DST)': ['09:30:00', '10:30:00'], 'AMC(DST)': ['16:00:00', '17:00:00']}

# 매일 특정 HH:MM 및 다음 HH:MM:SS에 작업 실행

'''
#schedule.every().day.at("05:30").do(main, db) # 05:
schedule.every().tuesday.at("05:30").do(main, db) # 화요일 -> 미국시각 월요일
schedule.every().wednesday.at("05:30").do(main, db) # 수요일 -> 미국시각 화요일
schedule.every().thursday.at("05:30").do(main, db) # 목요일 -> 미국시각 수요일
schedule.every().friday.at("05:30").do(main, db) # 금요일 -> 미국시각 목요일
schedule.every().saturday.at("05:30").do(main, db) # 토요일 -> 미국시각 금요일


while True:
    schedule.run_pending()
    time.sleep(1)

'''

def main(db):
    db = pymysql.connect(host='mymastertestdb.cwkjjkurliut.ap-northeast-2.rds.amazonaws.com', port=3306,
                         user='contest_manager',
                         password="thinkmanager6387", db='invest', charset='utf8')
    market = 'AMC'
    daily = 'DST'  # 서머타임, 표준시면 UST로
    market_time = market + '(' + daily + ')'
    # market_time = us_market_time_dic[name]
    print('미국 시황 장마감 수집 시작')
    yahoo_crawler(db, market_time)
    print('야후 파이낸스 뉴스 종료')
    invert_news_crawler(db, market_time)
    print('인베스팅 뉴스 종료')


################### 시장 시간대 구역 ############################

# 노멀한 메타 데이터에서 시간 추출
def yahoo_market_time(soup):
    datetimes = soup.find("time")
    datetimes = datetimes['datetime']
    dt = datetimes.split('T')
    d = dt[0]
    t = dt[1].split('.')
    t = t[0]
    dt = d + ' ' + t
    dt = dtime.strptime(dt, '%Y-%m-%d %H:%M:%S')
    one_hour_ago = dt - datetime.timedelta(hours=4)
    date_time = one_hour_ago  # html에서 GMT 기준으로 나오기에, -4 시간 빼야 미국 현재 시각, 표기로는 GMT+9 를 따름
    return date_time



## 개장전/개장후/장마감 시간대 서치
def investing_market_search_time(soup, market_time):
    if market_time == 0:
        market_time = ['00:10:00', '00:20:00']
    dt_frame = soup.find("div", {'class': 'contentSectionDetails'})
    dt_texts = dt_frame.find('span').text
    a = dt_texts.find('(')
    b = dt_texts.find(')')
    result = dt_texts[a + 1:b]
    result = result.replace(',', '')
    dt = result.split(' ')
    dt_pm = dt[3].find('PM')  # 있으면 + 없으면 -1 반환
    dt_mon = strptime(dt[0], '%b').tm_mon
    if dt_mon < 10:
        dt_mon = '0' + str(dt_mon)
    ymd = dt[2] + '-' + str(dt_mon) + '-' + dt[1]

    start_hms = market_time[0]
    end_hms = market_time[1]
    # start_hms = '16:00:00'
    # end_hms = '16:30:00'
    limit_start = ymd + ' ' + start_hms
    limit_end = ymd + ' ' + end_hms

    limit_start = dtime.strptime(limit_start, '%Y-%m-%d %H:%M:%S')
    limit_end = dtime.strptime(limit_end, '%Y-%m-%d %H:%M:%S')

    date_time = dt_format(dt_texts)

    return date_time, limit_start, limit_end


def yahoo_market_search_time(soup, market_time):
    if market_time == 0:
        market_time = ['00:10:00', '00:20:00']
    datetimes = soup.find("time")
    datetimes = datetimes['datetime']
    dt = datetimes.split('T')
    d = dt[0]
    t = dt[1].split('.')
    t = t[0]
    dt = d + ' ' + t
    dt = dtime.strptime(dt, '%Y-%m-%d %H:%M:%S')
    one_hour_ago = dt - datetime.timedelta(hours=4)
    date_time = one_hour_ago  # html에서 GMT 기준으로 나오기에, -4 시간 빼야 미국 현재 시각, 표기로는 GMT+9 를 따름

    start_hms = market_time[0]
    end_hms = market_time[1]
    # start_hms = '16:00:00'
    # end_hms = '16:30:00'
    limit_start = d + ' ' + start_hms
    limit_end = d + ' ' + end_hms

    limit_start = dtime.strptime(limit_start, '%Y-%m-%d %H:%M:%S')
    limit_end = dtime.strptime(limit_end, '%Y-%m-%d %H:%M:%S')

    return date_time, limit_start, limit_end


###################핀 비즈 구역 ############################

def finviz_news_crawler(db, category,ticker_list, ymd_date):

    news_info_list = []
    error_cnt = 0

    for t in range(len(ticker_list)):
        ticker, s_name, s_code = ticker_list[t][0], ticker_list[t][1], ticker_list[t][2]
        # ticker, s_name, s_code = ticker_list[1][0], ticker_list[1][1], ticker_list[1][2]
        try:
            fin_dic = finviz_news_crawling(ticker, ymd_date)
        except:
            error_cnt += 1
            print('['+str(error_cnt)+']', ticker)

            continue
        if len(fin_dic) == 0:
            continue  # 오늘 데이터 없으면 다음 티커 를 탐색
        p_list = fin_dic[ymd_date]

        for u in range(len(p_list)):
            url = p_list[u][-1]
            news_checking = finviz_news_check(db, url, ticker)
            if news_checking == 1:
                continue  # 중복된 url + 종목의 뉴스가 있다면 스킵

            try:
                news_info = fyahoo_stock_news_inlink_extract(url, headers)
                # [news_info] ... + 분류명 (종목 뉴스) , 티커, 종목명, 종목 코드
                # [news_info] ... + categroy ('stock_news') , ticker, stock_name, stock_code
                # category column 변경 필요 ->  market_time 으로, 기존의 시황 뉴스는 category = 'market_news' 로

                news_info_add = news_info + [category] + [ticker] + [s_name] + [s_code]
                url, title, headline, body = news_info_add[0], news_info_add[1], news_info_add[2], news_info_add[3]
                w_date, n_date, press = news_info_add[4], news_info_add[5], news_info_add[6]
                category, ticker, name, code = news_info_add[7], news_info_add[8], news_info_add[9], news_info_add[10]

                news_input = stock_news_insert(db, url, title, headline, body, w_date, n_date, press, category, ticker, name, code)
                nadd_info = [url, title, headline, body, w_date, n_date, press, category, ticker, name, code]
                # 나중엔 삭제 똔느 스킵
                news_info_list.append(nadd_info)
                # print(url,nadd_info[6:])  # 확인용

                ## db 저장하는 함수 필요 : 최근 글 중복 처리하는 방법도 필요

            except:
                print('Abnormal detected :', url)
                continue
            time.sleep(2)
        # print(ticker,len(news_info_list))
    print(error_cnt)
    return news_info_list


def finviz_news_crawling(name, ymd_date):
    c_press_list = ['Bloomberg', 'Reuters', 'Yahoo Finance', 'Investing.com', 'Zacks', 'The Telegraph',
                    'Insider Monkey',
                    'Benzinga', 'Simply Wall St.', 'Business Wire', 'The Independent', 'Fortune', 'CoinDesk', 'Variety',
                    'PR Newswire', 'WWD', 'Skift', 'LA Times', 'The Guardian', 'Poets & Quants', 'GuruFocus.com']

    url = "https://finviz.com/quote.ashx?t=" + name + "&p=d"
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    date_find = soup.find_all('table', {'class': 'fullview-news-outer'})
    date_finding = date_find[0].find_all('td', {'align': 'right'})

    news_link = date_find[0].find_all('div', {'class': 'news-link-left'})
    news_press = date_find[0].find_all('div', {'class': 'news-link-right'})

    purl_list = []

    date_dic = {}  # 만약을 위해  view에 보이는 날짜 전부를 수집, 평소엔 가장 상단의 키만을 사용 -> 다음 키 생성시 break

    for i in range(len(news_link)):
        press_name = news_press[i].find('span').text.lstrip()
        press_title = news_link[i].text
        press_url = news_link[i].find('a')['href']

        date_time = date_finding[i].text
        date_split_time = date_time.split(' ')
        if len(date_split_time) >= 2:
            ymdate = date_split_time[0].split('-')
            m, d, y = ymdate[0], ymdate[1], ymdate[2]
            m = strptime(m, '%b').tm_mon
            if m < 10:
                m = '0' + str(m)
            else:
                m = str(m)
            ymd_str = '20' + y + '-' + m + '-' + d  # dict 키 용
            dtime_pro = date_split_time[1]

            if ymd_str != ymd_date:
                break  # 오늘 아니면 탈출

            date_dic[ymd_str] = []

            if len(date_dic) > 2:
                del date_dic[ymd_str]
                break  # 딱 하루치만 보기

        else:
            dtime_pro = date_split_time[0]

        if press_url.find('finance.yahoo') == -1:
            continue
        if press_name not in c_press_list:
            continue

        date_dic[ymd_str].append([dtime_pro, press_name, press_title, press_url])
    return date_dic


def finviz_news_check(db, url, ticker):
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


def stock_info_load(db, select_ticker):
    checking_sql = "SELECT symbol, name, ISIN FROM Company_Information_Data"

    cur = db.cursor()
    cur.execute(checking_sql)
    result = cur.fetchall()

    df_db = pd.DataFrame(result, columns=['symbol', 'name', 'ISIN'])

    target_sname = []
    if select_ticker != 0 :
    # 선택한 것만 하는 것
        for s in range(len(select_ticker)):
            ticker = select_ticker[s]
            stock_info = df_db[df_db['symbol'] == ticker]
            list_con = stock_info.values.tolist()
            target_sname.append(list_con[0])

        df = pd.DataFrame(target_sname, columns=['symbol', 'name', 'ISIN'])
    else:
        df = df_db
    ticker_list = []

    for d in range(len(df)):
        ticker = df['symbol'].iloc[d]
        s_name = df['name'].iloc[d]
        s_code = df['ISIN'].iloc[d]
        ticker_list.append([ticker, s_name, s_code])

    return ticker_list


###################야후 파이낸스 구역 ############################

def yahoo_crawler(db, category):
    ## 미국 주식 거래시간
    us_market_time_dic = {'BMO(UST)': ['07:30:00', '09:30:00'], 'AMO(UST)': ['10:30:00', '11:30:00'],
                          'AMC(UST)': ['17:00:00', '18:30:00'], 'BMO(DST)': ['06:30:00', '09:00:00'],
                          'AMO(DST)': ['09:30:00', '10:30:00'], 'AMC(DST)': ['15:00:00', '17:00:00']}
    c_press_list = ['Bloomberg', 'Reuters', 'Yahoo Finance', 'Investing.com', 'Zacks', 'The Telegraph',
                    'Insider Monkey',
                    'Benzinga', 'Simply Wall St.', 'Business Wire', 'The Independent', 'Fortune', 'CoinDesk', 'Variety',
                    'PR Newswire', 'WWD', 'Skift', 'LA Times', 'The Guardian', 'Poets & Quants', 'GuruFocus.com', 'Kiplinger', 'Business Insider', 'USA TODAY']

    # c_press_list = ['Bloomberg', 'Reuters', 'Kiplinger', 'Business Insider', 'Yahoo Finance', 'USA TODAY', 'LA Times']
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}
    url_list = yahoo_crawling(db)
    market_time = us_market_time_dic[category]
    c = category
    # 임시 리턴
    pre_urls = []

    for u in range(len(url_list)):
        source = url_list[u][0]
        w_press = url_list[u][1]
        w_press = w_press.replace('News', '').lstrip()
        #print(w_press, source)
        pre_urls.append([w_press, source])

        if w_press in c_press_list:
            # try:
            news_parsing = fyahoo_news_info_extract_v1(source, headers, market_time)

            if news_parsing == 1:  # 시간대가 아니라면 ,
                continue
            else:
                print(news_parsing)
                t, h, b, w, n, p = news_parsing[1], news_parsing[2], news_parsing[3], news_parsing[4], news_parsing[5], news_parsing[6]
                result = news_insert(db, source, t, h, b, w, n, p, c)
            # except:
            #    continue

    return pre_urls


## 220614-15 임시 수집 타이밍  :  야후 파이낸스
def yahoo_crawling(db):
    url = 'https://finance.yahoo.com/topic/stock-market-news/'
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    #chrome_options = webdriver.ChromeOptions()
    #chrome_options.add_argument('--headless')
    #chrome_options.add_argument('--no-sandbox')
    #chrome_options.add_argument("--single-process")
    #chrome_options.add_argument("--disable-dev-shm-usage")

    path = 'chromedriver_105.exe'
    driver = webdriver.Chrome(path)  #, chrome_options=chrome_options)
    count = 200  # 페이지 한번에 보여줄수 있는양
    url_list = []
    driver.get(url)
    # 스크롤 전 높이
    before_h = driver.execute_script("return window.scrollY")
    # 무한 스크롤
    while True:
        # 맨 아래로 스크롤을 내린다.
        time.sleep(2)
        driver.find_element_by_css_selector("body").send_keys(Keys.END)
        # 스크롤 사이 페이지 로딩 시간
        time.sleep(2)
        # 스크롤 후 높이
        after_h = driver.execute_script("return window.scrollY")
        if after_h == before_h:
            break
        before_h = after_h
    #### 페이지내 url 긁어오기
    url_list = []

    for i in range(count):  # 페이지 한계로 200 개 정도 확인
        press_i = '//*[@id="Fin-Stream"]/ul/li[' + str(i + 1) + ']/div/div/div[2]'
        href_i = press_i + '/h3/a'
        href_xpath = driver.find_elements_by_xpath(href_i)
        press_xpath = driver.find_elements_by_xpath(press_i)
        if len(href_xpath) == 0:
            press_i = '//*[@id="Fin-Stream"]/ul/li[' + str(i + 1) + ']/div/div/div[1]'
            href_i = press_i + '/h3/a'
            href_xpath = driver.find_elements_by_xpath(href_i)
            press_xpath = driver.find_elements_by_xpath(press_i)
        if len(href_xpath) == 0:
            continue

        href = href_xpath[0].get_attribute('href')
        title = href_xpath[0].text
        press = press_xpath[0].text
        press = press.split('•')
        press = press[0].replace('Business', '').lstrip()
        url_list.append([href, press])

    driver.quit()

    return url_list


###################인베스팅 구역 ############################


## 뉴스 inversting.com 타겟
# 수집 대상 카테고리 : 주식시장 -> 셀레니움 (request 차단)  -> 추후 다시 수정하거나, 우선은 야후에 집중
def invert_news_crawler_v2(db, category):
    ## 미국 주식 거래시간
    us_market_time_dic = {'BMO(UST)': ['07:30:00', '10:00:00'], 'AMO(UST)': ['10:30:00', '11:30:00'],
                          'AMC(UST)': ['17:00:00', '18:30:00'], 'BMO(DST)': ['06:30:00', '09:00:00'],
                          'AMO(DST)': ['09:30:00', '10:30:00'], 'AMC(DST)': ['16:00:00', '17:30:00']}

    url = 'https://www.investing.com/news/'
    category_news = ['stock-market-news', 'commodities-news', 'economic-indicators']
    # 각 카테고리별 for 3 횟 문
    category_url = url + category_news[0]
    category_url = category_url +'/'+ '2'
    default_page = 2
    # default_page = page_select

    pre_url = 'https://www.investing.com'
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}
    market_time = us_market_time_dic[category]
    # c = category 나중에 카테고리 분류할때,

    path = 'chromedriver_105.exe'
    driver = webdriver.Chrome(path)
    driver.get(category_url)
    press_list = ['Investing.com']
    url_list = []

    for i in range(50):
        press_i = '//*[@id="leftColumn"]/div[4]/article[' + str(i + 1) + ']/div[1]'
        href_i = press_i + '/a'
        pname_i = press_i + '/span/span[1]'
        href_xpath = driver.find_elements_by_xpath(href_i)
        press_xpath = driver.find_elements_by_xpath(pname_i)
        if len(press_xpath) > 0:
            press_name = press_xpath[0].text.split('By')[1].lstrip()
            href_url = href_xpath[0].get_attribute('href')
            if press_name not in press_list:
                continue
            url_list.append([press_name, href_url])

    for u in range(len(url_list)):
        turl = url_list[u][1]  # 여기까지 한페이지내 첫 뉴스 기사 주소 가져오기
        print(turl)
        news_info = investing_news_info_extract(turl, headers, market_time)
        print(news_info)
        if news_info == 1:
            continue
        try:  # 220718  원인 불명  : ip 밴이나 다른 원인으로 추측
            ur, t, h, b, w, n, p = news_info[0], news_info[1], news_info[2], news_info[3], news_info[4], news_info[
                5], news_info[6]
            # print(ur)
            insert_db = news_insert(db, ur, t, h, b, w, n, p, c)
        except:
            time.sleep(5)
            continue
    return url_list
    ## 임의로, investing-news 만 우선 수집하도록.


## 인베스팅 페이지별 url 수집 : 종목 뉴스 버전 22개
def investing_stock_news_crawler(db, s_ids, s_news, s_names):
    url = 'https://www.investing.com/equities/'
    # 각 카테고리별 for 3 횟 문
    category_url = url + s_news
    ids = s_ids
    sn = s_names
    # category_url = category_url +'/'+ '5'
    default_page = 1
    # default_page = page_select
    pre_url = 'https://www.investing.com'
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}

    for p in range(default_page):  # 추후에 페이지별로 수집하기 위해 for문 사용, '/' + str(n)  -> 첫페이지는 사용안함
        response = requests.get(category_url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        page_source = soup.select('#leftColumn > div.mediumTitle1')
        # 페이지 넘기기
        p_title = page_source[0].find_all('div', {'class': 'textDiv'})

        # 한 페이지내 url 리스트화 하기
        url_list = []
        # t_list = []
        # 임시 언론사 명 추출
        press_list = []

        for i in range(10):  # 1 page 당 10 개의 글이 올라옴
            press_name = page_source[0].find_all('span', {'class': 'articleDetails'})[i].text
            press_name = press_name.split('\xa0')[0]
            press = press_name.split(' ')[1]

            url_p = p_title[i].find('a')['href']
            kurl = pre_url + url_p
            # t_list.append(title_p)
            if press == 'Investing.com':
                press = 'investing-new'
            url_list.append(kurl)
            press_list.append(press)

            for u in range(len(url_list)):
                turl = url_list[u]  # 여기까지 한페이지내 첫 뉴스 기사 주소 가져오기
                p = press_list[u]
                news_info = investing_stock_news_info_extract(turl, headers)
                ur, t, h, b, w, n = news_info[0], news_info[1], news_info[2], news_info[3], news_info[4], news_info[5]
                insert_db = stock_news_insert(db, ur, t, h, b, w, n, p, sn, ids)

        # 뉴스 파싱한 후
        # news_data.append(press)


################### 파싱 함수구역 ############################


# 하나의 뉴스 기사 에서 데이터 추출하기 : 시황 뉴스 (추후 수정)
def investing_news_info_extract(url, headers, market_time):
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    print(soup)
    # 날짜
    try:  # 날짜가 안찍히는 경우 'None' 으로 확인되고, 될때도 있고 안될때도 있음.
        date_time, limit_start, limit_end = investing_market_search_time(soup, market_time)

        if date_time >= limit_start and date_time < limit_end:  # 시황 마감후 30분 까지가 아니라면
            now = dtime.now()
            # 제목
            title = soup.find("meta", property="og:description")
            title = title['content']
            # 본문
            cont = soup.find('div', {'class': 'WYSIWYG articlePage'})
            conts = cont.find_all('p')
            for c in range(len(conts)):
                if conts[c].find('strong') != None:
                    conts[c].find('strong').decompose()

            conts = invest_context_end_kill(conts)
            body_ = context_preprocessing(conts)
            # 언론사명
            press_div = soup.find('div', {'class': 'contentSectionDetails'})
            press_img = press_div.find('img')
            press_split = str(press_img).split('/')
            press = press_split[-2].split('.')
            press = press[0]

            if press == 'Reuters':
                try:
                    head_ = period_index(conts)
                except:
                    head_ = conts
                body = body_p_clean(head_, press)
                body = body.split('(Reporting')
                bodys = body[0]
                bodys = bodys.split('((reporting')
                bodys = bodys[0]
                body = press_local_find(bodys, press)
                body = body.strip()
                headline_ = sent_tokenize(body)
                headline_ = headline_[:2]
                if len(headline_) < 2:
                    headline = headline_[0]
                else:
                    headline = headline_[0] + ' ' + headline_[1]

            elif press == 'investing-new':
                body_p = conts_clean(conts)
                # body_p = body_split(body)
                body_p = body_p.replace('\xa0', ' ')
                body = body_p.strip()
                headline_ = sent_tokenize(body)
                headline_ = headline_[:2]
                headline = headline_[0] + ' ' + headline_[1]

            else:  # 그외
                body_p = body_split(body_)
                body_p = body_p.replace('\xa0', ' ')
                # 패턴형으로 변경, 로이터랑 다르게  리스트 인덱스
                continue_ = 'Continue Reading'  # 패턴 함수로 확장
                body = body_.split(continue_)[0]
                body = body.strip()
                # body  = body.replace('\xa0',' ')
                headline_ = sent_tokenize(body)
                headline_ = headline_[:2]
                headline = headline_[0] + ' ' + headline_[1]

            news_data = [url, title, headline, body, date_time, now, press]
            # 분류 : 인베스팅 한정 제목 분류
            checking = title_classifier(title)
            # 인베스팅 헤드라인도 이제 분석해서 시황 뉴스로 분리해야함
            # if checking != 1:  # 제목에 특정 문구가 없다면, 헤드 라인 기반으로 검사, 샘플 수 가 적어 야후는 감안해야함
            #    head_check = news_close_time(headline)
            #    if head_check <= 1:
            #        news_data = 1

            if checking != 1:
                news_data = 1


        else:  # 시간대가 아니면...
            news_data = 1
    except:
        news_data = 2  # 원인불명 블락인가?
    return news_data


# 인링크 방식 뉴스 내용 파싱 : 야후 버전 실상 버전 2이나 구별을 위해 inlink만 사용
def fyahoo_stock_news_inlink_extract(url, headers):
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    news_data = []

    date_time = yahoo_market_time(soup)
    now = dtime.now()
    title = soup.find("meta", property="og:title")
    title = title['content']

    press = soup.find("img", {'class': "caas-img"})
    press = press['alt']

    conts = soup.find_all('p')  # .text
    content = soup.find_all('div', {'class': 'caas-body'})
    body_p = content[0]('p')
    body = body_p_clean(body_p, press)

    if press == 'USA TODAY':
        headline = context_preprocessing(conts[1:3])
        # body = context_preprocessing(conts)
    if press == 'Reuters':
        # body_p = period_index(body_p)
        body = body_p_clean(body_p, press)
        body = body.split('(Reporting')
        bodys = body[0]
        body = press_local_find(bodys, press)
    if press == 'LA Times':
        body = body.split('This story originally')
        body = body[0]
    if press == 'Bloomberg':
        body = body.split('(Updates')
        body = body[0]

    if press == 'Investing.com':
        body = body.split('Related Articles')
        body = body[0]

    if press == 'Zacks':
        body = body.split('Want the latest')
        body = body[0]

    body = body_split(body)
    headline_token = sent_tokenize(body)
    if len(headline_token) <= 2:
        headline = body
    else:
        headline = headline_token[0] + ' ' + headline_token[1]

    inlink_news = [url, title, headline, body, date_time, now, press]

    return inlink_news


################### 종목 뉴스 함수 구역 ############################

# 인베스팅 종목 뉴스 파싱 함수

# 인베스팅 주식 종목 뉴스 정보 추출 함수
def investing_stock_news_info_extract(url, headers):
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    date_time, limit_start, limit_end = investing_market_search_time(soup, 0)
    now = dtime.now()
    # 제목
    title = soup.find("meta", property="og:description")
    title = title['content']
    # 본문
    cont = soup.find('div', {'class': 'WYSIWYG articlePage'})
    conts = cont.find_all('p')
    for c in range(len(conts)):
        if conts[c].find('strong') != None:
            conts[c].find('strong').decompose()

    conts = invest_context_end_kill(conts)
    # body = context_preprocessing(conts)

    # 언론사명 -> 일부 언론사명이 깨짐으로써, 페이지내 있는 언론사 사용, 여깄는 것은 분별용
    press_div = soup.find('div', {'class': 'contentSectionDetails'})
    press_img = press_div.find('img')
    press_split = str(press_img).split('/')
    press = press_split[-2].split('.')
    press = press[0]

    if press == 'Reuters':
        try:
            head_ = period_index(conts)
        except:
            head_ = conts
        body = body_p_clean(head_, press)
        body = body.split('(Reporting')
        bodys = body[0]
        bodys = bodys.split('((reporting')
        bodys = bodys[0]
        body = press_local_find(bodys, press)
        body = body.strip()
        headline_ = sent_tokenize(body)
        headline_ = headline_[:2]
        if len(headline_) < 2:
            headline = headline_[0]
        else:
            headline = headline_[0] + ' ' + headline_[1]

    elif press == 'investing-new':
        body_p = conts_clean(conts)
        # body_p = body_split(body)
        body_a = body_p.replace('\xa0', ' ')
        body = body_a.strip()
        headline_ = sent_tokenize(body)
        headline_ = headline_[:2]
        headline = headline_[0] + ' ' + headline_[1]

    else:  # 그외
        body = context_preprocessing(conts)
        body_p = body_split(body)
        body_p = body_p.replace('\xa0', ' ')
        # 패턴형으로 변경, 로이터랑 다르게  리스트 인덱스
        continue_ = 'Continue Reading'  # 패턴 함수로 확장
        body = body.split(continue_)[0]
        body = body.strip()
        # body  = body.replace('\xa0',' ')
        headline_ = sent_tokenize(body)
        headline_ = headline_[:2]
        headline = headline_[0] + ' ' + headline_[1]


    news_data = [url, title, headline, body, date_time, now]  # press]

    # 분류 : 인베스팅 한정 제목 분류
    # checking = title_classifier(title)
    # 인베스팅 헤드라인도 이제 분석해서 시황 뉴스로 분리해야함
    # if checking != 1:  # 제목에 특정 문구가 없다면, 헤드 라인 기반으로 검사, 샘플 수 가 적어 야후는 감안해야함
    # news_data = 1
    print(news_data)

    return news_data


################### 기타 전처리 함수구역 ############################

# 만일을 위해서 함수화 했는데.. 생각보다 전처리가 잘 되었기에 ...
def clean_text(text):
    # cleaned_text = text.replace("\'", "'")
    cleaned_text = text.replace('  \n ', '')
    cleaned_text = cleaned_text.replace('(Bloomberg) -- ', '')

    return cleaned_text


# 날짜를 datetime format으로 변환 하기 위한 함수
def dt_format(dt_texts):
    a = dt_texts.find('(')
    b = dt_texts.find(')')

    result = dt_texts[a + 1:b]
    result = result.replace(',', '')
    dt = result.split(' ')
    dt_pm = dt[3].find('PM')  # 있으면 + 없으면 -1 반환

    if dt_pm != -1:  ## 오후 타임이면,
        pre_dt = dt[3].replace('PM', '')
        pre_dt = pre_dt.split(':')
        if int(pre_dt[0]) == 12:
            t, m = pre_dt[0], pre_dt[1]
        else:
            pm_time = str(int(pre_dt[0]) + 12)
            t, m = pm_time, pre_dt[1]

    else:  # 오전타임이면
        pre_dt = dt[3].replace('AM', '')
        pre_dt = pre_dt.split(':')
        t, m = pre_dt[0], pre_dt[1]

    dt_mon = strptime(dt[0], '%b').tm_mon

    if dt_mon <  10:
        dt_mon = '0' + str(dt_mon)

    str_datetimed = dt[2] + '-' + str(dt_mon) + '-' + dt[1] + ' ' + t + ":" + m + ":" + '00'
    currdate = datetime.datetime.strptime(str_datetimed, '%Y-%m-%d %H:%M:%S')

    return currdate


# 본문 리스트화된 것을 문자열로 연결
def context_preprocessing(cp):
    texts_list = []
    texts = ''
    for i in range(len(cp)):
        texts = texts + cp[i].text + ' '
        texts_list.append(cp[i].text)
    cleans = clean_text(texts)
    return cleans


### 뉴스 분류  : 키워드 방식인데, 아무래도 일부 키워드는 해결 불가하기에, 모델 구축 방향으로 가야함, 데이터가 쌓이면

# 맞는 뉴스인지 아닌지 판단 하기  : 키워드 기반
def title_classifier(title):
    # headline  = data[2]
    t_check = title_key_check(title)
    # h_check= news_close_time(headline )
    classifier = 0
    if t_check >= 1:
        classifier = 1  # 제목에 시황 뉴스를 암시하는 'stock market today' 가 있다면,
    # 제목엔 없지만 헤드라인에 관련 키워드가 한개이상이라면,

    return classifier


# # 인베스팅 및 다른 규칙 제목 한정 제목 분류
def title_key_check(sentence):
    title_key = ['smtody', 'cloftrade', 'wallst', 'pts', 'usastocks', 'liveupdate', 'usatocksturn', 'marketshows']
               #  'markets', 'market']  # 'futures',
    un_key = ['emerging', 'EMERGING', 'global', 'GLOBAL']
    sentence = sentence.lower()
    sentence = sentence.replace("stock market today", title_key[0])
    sentence = sentence.replace("stock market today:", title_key[0])
    sentence = sentence.replace("close of trade", title_key[1])
    sentence = sentence.replace("us stocks-wall", title_key[2])
    # sentence = sentence.replace("wall st", title_key[2])
    sentence = sentence.replace("pts;", title_key[3])
    sentence = sentence.replace("u.s. stocks", title_key[4])
    sentence = sentence.replace("live updates:", title_key[5])
    sentence = sentence.replace("us stocks turn", title_key[6])
    sentence = sentence.replace("market shows", title_key[7])

    word_count = 0
    for t in range(len(title_key)):
        if title_key[t] in sentence:
            word_count += 1
        # 제거
        if title_key[t] in un_key:
            word_count = 0
            break

    return word_count


# 야후 파이낸스 전용 본문 추출

def body_p_clean(body_p, press):
    # body_p = soup.find_all('p')
    body = ''

    for s in range(len(body_p)):
        body_text = body_p[s].text
        if press == 'Yahoo Finance':
            if body_text == '—':
                break
        if press == 'Bloomberg':
            if s == 0:
                body_text = bloomberg_match(body_text)
            else:
                if body_p[s].find('a') is not None:
                    continue
                more_finding = body_text.find('Most Read')
                if more_finding == 0:
                    continue
                read_finding = body_text.find('Read more:')
                if read_finding == 0:
                    continue
                body_text = bloomberg_end_match(body_text)
                if body_text.find('More market') != -1:
                    break
                if len(body_text) == 0:
                    continue
                if body_text.find('this week:') != -1:
                    break
        if press == 'Kiplinger':
            if body_text.find('SEE MORE') != -1:
                continue
            if body_text.find('Sign up') != -1:
                continue
            if body_text.find('YCharts') != -1:
                break

        if press == 'Zacks':
            # check_tag = body_p[s].find('strong')
            # if check_tag != None:
            #    continue
            zacks_finding = body_text.find('Zacks Investment Research?')

            if zacks_finding >= 0:
                continue

            infull_finding = body_text.find('[In full disclosure,')
            if infull_finding >= 0:
                continue

        if press == 'Yahoo Finance':
            check_tag = body_p[s].find('em')
            if check_tag != None:
                continue
            yahoo_click_finding = body_text.find('Click here for')
            if yahoo_click_finding >= 0:
                continue
            yahoo_read_finding = body_text.find('Read the latest')
            if yahoo_read_finding >= 0:
                continue

        if press == 'Benzinga':
            see_benzinga_finding = body_text.find('See more')
            if see_benzinga_finding >= 0:
                break

        if press == 'Insider Monkey':
            insider_monkey_c_finding = body_text.find('Click to continue')
            if insider_monkey_c_finding >= 0:
                break
            insider_monkey_s_finding = body_text.find('Suggested Articles:')
            if insider_monkey_s_finding >= 0:
                break

        if press == 'Business Wire':
            business_w_finding = body_text.find('NOTE TO')
            if business_w_finding >= 0:
                break

        if press == 'Fortune':
            fortune_finding = body_text.find('featured on Fortune.com')
            if fortune_finding >= 0:
                break

        if press == 'Simply Wall St.':
            sw_st_finding = body_text.find('free platform')
            if sw_st_finding >= 0:
                break

        if press == 'Variety':
            variety_finding = body_text.find('Best of Variety')
            if variety_finding >= 0:
                break

        if press == 'PR Newswire':
            pr_newswire_finding = body_text.find('Trademarks')
            if pr_newswire_finding >= 0:
                break

        if press == 'Skift':
            skift_finding = body_text.find('Subscribe to Skift')
            if skift_finding >= 0:
                break

        if press == 'Poets & Quants':
            pq_finding = body_text.find('MISS POLL')
            if pq_finding >= 0:
                break

        if press == 'ACCESSWIRE':
            accwire_finding = body_text.find('View additional')
            if accwire_finding >= 0:
                break

        if press == 'InvestorPlace':
            inplace_finding = body_text.find('More From InvestorPlace')
            if inplace_finding >= 0:
                break

        if press == 'GuruFocus.com':
            gf_finding = body_text.find('appeared on GuruFocus')
            if gf_finding >= 0:
                break

        if s == 0:  # 첫 빈 공간 고려하기
            body += body_text
        else:
            body = body + '\n' + body_text

    return body


def body_split(text):
    tlink = ''
    tokenized_text = sent_tokenize(text)

    for d in range(len(tokenized_text)):
        tokend = tokenized_text[d]
        report_kill = reporter_match(tokend)
        report_kill = report_kill.lstrip()
        site_kill = investing_match(report_kill)
        tokend = site_kill.strip()
        if len(tokend) == 0:
            continue
        if d == 0:
            tlink = tokend
        else:
            tlink = tlink + ' ' + tokend

    return tlink


def reporter_match(text):
    # 기자 이름 지우기
    by_ = 'By'
    name_ = '[a-zA-Z]+'
    reporter_by = re.compile(by_)
    reporter_first_name = re.compile(name_)
    reporter_last_name = re.compile(name_)

    reporter_pattern = re.compile(by_ + ' ' + name_ + ' ' + name_)
    reporter = reporter_pattern.match(text)
    if reporter != None:
        reporter = reporter.group(0)
        text = text.replace(reporter, '')

    return text


def reporter_match_and(text):
    # 기자 이름 지우기
    by_ = 'By'
    name_ = '[a-zA-Z]+'
    reporter_by = re.compile(by_)
    reporter_first_name = re.compile(name_)
    reporter_last_name = re.compile(name_)

    reporter_pattern = re.compile(by_ + ' ' + name_ + ' ' + name_)
    reporter = reporter_pattern.match(text)
    if reporter != None:
        reporter = reporter.group(0)
        text = text.replace(reporter, '')

    return reporter

def investing_match(text):
    # 언론사 이름 지우기
    site_ = 'Investing.com'
    spect_ = '[–|-]{1,3}'

    site_pattern = re.compile(site_ + ' ' + spect_)
    site_name = site_pattern.match(text)
    # print(site_name)
    if site_name != None:
        site_name = site_name.group(0)

        text = text.replace(site_name, '')
    return text

def bloomberg_match(text):
    # 언론사 이름 지우기
    site_pattern = '(Bloomberg)'
    site_name = text.replace(site_pattern, '').lstrip()
    spect_pattern = re.compile('[-]+')

    doc = spect_pattern.match(site_name)
    if doc != None :
        doc = doc.group(0)
        text = site_name.replace(doc,'').lstrip()
    return text

def bloomberg_end_match(text):
    # 언론사 이름 지우기
    #text =  '©2022 Bloomberg L.P.'
    c_spect = '©[0-9]+\s[a-zA-Z]+\s[a-zA-Z]+.[a-zA-Z]+.'
    c_pattern = re.compile(c_spect )
    doc = c_pattern.match(text)

    if doc != None :
        doc = doc.group(0)
        text = text.replace(doc,'').strip()
    return text



def period_index(body_p):
    period_index = ''
    by_ = 'By'
    name_ = '[a-zA-Z]+'
    reporter_pattern = re.compile(by_ + ' ' + name_ + ' ' + name_)

    for r in range(len(body_p)):
        text = body_p[r].text
        report_split = reporter_pattern.match(text)
        if report_split != None:
            rs_ = body_p.index(body_p[r])
            period_index = rs_

    body_p = body_p[period_index + 1:]

    return body_p


# 로이터 용 언론사명 본문에서 끄집어서 짜르기
def press_local_find(text, press):
    # 본문내 불필요한 요일 언론사 이름 지우기
    # press = 'Reuters'
    spect_p = '(' + press + ')' + ' ' + '-'
    dt_press = text.find(spect_p)
    text = text[dt_press + len(spect_p):]
    text = text.lstrip()

    return text


# 220722 로이터 전처리 최신 버전
def reuters_preprocess(body_p):
    #body_list = []
    texts = ''
    cnt = 0
    for i in range(len(body_p )):
        text = body_p[i].text
        if text.find('(Updates with') >= 0:
            continue
        if text.find('For a Reuters') >= 0:
            continue
        if text.find('*') >= 0:
            continue
        reporter_name = reporter_match_and(text)
        #print(len(str(reporter_name)),reporter_name )
        if len(str(reporter_name)) > 5 :
            continue
        if text == '':
            continue
        text = reuters_first_del(text)
        if text.find('Reporting') >= 0:
            text = text.split('(Reporting')
            text = text[0]
        if cnt == 0:
            texts = text
        else:
            texts = texts + ' '+text
        #print(text)
        cnt += 1
        #body_list.append(text)
    return texts


def reuters_first_del(text):
    text = text.replace('NEW YORK,','')
    text = text.lstrip()
    c_spect = '[a-zA-Z]+\s[0-9]+\s[^a-zA-Z0-9]{1}[Reuters]+[^a-zA-Z0-9]{1}'
    c_pattern = re.compile(c_spect )
    doc = c_pattern.match(text)

    if doc != None :
        doc = doc.group(0)
        text = text.replace(doc,'').strip()

    text = text.lstrip('-')
    text = text.lstrip()
    return text

# 220927 인베스팅 본문 오류 해결 함수
def conts_clean(conts):
    conts_del = ['Position added successfully to: \n']
    conts_full = ''

    for c in range(len(conts)):
        page_line = conts[c].text
        if page_line in conts_del:
            continue
        page_line = reporter_match(page_line)
        page_line = investing_match(page_line)
        conts_full += page_line + ' '
    conts_full = conts_full.strip()

    return conts_full





## 그외 기타 전처리 함수
# close_time_headline_keywords = ['usastocks', 'usastock', 'djia' , 'nasdaq', 'nyse','gspc','point']

def news_close_time(content):
    headline_keywords = ['usastocks', 'usastock', 'djia', 'nasdaq', 'nyse', 'gspc', 'point']
    checking_text = market_preprocessing(content)
    word_count = 0
    ## 내용 검사
    for i in range(len(checking_text)):
        word = checking_text[i]
        if word in headline_keywords:
            word_count += 1

    return word_count  # 1 이상이면 맞다는 것.


def market_preprocessing(sentence):
    stop_words = set(stopwords.words('english'))
    sentence = sentence.lower()
    sentence = sentence.replace("standard & poor's 500", 'gspc')
    sentence = sentence.replace('s&p 500', 'gspc')
    sentence = sentence.replace('s&p', 'gspc')
    sentence = sentence.replace('u.s. stock', 'usastock')
    sentence = sentence.replace('dow jones industrial average', 'djia')
    sentence = sentence.replace('%', ' ')
    sentence = sentence.replace('%,', ' ')
    word_tokens = word_tokenize(sentence)  # 월스트리트저널 기반 분해 -> s&p 500 을 산산조각냄
    result = []
    for w in word_tokens:
        if w not in stop_words:
            result.append(w)
    lemm = WordNetLemmatizer()
    wpos = []
    # 이건 필요없을 듯
    for w in result:
        wn = lemm.lemmatize(w, pos='n')
        wpos.append(wn)
    tagged_list = pos_tag(wpos)

    sw = ['.', ',', ':', 'CD', '(', ')', '``', "''", 'POS']
    removed_list = []
    for word in tagged_list:
        if word[1] not in sw:
            removed_list.append(word[0])
    removed_list = set(removed_list)
    removed_list = list(removed_list)

    return list(removed_list)


# 본문 리스트화된 것을 문자열로 연결
def invest_context_end_kill(cp):
    texts_list = []
    texts = ''
    con = 'continue reading'
    e_num_pattern = re.compile('^©[0-9]{1,4}\s[a-zA-Z]+')  # \s[a-zA-Z]+

    if cp[-1].text.lower().find(con) >= 0:
        del cp[-1]
    if cp[0].text == '\n':
        del cp[0]

    for i in range(len(cp)):
        m = e_num_pattern.match(cp[i].text)
        if m != None:
            pm = m.group(0)
            if pm.find(pm) >= 0:
                del cp[i]
                break

    return cp


###### 데이터 불러오는 것 ###########################################

def korean_like_data_split(df):
    used_dk = df[df['사용 여부'] == 1]
    df_sample = used_dk.loc[:, ['종목코드', '종목명', '종목 뉴스 주소']]
    category_ids = []
    category_news = []
    category_names = []

    for s in range(len(df_sample)):
        stock_news = df_sample['종목 뉴스 주소'].iloc[s].split('/')
        stock_news = stock_news[-1]
        stock_ids = df_sample['종목코드'].iloc[s]
        stock_name = df_sample['종목명'].iloc[s]
        category_ids.append(stock_ids)
        category_news.append(stock_news)
        category_names.append(stock_name)

    return category_ids, category_news, category_names

## 아마존 DB 티커 불러올때, 핀비즈에서 검색 안되는 리스트
def notfind_symbol_load():
    unfind_df = pd.read_excel('./data/notfind_symbol.xlsx')
    unfind_df_list = unfind_df.values.tolist()
    un_symbol_list = []

    for u in range(len(unfind_df_list)):
        un_symbol = unfind_df_list[u][0]
        un_symbol_list.append(un_symbol)
    return un_symbol_list


##### DB 함수들 ################################################


## DB 저장용 -> 추후 오라클 DB화
# 시황 뉴스 데이터 추가 - Question table
def news_insert(db, url, title, headline, body, w_date, n_date, press, market_time):
    category = 'market_news'
    cur = db.cursor()
    sql = """insert into US_Stock_Market_News(URL, title, headline, body, w_date, n_date, press, category, market_time) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    try:
        cur.execute(sql, (url, title, headline, body, w_date, n_date, press, category,market_time ))
        db.commit()
    except pymysql.Error as msg:
        pass


# 종목 뉴스 데이터 추가 - Question table
def stock_news_insert(db, url, title, headline, body, w_date, n_date, press, category, ticker, stock_name, stock_code):
    cur = db.cursor()  # ISIN
    sql = """insert into US_Stock_Market_News(url, title, headline, body, w_date, n_date, press, category, symbol, name, ISIN) values (%s, %s, %s, %s, %s, %s, %s, %s, %s , %s, %s)"""
    try:
        cur.execute(sql, (url, title, headline, body, w_date, n_date, press, category, ticker, stock_name, stock_code))
        db.commit()
    except pymysql.Error as msg:
        print(msg)
        pass


################## 안쓰게됬거나 버전업 이전의 함수 #####################


## 뉴스 inversting.com 타겟
# 수집 대상 카테고리 : 주식시장
def invert_news_crawler(db, category):
    ## 미국 주식 거래시간
    us_market_time_dic = {'BMO(UST)': ['07:30:00', '09:30:00'], 'AMO(UST)': ['10:30:00', '11:30:00'],
                          'AMC(UST)': ['17:00:00', '18:30:00'], 'BMO(DST)': ['06:30:00', '09:00:00'],
                          'AMO(DST)': ['09:30:00', '10:30:00'], 'AMC(DST)': ['15:00:00', '17:00:00']}

    url = 'https://www.investing.com/news/'
    category_news = ['stock-market-news', 'commodities-news', 'economic-indicators']
    # 각 카테고리별 for 3 횟 문
    category_url = url + category_news[0]
    # category_url = category_url +'/'+ '3'
    default_page = 2
    # default_page = page_select
    pre_url = 'https://www.investing.com'
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}
    market_time = us_market_time_dic[category]
    c = category
    for p in range(default_page):
        if p > 0:
            category_url = category_url + '/' + str(p + 1)

        response = requests.get(category_url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        page_source = soup.select('#leftColumn > div.largeTitle')

        # leftColumn > div.largeTitle > article:nth-child(1) > div.textDiv > a
        # 페이지 넘기기
        p_title = page_source[0].find_all('a')
        # 한 페이지내 url 리스트화 하기
        url_list = []
        for i in range(len(p_title)):  # 광고를 섞어났음;;
            try:  # 제목만 있는 url 추출
                title_p, url_p = p_title[i]['title'], p_title[i]['href']
                kurl = pre_url + url_p
                url_list.append(kurl)
            except:
                continue


        for u in range(len(url_list)):
            turl = url_list[u]  # 여기까지 한페이지내 첫 뉴스 기사 주소 가져오기
            news_info = investing_news_info_extract(turl, headers, market_time)
            if news_info == 1:
                continue
            try:  # 220718  원인 불명  : ip 밴이나 다른 원인으로 추측
                ur, t, h, b, w, n, p = news_info[0], news_info[1], news_info[2], news_info[3], news_info[4], news_info[5], news_info[6]
                #print(ur, t)
                # print(ur)
                insert_db = news_insert(db, ur, t, h, b, w, n, p, c)
            except:
                time.sleep(5)
                continue

# 종목 뉴스 데이터 추가 - Question table
def stock_news_insert_v1(db, url, title, headline, body, w_date, n_date, press, stock_name, stock_code):
    cur = db.cursor()
    sql = """insert into US_Stock_News(URL, title, headline, body, w_date, n_date, press, stock_name, stock_code) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    try:
        cur.execute(sql, (url, title, headline, body, w_date, n_date, press, stock_name, stock_code))
        db.commit()
    except pymysql.Error as msg:
        ''' ## 중복 처리는 다음에... 
        a = stock_name+ ','
        b = stock_code + ','
        #url = 'https://www.investing.com/news/commodities-news/us-auto-agency-will-not-allow-ev-owners-to-pick-alert-sounds-2846490'
        sql = """UPDATE US_Stock_News SET stock_name = CONCAT('""" + a + """', stock_name ) , stock_code = CONCAT('""" + b + """',stock_code) where URL = '""" + url + """'"""
        cur.execute(sql)
        db.commit()
        '''
        pass


# 인링크 방식 뉴스 내용 파싱 : 야후 버전
def fyahoo_news_info_extract_v1(url, headers, market_time):
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    news_data = []

    date_time, limit_start, limit_end = yahoo_market_search_time(soup, market_time)

    if date_time >= limit_start and date_time < limit_end:  # 시황 마감후 1시 30분 까지가 아니라면
        now = dtime.now()
        title = soup.find("meta", property="og:title")
        title = title['content']

        press = soup.find("img", {'class': "caas-img"})
        press = press['alt']

        conts = soup.find_all('p')  # .text
        content = soup.find_all('div', {'class': 'caas-body'})
        body_p = content[0]('p')

        # body_p = period_index(body_p)
        body = body_p_clean(body_p, press)

        # if headline.find('*') == -1:
        #    headline = context_preprocessing(conts[:2])
        if press == 'USA TODAY':
            headline = context_preprocessing(conts[1:3])
            # body = context_preprocessing(conts)
        if press == 'Reuters':
            body = reuters_preprocess(body_p)
        if press == 'LA Times':
            body = body.split('This story originally')
            body = body[0]
        # if press == 'Kiplinger':
        #    body = body.split('You may also like')
        #    body = body[0]

        body = body_split(body)
        headline_token = sent_tokenize(body)
        if len(headline_token) <= 2 :
            headline = body
        else:
            headline = headline_token[0] + ' ' + headline_token[1]

        inlink_news = [url, title, headline, body, date_time, now, press]
        #print(inlink_news)
        # 분류
        checking = title_classifier(title)

        if checking != 1:  # 제목에 특정 문구가 없다면, 헤드 라인 기반으로 검사, 샘플 수 가 적어 야후는 감안해야함  -> 너무 범위가 좁아짐... 우선 제목만
            inlink_news = 1

        if len(body) < 1 : # 수집 당시 본문 내용 없던 뉴스 제거
            inlink_news = 1
        #print(inlink_news)
    else:
        inlink_news = 1
    news_data = inlink_news

    return news_data
