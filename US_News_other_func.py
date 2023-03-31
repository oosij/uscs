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
import random
import json

headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
        'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}



# 임시용

# 임시 시가총액에 기반한 ETF 100개   ->  ['심볼' , '이름', '분류']
def symbols_load_test():
    etf_path = './data/ETF_top100_sample.xlsx'
    df = pd.read_excel(etf_path)

    symbol_list = []

    for d in range(len(df['Symbol'])):
        ticker = df['Symbol'].iloc[d]
        symbol_list.append([ticker, '', ''])
    etf_df = pd.DataFrame(symbol_list, columns=['Symbol', 'Name', 'Class'])

    return etf_df

#=========================================================================
#                         뉴스 DB 저장용 221129
#=========================================================================

# 종목 뉴스 데이터 추가 - Question table
def stock_news_insert(db, url, title, body, w_date, n_date, press, category, ticker):
    cur = db.cursor()  # ISIN
    sql = """insert into US_Stock_Market_News(url, title, body, w_date, n_date, press, category, symbol) values (%s, %s, %s, %s, %s, %s, %s, %s)"""
    try:
        cur.execute(sql, (url, title, body, w_date, n_date, press, category, ticker))
        db.commit()
    except pymysql.Error as msg:
        print(msg)
        pass


#=========================================================================
#                        인링크 뉴스 크롤러 221129
#=========================================================================

### Barchart
def barchart_news_crawler(select):
    # 로컬 DB TEST 접속
    db = pymysql.connect(host='127.0.0.1', port=3306, user='root',
                         password="1234", db='jisoo', charset='utf8')

    news_list = []

    if select == 'etf':  # 이건 나중에...
        #select_df = symbols_load_test()
        select_df = cnbc_etf_list()
        select_on = select + 's-funds'


    if select == 'future':
        select_df = barchart_commdities_table_info()
        select_on = select + 's'


    category = select + '_news'

    for i in range(len(select_df)):
        symbol = select_df['Symbol'][i]
        # url = 'https://www.barchart.com/' + select_on + '/quotes/'+ symbol+'/news'
        url = 'https://www.barchart.com/' + select_on + '/quotes/' + symbol + '/news'
        # c_name = select_df ['Symbol'][i]  -> 카테고리명 추후 수정 , 원자재 같은 경우에 필요 sub_category
        # c_name = select_on

        purl_list = barchart_news_crawling(url, headers)

        for u in range(len(purl_list)):
            press = purl_list[u][0]
            turl = purl_list[u][1]
            symbol_input = symbol.replace('*0', '')
            overlap_checking = news_overlap_check(db, turl, symbol_input)
            if overlap_checking == 1:
                continue  # break가 나을거같긴한데...
            try: ## 원인 불명 데이터 missing 경우... 일단 예외처리!
                news_info = barchart_news_inlink_parser(turl, headers, press)
            except:
                continue
            # inser db info
            in_url, title, body = news_info[0], news_info[1], news_info[2]
            w_date, n_date = news_info[3], news_info[4]
            press = news_info[5]
            if press.find('Motley') >= 0:
                press = 'The Motley Fool'
            result = stock_news_insert(db, in_url, title, body, w_date, n_date, press, category, symbol_input)
            news_list.append(news_info)
            time.sleep(2)
    return news_list

def barchart_news_crawling(url, headers):
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    surl = soup.find_all('div', {'class': 'story clearfix'})
    purl_list = []

    for s in range(len(surl)):
        kurl = surl[s]
        link_url = kurl.find('a')['href']
        press = kurl.find('span').text
        press_split = press.split('-')
        press = press_split[0].strip()
        purl_list.append([press, link_url])
        time.sleep(2)
    return purl_list


#=========================================================================
#                         인링크 뉴스 파서  221129
#=========================================================================

### Barchart
def barchart_news_inlink_parser(url, headers, press):
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    #  제목
    title = soup.find("meta", property="og:title")
    title = title['content']

    now = dtime.now()
    dt = soup.find("meta", itemprop="datePublished")
    dt_str = dt['content']
    datetime_format = '%m/%d/%y %H:%M:%S'
    dt = dtime.strptime(dt_str, datetime_format)

    div_content = soup.find('div', {'class': 'bc-news-item'})
    str_dict = div_content.find('data-media-overlay-news')['data-news-item']
    json_content = json.loads(str_dict)
    clean_text = json_content['content']
    body = re.sub('(<([^>]+)>)', '', clean_text)
    body = body.strip()

    news_data = [url, title, body, dt, now, press]

    return news_data


#=========================================================================
#                            기타 함수들  221129
#=========================================================================

def barchart_commdities_table_info():
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}

    url = 'https://www.barchart.com/futures/highs-lows/all'
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    s_info_list = []

    commodity_find = soup.find('div', {'class': 'commodity-select'})
    optgroup_category = commodity_find('optgroup')

    for c in range(len(optgroup_category)):
        category = optgroup_category[c]
        category_name = category['label']
        category_name = category_name.replace('-', '')
        if category_name.find('European') >= 0:
            break

        symbol_info = category.find_all('option')

        for s in range(len(symbol_info)):
            symbol = symbol_info[s]['value'].split('/')[-2]  # *0
            name = symbol_info[s].text
            s_info = [symbol, name, category_name]
            s_info_list.append(s_info)
    # print(len(s_info_list))
    commdities_df = pd.DataFrame(s_info_list, columns=['Symbol', 'Name', 'Class'])
    return commdities_df




# barchart 임의로, 용무가 끝나면 삭제
def etf_symbols_load():
    #etf_path = './data/ETF_top100_sample.xlsx'
    etf_path = './data/ETF_sample_12.xlsx'
    df = pd.read_excel(etf_path)

    symbol_list = []

    for d in range(len(df['Symbol'])):
        ticker = df['Symbol'].iloc[d]
        symbol_list.append(ticker)
    return symbol_list


# 뉴스 중복 체크 : url, symbol 확인 후, continue 가능하도록!
def news_overlap_check(db, url, ticker):
    # db에 url 가 있다면 break! ]
    checking_sql = "SELECT * FROM US_Stock_Market_News WHERE url = '" + url + "' and symbol ='" + ticker + "'"

    cur = db.cursor()
    cur.execute(checking_sql)
    result = cur.fetchall()

    if len(result) >= 1:
        result = 1  # 1이면 continue
    else:
        result = 0
    return result

## cnbc 뉴스 사이트의 지수/원자재 etf 목록
def etf_symbol_extract(category_etf):
    etf_url = 'https://www.cnbc.com/' + category_etf + '-etfs/'

    path = 'chromedriver_107.exe'
    driver = webdriver.Chrome(path)  # , chrome_options=chrome_options)
    driver.get(etf_url)

    table_data = []
    i = 0

    time.sleep(5)

    while True:  ## class가 변경될까봐, whlie 문으로 타협
        table_i = '//*[@id="MarketData-MarketsSectionTable-4"]/section/div/div/div/div[1]/div/table/tbody/tr[' + str(
            i + 1) + ']'
        symbol_i = table_i + '/td[1]'
        name_i = table_i + '/td[2]'
        symbol_xpath = driver.find_elements_by_xpath(symbol_i)

        if len(symbol_xpath) == 0:  # 더 이상 etf 종목이 없다면.
            break
        name_xpath = driver.find_elements_by_xpath(name_i)

        table_info = [symbol_xpath[0].text, name_xpath[0].text, category_etf]
        table_data.append(table_info)
        i += 1
    driver.close()
    return table_data

def cnbc_etf_list():
    ## 메인 함수
    index_etf = etf_symbol_extract('index')
    commodity_etf = etf_symbol_extract('commodity')

    table_data = index_etf + commodity_etf

    etf_df = pd.DataFrame(table_data, columns = ['Symbol','Name', 'Class'])
    return etf_df