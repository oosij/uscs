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




### Nasdaq
def nasdaq_news_crawler(db):
    url_list = cross_df_url()
    time.sleep(5)
    # 뉴스 url 리스트 가져오기
    purl_list = []
    # url = "https://www.nasdaq.com/market-activity/commodities/cl%3Anmx/news-headlines"

    final_cnt = len(url_list)
    d = 0
    news_list = []
    while True:
        path = '../chromedriver_107.exe'
        driver = webdriver.Chrome(path)  # , chrome_options=chrome_options)
        url = url_list[d]
        try:
            driver.get(url)
            time.sleep(3)
        except :
            print('timeout!')
            time.sleep(30)
            continue
        cnt = 0

        # 뉴스 게시글
        for i in range(8):
            select_i = '/html/body/div[3]/div/main/div[2]/div[4]/div[3]/div/div[1]/div/div[1]/ul/li[' + str(
                i + 1) + ']/a'
            href_xpath = driver.find_elements_by_xpath(select_i)
            if len(href_xpath) == 0:
                continue
            purl = href_xpath[0].get_attribute('href')
            purl_list.append(purl)
            news_info = nasdaq_news_inlink_parser(purl, headers)
            # inser db info
            #in_url, title, body = news_info[0], news_info[1], news_info[2]
            #w_date, n_date = news_info[3], news_info[4]
            #press, category = news_info[5], 'future'
            #result = stock_news_insert(db, url, title, body, w_date, n_date, press, category, symbol)
            news_list.append(news_list)
            time.sleep(2)
            cnt += 1
        if cnt == 0:
            print('delay error')
            driver.quit()
            continue
        #print(d, url, len(purl_list))
        d += 1
        final_cnt -= 1
        driver.quit()
        if final_cnt == 0:
            break
        time.sleep(5)

    return purl_list, news_info




### Nasdaq
def nasdaq_news_inlink_parser(url, headers):  # v2

    now = dtime.now()
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    #  제목
    title = soup.find("meta", property="og:title")
    title = title['content']

    #  언론사
    press = soup.find('div', {'class': 'syndicate-logo'}).find('img')['alt'].replace('-Logo', '')

    #  날짜
    dt = soup.find('time')

    if dt == None:
        dt = soup.find('p', {'class': 'jupiter22-c-author-byline__timestamp'})
        dt_text = dt.text.replace(',', '')
        dt_split = dt_text.split('—')
        time_split = dt_split[1].split(' ')[1]
        ampm = dt_split[1].split(' ')[2]
        if ampm == 'am':
            time_stamp = time_split
        if ampm == 'pm':
            time_hour = time_split.split(':')[0]
            if time_hour == '12':
                hour = time_hour
            else:
                hour = str(int(time_hour) + 12)
            time_stamp = hour + ':' + time_split.split(':')[1]
        dt_string = dt_split[0].strip() + ' ' + time_stamp

        datetime_type = dtime.strptime(dt_string, '%B %d %Y %H:%M')


    else:
        dt = dt.text
        if dt.find('AM') >= 0:
            dt_string = dt.split('AM')[0]
        if dt.find('PM') >= 0:
            date_fram = dt.split('PM')[0]
            date_split = date_fram.split(' ')
            time_split = date_split[2].split(':')
            if time_split[0] == '12':
                hour = time_split[0]
            else:
                hour = str(int(time_split[0]) + 12)
            time_hm = hour + ':' + time_split[1]
            dt_string = date_split[0] + ' ' + date_split[1] + ' ' + time_hm
        datetime_type = dtime.strptime(dt_string, '%b %d, %Y %H:%M')

    #  본문
    conts = soup.find('div', {'class': 'body__content'})
    body_p = conts.find_all('p')
    body = ''

    for i in range(len(body_p)):
        text = body_p[i].text
        if text.find('please visit') >= 0:
            break
        if i == 0:
            body = text
        else:
            body = body + ' ' + text
    body = body.replace('\n', '')
    news_data = [url, title, body, datetime_type, now, press]

    return news_data


def nasdaq_commdities_table_xpath(table_xpath):
    class_name = table_xpath[0].find_element_by_tag_name('h2').text
    record_extract = table_xpath[0].find_element_by_tag_name('tbody').text

    record_list = record_extract.split('\n')
    sn_list = []

    for r in range(len(record_list)):
        record = record_list[r]
        if record.find('%') >= 0:
            continue
        sn_list.append(record)

    list_chunked = list_chunk(sn_list, 2)

    for c in range(len(list_chunked)):
        sn_data = list_chunked[c]
        sn_data.append(class_name)
    return list_chunked


def table_to_url_list(commdities_df):
    url_list = []
    for c in range(len(commdities_df)):
        commditiy, class_name = commdities_df['Symbol'][c], commdities_df['Class'][c]
        surl = 'https://www.nasdaq.com/market-activity/commodities/' + commditiy + '/news-headlines'
        url_list.append(surl)
    return url_list


def list_chunk(lsn, n):
    return [lsn[i:i + n] for i in range(0, len(lsn), n)]


def nasdaq_commdities_table_info():
    commdities_url = 'https://www.nasdaq.com/market-activity/commodities/'

    path = '../chromedriver_107.exe'
    driver = webdriver.Chrome(path)  # , chrome_options=chrome_options)
    driver.get(commdities_url)

    table_data = []
    i = 0
    while True:  ## class가 변경될까봐, whlie 문으로 타협
        table_i = '/html/body/div[3]/div/main/div[2]/article/div[2]/div/section[' + str(i + 1) + ']/div/div'
        table_xpath = driver.find_elements_by_xpath(table_i)
        if len(table_xpath) == 0:  # 더 이상 class 구분이 없다면.
            break
        table_info = nasdaq_commdities_table_xpath(table_xpath)
        table_data += table_info
        i += 1
    driver.close()
    commdities_df = pd.DataFrame(table_data, columns=['Symbol', 'Name', 'Class'])

    return commdities_df

def cross_df_url():
    df = nasdaq_commdities_table_info()
    url_list = table_to_url_list(df)
    return url_list

