import re
from datetime import datetime as dtime
import datetime
import time
import pymysql
import torch
import transformers
from transformers import BartTokenizer, BartForConditionalGeneration
from transformers import logging
from nltk import sent_tokenize
from summarizer import Summarizer
import warnings
warnings.filterwarnings('ignore')
logging.set_verbosity_warning()

#db = pymysql.connect(host='127.0.0.1',  port = 3306 , user = 'root',
#                           password = "1234", db = 'jisoo', charset = 'utf8')

def bart_summarize(text, num_beams, length_penalty, max_length, min_length, no_repeat_ngram_size):
    model = BartForConditionalGeneration.from_pretrained('facebook/bart-large-cnn')
    tokenizer = BartTokenizer.from_pretrained('facebook/bart-large-cnn')

    torch_device = 'cpu'

    text = text.replace('\n', '')
    text_input_ids = tokenizer.batch_encode_plus([text], return_tensors='pt', max_length=1024)['input_ids'].to(
        torch_device)
    summary_ids = model.generate(text_input_ids, num_beams=int(num_beams), length_penalty=float(length_penalty),
                                 max_length=int(max_length), min_length=int(min_length),
                                 no_repeat_ngram_size=int(no_repeat_ngram_size))
    summary_txt = tokenizer.decode(summary_ids.squeeze(), skip_special_tokens=True)
    return summary_txt


def bart_generate(doc):
    #target = sent_tokenize(doc)
    #target_sent = target[0] + ' ' + target[1] + ' ' + target[2]
    #print(len(target),len(target_sent ))
    target_sent = doc

    ## bart
    num_beams = 4
    length_penalty = 2.0
    max_length =  142
    min_length = 56
    no_repeat_ngram_size = 3
    # 문장별 입력... 별로 좋은 결과는 x
    #bart_1 = bart_summarize(target[0] , num_beams, length_penalty, max_length, min_length, no_repeat_ngram_size)
    #bart_2 = bart_summarize(target[1], num_beams, length_penalty, max_length, min_length, no_repeat_ngram_size)
    #bart_3 = bart_summarize(target[1], num_beams, length_penalty, max_length, min_length, no_repeat_ngram_size)
    #bart = bart_1 + ' ' + bart_2 + ' ' + bart_3

    bart = bart_summarize(target_sent , num_beams, length_penalty, max_length, min_length, no_repeat_ngram_size)

    return bart


## 현재 - 내일 사이의 뉴스 요약
def today_tomorrow(now):
    #today = now.strftime('%Y-%m-%d')
    us_today = now - datetime.timedelta(days=1)  # 미국 시각과 한국 시간 차이
    today = us_today.strftime('%Y-%m-%d')
    now_after = us_today + datetime.timedelta(days=1)
    tomorrow = now_after.strftime('%Y-%m-%d')

    return today, tomorrow

## where press != 'Kiplinger' and
## 시황 전용 : 종목 지정이 필요없음
def search_sql_news(db, start_, end_, category):
    search_sql = """select title, body, w_date,  press  from US_Stock_Market_News
                where category = '""" + category + """' and  w_date between date('"""+start_+"""') and date('"""+end_+"""') and in_col is NULL """
    #search_sql = """select title, body, w_date, press from US_Stock_Market_News
    #                where category = '""" + category + """' and w_date between date('""" + start_ + """') and date('""" + end_ + """') and symbol = '""" + symbol + """' and in_col is NULL """

    cursor = db.cursor()
    cursor.execute(search_sql)
    rows = cursor.fetchall()
    press = list(rows)

    p_list = []
    t_list = []
    text = ''
    title = ''
    press_checking = []


    for i in range(len(press)):
        date = press[i][2]
        pchess = press[i][3]
        if pchess in press_checking:
            continue
        else:
            press_checking.append(pchess)
        p_list.append(press[i][1])
        t_list.append(press[i][0])
        if len(text) == 0:
            text = press[i][1]
            title = press[i][0]
        else:
            text = text + ' ' + press[i][1]
            title = title + ' ' + press[i][0]

    texts = ''
    subject = ''
    for t in range(len(p_list)):
        if t >= 3:
            break
        if t == 0:
            texts = p_list[t]
            subject = t_list[t]
        else:
            texts = texts + ' ' + p_list[t]
            subject = subject + ' ' + t_list[t]
        print(t_list[t])
        print()
    #date_str = date - datetime.timedelta(days = 1)
    #date_str = date_str.strftime('%Y-%m-%d')
    date_str = start_
    #print(date_str)
    return texts, date_str

## stock / etf / future 전용 : 종목 지정 가능 , 추후 필요하면 sub_category 지정도 필요할지도, 아니면 sub_만...    221201
def sef_search_sql_news(db, start_, end_, category, symbol):
    search_sql = """select title, body, w_date, press from US_Stock_Market_News
                    where category = '""" + category + """' and w_date between date('""" + start_ + """') and date('""" + end_ + """') and symbol = '""" + symbol + """'"""

    cursor = db.cursor()
    cursor.execute(search_sql)
    rows = cursor.fetchall()
    press = list(rows)
    print(len(press))

    p_list = []
    t_list = []
    text = ''
    title = ''
    press_checking = []
    sub_class = press[0][-1]

    for i in range(len(press)):
        date = press[i][2]
        pchess = press[i][3]
        if pchess in press_checking:
            continue
        else:
            press_checking.append(pchess)
        p_list.append(press[i][1])
        t_list.append(press[i][0])
        if len(text) == 0:
            text = press[i][1]
            title = press[i][0]
        else:
            text = text + ' ' + press[i][1]
            title = title + ' ' + press[i][0]

    texts = ''
    subject = ''
    for t in range(len(p_list)):
        if t >= 3: # 3개이상이면 브레이크 , 그러나 시황뉴스 제외하곤 조건 x
            break
        if t == 0:
            texts = p_list[t]
            subject = t_list[t]
        else:
            texts = texts + ' ' + p_list[t]
            subject = subject + ' ' + t_list[t]
        print(t_list[t])
        print()
    #date_str = date - datetime.timedelta(days = 1)
    #date_str = date_str.strftime('%Y-%m-%d')
    date_str = start_
    #print(date_str)
    return texts, date_str


def summary_run(db, today, tomorrow, category):
    #now = dtime.now()
    #today, tomorrow = today_tomorrow(now)

    #today = '2022-11-30'  # 차후 특정 시간대 수집한 뉴스 3가지만 가능하도록...
    #tomorrow = '2022-12-01'  # 차후 특정 시간대 수집한 뉴스 3가지만 가능하도록...

    #print(today, tomorrow)

    texts, date = search_sql_news(db,  today, tomorrow, category)
    bert_summary = Summarizer()
    bsum = bert_summary(texts,  ratio=0.2, num_sentences=3)  # ratio=0.2
    bart = bart_generate(bsum)

    #  이 2개의 원문을 db tabel에 저장할수 있도록
    return bsum, bart, date

def symbol_summary_run(db, today, tomorrow, category, symbol):  # 시황 제외한 버전
    #now = dtime.now()
    #today, tomorrow = today_tomorrow(now)

    #today = '2022-11-30'  # 차후 특정 시간대 수집한 뉴스 3가지만 가능하도록...
    #tomorrow = '2022-12-01'  # 차후 특정 시간대 수집한 뉴스 3가지만 가능하도록...

    #print(today, tomorrow)

    texts, date = sef_search_sql_news(db,  today, tomorrow, category, symbol)
    bert_summary = Summarizer()
    bsum = bert_summary(texts,  ratio=0.2, num_sentences=3)  # ratio=0.2
    bart = bart_generate(bsum)

    #  이 2개의 원문을 db tabel에 저장할수 있도록
    return bsum, bart, date


### 문장 분리 강화판 : 대화문 처리
## 지금은 쓰지말고, 추후 모델 쪽으로 개선넘어갈때 사용 : 현재는 큰의미 x , bert 자체 문제이므로
def contents_list(body_split):
    content_list = []

    for b in range(len(body_split)):
        text = body_split[b]
        text = dqmark_split(text)
        if len(text) == 2:
            t1 = text[0].strip()
            t2 = text[1].strip()
            content_list.append(t1)
            content_list.append(t2)
        else:
            t0 = text.strip()
            content_list.append(t0)
    return content_list


def dqmark_split(text):
    dqmark_sent = '[.]” '
    dqmark_pattern = re.compile(dqmark_sent)
    doc = re.search(dqmark_pattern, text)

    if doc != None:
        start_str = doc.start()
        end_str = doc.end()
        body_one = text[:start_str + 2]
        body_two = text[end_str:]
        text = body_one, body_two

    return text


## DB 테이블 저장 : 시황 / 종목-etf / 경제매크로
def summary_m_insert(bert, bart, category, sub_category, date):
    db = pymysql.connect(host='mymastertestdb.cwkjjkurliut.ap-northeast-2.rds.amazonaws.com', port=3306,
                         user='contest_manager',
                         password="thinkmanager6387", db='invest', charset='utf8')

    cur = db.cursor()
    sql = """insert into US_News_Summary(bert_summary, bart_generate, category, sub_category,  date) values (%s, %s, %s, %s, %s)"""
    try:
        cur.execute(sql, (bert, bart, category, sub_category,  date))
        db.commit()
    except pymysql.Error as msg:
        pass

def summary_se_insert(bert, bart, category, sub_category, date, start_date, end_date, symbol):
    db = pymysql.connect(host='mymastertestdb.cwkjjkurliut.ap-northeast-2.rds.amazonaws.com', port=3306,
                         user='contest_manager',
                         password="thinkmanager6387", db='invest', charset='utf8')

    cur = db.cursor()
    sql = """insert into US_News_Summary(bert_summary, bart_generate, category, sub_category,  date, start_date, end_date, symbol) values (%s, %s, %s, %s, %s, %s, %s, %s)"""
    try:
        cur.execute(sql, (bert, bart, category, sub_category, date, start_date, end_date, symbol))
        db.commit()
    except pymysql.Error as msg:
        pass

def summary_f_insert(bert, bart, category, sub_category, date):
    db = pymysql.connect(host='mymastertestdb.cwkjjkurliut.ap-northeast-2.rds.amazonaws.com', port=3306,
                         user='contest_manager',
                         password="thinkmanager6387", db='invest', charset='utf8')

    cur = db.cursor()
    sql = """insert into US_News_Summary(bert_summary, bart_generate, category, sub_category date) values (%s, %s, %s, %s, %s)"""
    try:
        cur.execute(sql, (bert, bart, category,  sub_category, date))
        db.commit()
    except pymysql.Error as msg:
        pass
### 전체적으로 sub_category 추출 하는게 아닌, 차후에 리스트던 df던 sub_category , 종목 , 카테고리 등을 정리해서 인풋으로 저장할수있도록 지정!


'''
doc = """
U.S consumer prices barely rose in November amid declines in the cost of gasoline and used cars, leading to the smallest annual increase in inflation in nearly a year, which could give the Federal Reserve cover to start scaling back the size of its interest rate increases on Wednesday.
The consumer price index increased 0.1% after advancing 0.4% in October, the Labor Department said on Tuesday. Economists polled by Reuters had forecast the CPI gaining 0.3%In the 12 months through November, the CPI climbed 7.1%. That was the smallest advance since December 2021. Annual inflation is slowing in part as last year's big increases drop out of the calculation, while Fed tightening is also dampening demand.
Headline inflation in the U.S. fell by more than expected in November to its lowest level this year, bolstering hopes that the Federal Reserve may not have to raise interest rates much further in order to bring it back firmly under control.
Consumer prices rose 0.1% from October and were up 7.1% from a year earlier, the Bureau of Economic Analysis said, as a drop in energy prices took the sting out of another chunky increase in shelter costs. Excluding volatile fuel and energy components, the 'core' CPI index rose 0.2% on the month and 6.0% on the year, representing a clear slowdown from October's 6.3%.
Headline inflation has now fallen for five months in a row but still remains more than three times the Federal Reserve's target level of 2.0%. Analysts expect the year-on-year rate to ease further as the sharp rises in oil and other energy prices at the back end of last year pass out of the calculations. High-frequency data for house prices and traded goods (especially used cars, prices for which fell 2.9% last month) also suggest that near-term price pressures are also starting to abate, albeit from acutely high levels.
A key gauge of US consumer prices in November posted the smallest monthly advance in more than a year, indicating the worst of inflation has likely passed and validating an anticipated slowing in the pace of Federal Reserve interest-rate hikes.
Excluding food and energy, the consumer price index rose 0.2% in November and was up 6% from a year earlier, according to a Labor Department report Tuesday. Economists see the gauge — known as the core CPI — as a better indicator of underlying inflation than the headline measure. 
The overall CPI increased 0.1% from the prior month and was up 7.1% from a year earlier, as lower energy prices helped offset rising food costs.
"""

bert_summary = Summarizer()
bsum = bert_summary(doc,  ratio=0.2, num_sentences=3)  # ratio=0.2
bart = bart_generate(bsum)

print(bsum)
print(bart)
'''