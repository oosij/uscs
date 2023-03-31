## import load !
import pandas as pd
import schedule
import openpyxl
import re
import requests
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen

import datetime
from datetime import datetime as dtime
from dateutil.parser import parse
from time import strptime

import nltk
from nltk.corpus import stopwords
from nltk.tag import pos_tag
from nltk.tokenize import word_tokenize
from nltk.tokenize import WordPunctTokenizer
from nltk.stem import WordNetLemmatizer
from collections import Counter
from nltk import sent_tokenize

from flair.data import Sentence
from flair.models import SequenceTagger

from gensim.models import Word2Vec
from sklearn.manifold import TSNE
from sklearn.decomposition import PCA
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
from gensim.models import KeyedVectors
import gensim.models as g
import time
import pymysql

from plotly.offline import init_notebook_mode, iplot, plot
import plotly.graph_objs as go

from sklearn.decomposition import IncrementalPCA    # inital reduction
from sklearn.manifold import TSNE                   # final reduction
import numpy as np                                  # array handling
import warnings
warnings.filterwarnings('ignore')
from transformers import logging
logging.set_verbosity_warning()

db = pymysql.connect(host='127.0.0.1',  port = 3306 , user = 'root',
                           password = "1234", db = 'jisoo', charset = 'utf8')


# load tagger
tagger = SequenceTagger.load("flair/ner-english-large")


# docs_list = [doc1, doc2, doc3]

def del_inc_preprocess(list):
    del_inc = ['nasdaq', 'fed', 'technology', 'treasury', 'eral reserve', 's&p', 'stifel economics', 'bloomberg', 'yahoo finance live', 'new york stock exchange', 'nasdaq stock exchange',
               'oil & gas', 'basic materials', 'utilities' , 'labor department', 'eral reserve bank' , 'consumer goods', 'federal reserve', 'worries', 'institute for supply management',
               'bmo capital markets', 'utilities and consumer services', 'basic materials and technology', 'commerce department', 'intelligence', 'wells fargo', 'reuters','global', 'opec+',
               'yahoo finance']
    in_inc = []

    for i in range(len(list)):
        inc = list[i]
        if inc in del_inc :
            continue
        inc = inc.replace('\xa0', ' ')
        in_inc.append(inc)
    return in_inc


## 현재 - 내일 사이의 뉴스 요약
def today_tomorrow(now):
    #today = now.strftime('%Y-%m-%d')
    us_today = now - datetime.timedelta(days=1)  # 미국 시각과 한국 시간 차이
    today = us_today.strftime('%Y-%m-%d')
    now_after = us_today + datetime.timedelta(days=1)
    tomorrow = now_after.strftime('%Y-%m-%d')

    return today, tomorrow


def db_doc_three_extract(db, start_, end_ , category):
    db = pymysql.connect(host='127.0.0.1', port=3306, user='root',
                         password="1234", db='jisoo', charset='utf8')

    #start_ = '2022-11-30'
    #end_ = '2022-12-01'
    #category = 'AMC(UST)'
    # num = 3
    search_sql = """select title, body, w_date, press from US_Stock_Market_News 
                where category = '""" +category+ """' and w_date between date('""" + start_ + """') and date('""" + end_ + """');"""
    cursor = db.cursor()

    cursor.execute(search_sql)
    rows = cursor.fetchall()
    data_list = list(rows)

    docs = []
    press_list = []
    checking = []

    for d in range(len(data_list)):
        doc = data_list[d]
        press = doc[3]
        if press in press_list:
            if press in checking:
                continue
            # checking.append(press)
            # docs.append(doc)
        else:
            press_list.append(press)
            docs.append(doc[1])
            checking.append(press)
        if len(docs) >= 3:
            break

    return docs


## 220822 할것

def docs_inc(doc):

    dword_list = []
    cinc_list = []

    for i in range(len(doc)):
        doc_data = doc[i]
        dner_pos = doc_ner_pos(doc_data)
        cinc = check_inc(dner_pos)
        cinc_list += cinc
        dwords = del_stop_words(dner_pos)
        dword_list.append(dwords)
    return dword_list, cinc_list


## 220824 임시 키워드 추출, 회사/섹터

def keyword_x_y_label(dword_list, cinc_list):
    # word2vec
    model = Word2Vec(sentences=dword_list, vector_size=300, window=5, min_count=1, epochs=30, sg=0)
    num_dimensions = 2  # final num dimensions (2D, 3D, etc)

    # extract the words & their vectors, as numpy arrays
    vectors = np.asarray(model.wv.vectors)
    labels = np.asarray(model.wv.index_to_key)  # fixed-width numpy strings

    # reduce using t-SNE
    tsne = TSNE(n_components=num_dimensions, random_state=0)
    vectors = tsne.fit_transform(vectors)

    x_vals = [v[0] for v in vectors]
    y_vals = [v[1] for v in vectors]

    incs = list(set(cinc_list))

    xs = []
    ys = []
    texts = []

    for i in range(len(labels)):
        if labels[i] in incs:
            xs.append(x_vals[i])
            ys.append(y_vals[i])
            texts.append(labels[i])

    company_xylabel_list = [xs, ys, texts]

    sector_xylabel_list = sectors_extract_only(x_vals, y_vals, labels)

    return company_xylabel_list, sector_xylabel_list


## 이건 섹터만
def sectors_extract_only(x_vals, y_vals, labels):  # 임시 함수
    sectors = ['technology', 'oil', 'energy', 'materials', 'consumer', 'service', 'consumer service', 'gasoline',
               'semiconductors',  'electric', 'vehicle', 'maker', 'food', 'gaming', 'consumer goods'] # megacap


    sector_number = []
    xs = []
    ys = []
    texts = []

    for i in range(len(labels)):
        if labels[i] in sectors:
            sector_number.append([labels[i], i])
            # print(labels[i], i)

    for s in range(len(sector_number)):
        number = sector_number[s][1]
        # label_wd = sector_number[s][0]
        xs.append(x_vals[number])
        ys.append(y_vals[number])
        texts.append(labels[number])

    return [xs, ys, texts]


## 220822 할것

def word2vec_x_y_label(dword_list, cinc_list):
    # word2vec
    model = Word2Vec(sentences=dword_list, vector_size=300, window=5, min_count=1, epochs=30, sg=0)
    num_dimensions = 2  # final num dimensions (2D, 3D, etc)

    # extract the words & their vectors, as numpy arrays
    vectors = np.asarray(model.wv.vectors)
    labels = np.asarray(model.wv.index_to_key)  # fixed-width numpy strings

    # reduce using t-SNE
    tsne = TSNE(n_components=num_dimensions, random_state=0)
    vectors = tsne.fit_transform(vectors)

    x_vals = [v[0] for v in vectors]
    y_vals = [v[1] for v in vectors]

    incs = list(set(cinc_list))

    xs = []
    ys = []
    texts = []

    for i in range(len(labels)):
        if labels[i] in incs:
            xs.append(x_vals[i])
            ys.append(y_vals[i])
            texts.append(labels[i])

    x, y, text = sectors_extract(xs, ys, x_vals, y_vals, texts, labels)

    return x, y, text


def sectors_extract(xs, ys, x_vals, y_vals, texts, labels):  # 임시 함수
    sectors = ['technology', 'oil', 'energy', 'materials', 'consumer', 'service', 'consumer service', 'gasoline',
               'semiconductors', 'electric', 'vehicle', 'maker', 'food', 'gaming'] # megacap

    sector_number = []

    for i in range(len(labels)):
        if labels[i] in sectors:
            sector_number.append([labels[i], i])
            # print(labels[i], i)

    for s in range(len(sector_number)):
        number = sector_number[s][1]
        # label_wd = sector_number[s][0]
        xs.append(x_vals[number])
        ys.append(y_vals[number])
        texts.append(labels[number])

    return xs, ys, texts


def check_inc(ner_pos_check):
    check_inc = []

    for d in range(len(ner_pos_check)):
        wds = ner_pos_check[d][0]
        pos = ner_pos_check[d][1]
        if pos == 'ORG':
            check_inc.append(wds.lower().strip())
    return check_inc


def doc_ner_pos(docs):
    # make example sentence
    sent = docs
    sentence = Sentence(sent)

    # predict NER tags
    tagger.predict(sentence)

    pick_data = []

    for entity in sentence.get_spans('ner'):
        if entity.tag == 'ORG' or entity.tag == 'MISC' or entity.tag == 'PER':
            data = sent[
                   entity.start_position:entity.end_position], entity.tag  # , entity.start_position, entity.end_position
            # print(data )
            pick_data.append(data)

    pick_data = list(set(pick_data))

    change_doc = docs

    for p in range(len(pick_data)):
        words = pick_data[p][0]
        tag = pick_data[p][1]

        word_tag = '#' + words + '/' + tag + '#'
        change_doc = change_doc.replace(words, word_tag)

    shap_token = change_doc.split('#')
    shap_list = []
    sent_list = []
    sent_ner_pos = []

    for s in range(len(shap_token)):
        sent_ = shap_token[s]
        if sent_.find('/ORG') >= 0 or sent_.find('/MISC') >= 0 or sent_.find('/PER') >= 0:
            # shap_list.append(sent_)
            sent_ = sent_.split('/')
            sent_ = sent_[0], sent_[1]
            sent_ner_pos += [sent_]
            continue

        words_token = word_tokenize(sent_)
        pos_token = pos_tag(words_token)
        sent_ner_pos += pos_token
    return sent_ner_pos


def del_stop_words(sent_ner_pos):
    stop_words_list = stopwords.words('english')
    stop_words = set(stopwords.words('english'))

    pos_del = ['.', ',', '(', ':', ')', 'CD']
    word_del = ['%', '$', '/', "'s", "``", "''", "’", '*', '', "'"]
    dword_list = []

    for i in range(len(sent_ner_pos)):
        word = sent_ner_pos[i][0]
        pos = sent_ner_pos[i][1]
        if pos in pos_del:
            continue
        if word in word_del:
            continue
        if len(word) < 2:
            continue
        if word.lower() not in stop_words:
            dword_list.append(word.lower().strip())
    return dword_list


def word_visual(xs, ys, texts):
    trace = go.Scatter(x=xs, y=ys, mode='text', text=texts)
    data = [trace]
    plot_in_notebook = True

    if plot_in_notebook:
        init_notebook_mode(connected=True)
        iplot(data, filename='word-embedding-plot')
    else:
        plot(data, filename='word-embedding-plot.html')


##

## DB 테이블 저장
def keyword_insert(db, company, sector, category, date):
    cur = db.cursor()
    sql = """insert into US_News_Keyword(company, sector, category, date) values (%s, %s, %s, %s)"""
    try:
        cur.execute(sql, (company, sector, category,  date))
        db.commit()
    except pymysql.Error as msg:
        pass

