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

from Info_extract_func import db_doc_three_extract, docs_inc, keyword_x_y_label, keyword_insert, del_inc_preprocess, today_tomorrow
import warnings
warnings.filterwarnings('ignore')
from transformers import logging
logging.set_verbosity_warning()

def info_main(category):
    db = pymysql.connect(host='127.0.0.1', port=3306, user='root',
                         password="1234", db='jisoo', charset='utf8')

    # load tagger
    #tagger = SequenceTagger.load("flair/ner-english-large")

    now = dtime.now()
    today, tomorrow = today_tomorrow(now)
    print(today, tomorrow )

    #date_in =  '2022-08-24' # 현재 날짜 자동으로 할수 있도록 !
    docs = db_doc_three_extract(db, today, tomorrow,category)

    dword_list, cinc_list = docs_inc(docs)
    company_val, sector_val =  keyword_x_y_label(dword_list, cinc_list)

    company_inc = del_inc_preprocess(company_val[2])

    ## 데이터 저장 -> DB
    print('stock company =', company_inc)
    print('issue sector =', sector_val[2])
    print('news time class =', category )
    print('=== ' + category + ' (미국) 키워드 추출 완료', today, '===')
    #save = keyword_insert(db, company_val[2], sector_val[2], category, today)

    #return save


## 매일 자동 스케쥴러

#category = 'AMC(UST)'
category = 'market_news'

print('===== 미국 뉴스 키워드 자동화 中 =====')
#info_main(category)

# 장마감 시간 기준
end_time = "07:40"

# 월요일
schedule.every().tuesday.at(end_time).do(info_main, category) # 화요일 -> 미국시각 월요일
# 화요일
schedule.every().wednesday.at(end_time).do(info_main, category) # 수요일 -> 미국시각 화요일
# 수요일
schedule.every().thursday.at(end_time).do(info_main, category) # 목요일 -> 미국시각 수요일
# 목요일
schedule.every().friday.at(end_time).do(info_main, category) # 금요일 -> 미국시각 수요일
# 금요일
schedule.every().saturday.at(end_time).do(info_main, category) # 토요일 -> 미국시각 금요일


while True:
    schedule.run_pending()
    time.sleep(1)
