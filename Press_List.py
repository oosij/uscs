## import load !

#from webdriver_manager.chrome import ChromeDriverManager

from code_store.Google_News_Search import *
from US_News_func import *

## 아웃링크 파서 : 221116 기준 10개 (야후 파이낸스 제외)

# Nasdaq
def nasdaq_news_parser(url, headers, press):
    now = dtime.now()
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    #  제목
    title = soup.find("meta", property="og:title")
    title = title['content']

    #  날짜
    dt = soup.find('p', {'class': 'jupiter22-c-author-byline__timestamp'})
    dt_text = dt.text.replace(',', '')
    dt_split = dt_text.split('—')
    time_split = dt_split[1].split(' ')[1]
    ampm = dt_split[1].split(' ')[2]
    if ampm == 'am':
        time_stamp = time_split
    if ampm == 'pm':
        time_hour = time_split.split(':')[0]
        hour = str(int(time_hour) + 12)
        time_stamp = hour + ':' + time_split.split(':')[1]

    dt_string = dt_split[0].strip() + ' ' + time_stamp
    datetime_type = dtime.strptime(dt_string, '%B %d %Y %H:%M')

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


# ETF Trends

def etf_trands_news_parser(url, headers, press):
    now = dtime.now()
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    #  제목
    title = soup.find("meta", property="og:title")
    title = title['content']

    #  날짜
    dt = soup.find('meta', property='article:published_time')
    dt = dt['content']
    dt_split = dt.split('T')
    dt_ymd = dt_split[0]
    dt_time = dt_split[1].split('+')[0]
    # dt = newsdate_preprocessing(dt)
    dt_string = dt_ymd + ' ' + dt_time
    datetime_type = dtime.strptime(dt_string, '%Y-%m-%d %H:%M:%S')

    #  본문
    conts = soup.find('div', {'class': 'post-content post-dynamic description'})
    body_p = conts.find_all('p')
    body = ''

    for i in range(len(body_p)):
        text = body_p[i].text
        if text.find('For more news') >= 0:
            break
        if i == 0:
            body = text
        else:
            body = body + ' ' + text

    news_data = [url, title, body, datetime_type, now, press]

    return news_data


# ETF Daily News

def etf_daily_news_parser(url, headers, press):
    now = dtime.now()
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    #  제목
    title = soup.find("meta", property="og:title")
    title = title['content']

    #  날짜
    dt = soup.find('meta', property='article:published_time')
    dt = dt['content']
    dt_split = dt.split('T')
    dt_ymd = dt_split[0]
    dt_time = dt_split[1].split('+')[0]
    dt_string = dt_ymd + ' ' + dt_time
    datetime_type = dtime.strptime(dt_string, '%Y-%m-%d %H:%M:%S')

    #  본문
    conts = soup.find('div', {'class': 'entry'})
    body_p = conts.find_all('p')
    body = ''

    for i in range(len(body_p)):
        text = body_p[i].text
        if text.find('FREE') >= 0:
            break
        if i == 0:
            body = text
        else:
            body = body + ' ' + text
    body = body.replace('\n', '')

    news_data = [url, title, body, datetime_type, now, press]

    return news_data


#  The Motley Fool

def the_montley_fool_news_parser(url, headers, press):
    now = dtime.now()
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    #  제목
    title = soup.find("meta", property="og:title")
    title = title['content']

    #  날짜
    dt = soup.find('meta', {'name': 'date'})
    dt = dt['content']
    dt_split = dt.split('T')
    dt_ymd = dt_split[0]
    dt_time = dt_split[1].split('+')[0].replace('Z', '')
    dt_string = dt_ymd + ' ' + dt_time
    datetime_type = dtime.strptime(dt_string, '%Y-%m-%d %H:%M:%S')

    #  본문
    conts = soup.find('div', {'class': 'md:w-3/4 md:pr-80'})

    body_p = conts.find_all('p')
    body = ''

    for i in range(len(body_p)):
        text = body_p[i].text
        if text.find('no position') >= 0:
            break
        if i == 0:
            body = text
        else:
            body = body + ' ' + text

    news_data = [url, title, body, datetime_type, now, press]
    return news_data


#  Benzinga
def benzinga_news_parser(url, headers, press):
    now = dtime.now()
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    #  제목
    title = soup.find("meta", property="og:title")
    title = title['content']

    #  날짜
    dt = soup.find('span', {'class': 'date'})
    dt_text = dt.text
    if dt_text.find('PM') >= 1:
        dt_text = dt_text.replace('PM', '')
        dt_string = dt_text.strip()
        time_split = dt_string.split(' ')
        time_num = time_split[-1].split(':')
        hour = str(int(time_num[0]) + 12)
        minute = time_num[1]
        dt_string = time_split[0] + ' ' + time_split[1] + ' ' + time_split[2] + ' ' + hour + ':' + minute

    if dt_text.find('AM') >= 1:
        dt_text = dt_text.replace('AM', '')
        dt_string = dt_text.strip()

    datetime_type = dtime.strptime(dt_string, '%B %d, %Y %H:%M')

    #  본문
    conts = soup.find('div', {'class': 'ArticleBody__ArticleBodyDiv-sc-l6jpud-0 fZXicg article-content-body-only'})
    body_p = conts.find_all('p')
    body = ''

    for i in range(len(body_p)):
        text = body_p[i].text
        if text.find('Read Next') >= 0:
            break
        if i == 0:
            body = text
        else:
            body = body + ' ' + text
    body = body.replace('\xa0', '')

    news_data = [url, title, body, datetime_type, now, press]

    return news_data


#  Equities News
def equities_news_parser(url, headers, press):
    now = dtime.now()
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    #  제목
    title = soup.find("meta", property="og:title")
    title = title['content']

    #  날짜
    dt = soup.find('meta', {'name': 'article.published'})
    dt = dt['content']
    datetime_type = newsdate_preprocessing(dt)

    #  본문
    conts = soup.find_all('p')
    del_a = soup.find('p', {'class': 'text-center margin-top-20'}).decompose()
    del_b = soup.find('p', {'id': 'signupSuccessMessageText'}).decompose()
    del_c = soup.find('p', {'id': 'modsignupErrorMessageText'}).decompose()
    del_d = soup.find('p', {'class': 'text small'}).decompose()
    conts = soup.find_all('p')

    body_list = []
    body = ''

    for b in range(len(conts)):
        p_text = conts[b].text.strip()
        if p_text.find('DISCLOSURE') >= 0:
            break
        if len(p_text) == 0:
            continue
        body_list.append(p_text)

        if b == 0:
            body = p_text
        else:
            body = body + ' ' + p_text
    body = body.replace('\xa0', ' ')

    news_data = [url, title, body, datetime_type, now, press]

    return news_data


#  MoneyShow   - > 기사 내용이 너무 적음... 일단 ...

def moneyshow_news_parser(url, headers, press):
    now = dtime.now()
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    #  제목
    title = soup.find("meta", property="og:title")
    title = title['content']

    #  날짜
    dt = soup.find('div', {'class': 'my-2 text-muted font-roboto font-14'})
    dt = dt.find('span')
    dt_text = dt.text
    date_split_list = dt_text.split(' ')
    ymd_s, time_s, ampm = date_split_list[0], date_split_list[1], date_split_list[2]
    ymd_three = ymd_s.split('/')
    dt_ymd = ymd_three[2] + '-' + ymd_three[0] + '-' + ymd_three[1]
    if ampm == 'am':
        dt_time = time_s
    if ampm == 'pm':
        ts_list = time_s.split(':')
        hour = str(int(ts_list[0]) + 12)
        dt_time = hour + ':' + ts_list[1]
    dt_string = dt_ymd + ' ' + dt_time
    datetime_type = dtime.strptime(dt_string, '%Y-%m-%d %H:%M')

    #  본문
    conts = soup.find('div', {'class': 'mt-4 pt-4 font-roboto article-body'})
    body_p = conts.find_all('p')
    body = ''

    for i in range(len(body_p)):
        text = body_p[i].text
        if text.find('To learn more') >= 0:
            break
        if i == 0:
            body = text
        else:
            body = body + ' ' + text
    body = body.replace('\xa0', '')
    news_data = [url, title, body, datetime_type, now, press]

    return news_data


#  Fxstreet

def fxstreet_news_parser(url, headers, press):
    now = dtime.now()
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    #  제목
    title = soup.find("meta", property="og:title")
    title = title['content']

    #  날짜
    dt = soup.find('time')
    dt = dt['datetime']
    dt_split = dt.split('T')
    dt_ymd = dt_split[0]
    dt_time = dt_split[1].split('+')[0].replace('Z', '')
    dt_string = dt_ymd + ' ' + dt_time
    datetime_type = dtime.strptime(dt_string, '%Y-%m-%d %H:%M:%S')

    #  본문
    conts = soup.find('div', {'id': 'fxs_article_content'})
    body_p = conts.find_all('p')
    body = ''

    for i in range(len(body_p)):
        text = body_p[i].text
        if text.find('WSJ') >= 0:
            break
        if i == 0:
            body = text
        else:
            body = body + ' ' + text
    body = body.replace('\xa0', ' ')
    news_data = [url, title, body, datetime_type, now, press]

    return news_data


#  Entrepreneur

def entrepreneur_news_parser(url, headers, press):
    now = dtime.now()
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    #  제목
    title = soup.find("meta", property="og:title")
    title = title['content']

    #  날짜
    dt = soup.find('meta', property='article:published_time')
    dt = dt['content']
    dt = dt.replace('T', ' ')
    dt = dt.replace('+', '')
    dt = dt.replace('00:00', '')
    datetime_type = dtime.strptime(dt, '%Y-%m-%d %H:%M:%S')

    #  본문
    conts = soup.find('div', {'class': 'max-w-3xl prose prose-blue text-lg leading-8 mb-8'})
    body_p = conts.find_all('p')
    body = ''

    for i in range(len(body_p)):
        text = body_p[i].text
        if text.find('Learn more') >= 0:
            break
        if i == 0:
            body = text
        else:
            body = body + ' ' + text
    body = body.replace('\xa0', '')
    body = body.replace('\n\n', '')
    news_data = [url, title, body, datetime_type, now, press]

    return news_data


#  Compound Advisors

def compound_advisors_news_parser(url, headers, press):
    now = dtime.now()
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    #  제목
    title = soup.find("meta", property="og:title")
    title = title['content']

    #  날짜
    dt = soup.find('meta', property='article:published_time')
    dt = dt['content']
    dt = dt.replace('T', ' ')
    dt = dt.replace('+', '')
    dt = dt.replace('00:00', '')
    dt_split = dt.split(' ')
    times = dt_split[1].split('-')[0]
    dt = dt_split[0] + ' ' + times
    datetime_type = dtime.strptime(dt, '%Y-%m-%d %H:%M:%S')

    #  본문
    conts = soup.find('div', {'class': 'single-content content'})
    body_p = conts.find_all('p')
    body = ''

    for i in range(len(body_p)):
        text = body_p[i].text
        if text.find('click here') >= 0:
            continue
        if i == 0:
            body = text
        else:
            body = body + ' ' + text
    body = body.replace('\xa0', '')
    body = body.replace('\n\n', '')

    news_data = [url, title, body, datetime_type, now, press]

    return news_data
