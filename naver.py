import json

import time

import os

# 크롤링 세팅

from bs4 import BeautifulSoup

from selenium import webdriver

# firebase 세팅 (HDFS 연결 전 데이터 수집 테스트용)

import firebase_admin

from firebase_admin import credentials, firestore

# 정치, 경제, 사회, 생활/문화, 세계, IT/과학, 연예, 스포츠

headline_url = {'pol': 'https://news.naver.com/main/main.naver?mode=LSD&mid=shm&sid1=100',

                'eco': 'https://news.naver.com/main/main.naver?mode=LSD&mid=shm&sid1=101',

                'soc': 'https://news.naver.com/main/main.naver?mode=LSD&mid=shm&sid1=102',

                'cul': 'https://news.naver.com/main/main.naver?mode=LSD&mid=shm&sid1=103',

                'wor': 'https://news.naver.com/main/main.naver?mode=LSD&mid=shm&sid1=104',

                'IT': 'https://news.naver.com/main/main.naver?mode=LSD&mid=shm&sid1=105',

                'ent': 'https://m.entertain.naver.com/ranking',

                'spo': 'https://sports.news.naver.com/ranking/index'}

# 뉴스 카테고리와 유튜브 카테고리 매칭 (0번은 한글 카테고리, 1번부터 유튜브 코드)

category_matching = {'ent': ['연예', 24], 'pol': ['정치', 25], 'eco': ['경제', 25], 'soc': ['사회', 25],

                     'cul': ['생활/문화', 25], 'wor': ['세계', 25], 'IT': ['IT/과학', 28], 'spo': ['스포츠', 17]}

# 데이터 수집 시간

collect_time = time.strftime('%Y%m%d%H%M')

print(collect_time)

crawled_news = []

# webdriver 세팅

options = webdriver.ChromeOptions()

options.add_argument('--headless')        # Head-less 설정

options.add_argument('--no-sandbox')

options.add_argument('--disable-dev-shm-usage')

driver = webdriver.Chrome('chromedriver', options=options)

def save_to_firestore():

    cred = credentials.Certificate("dpwp-project.json")

    firebase_admin.initialize_app(cred)

    db = firestore.client()

    collection = db.collection(u'n_raw_data')

    document_id = f'n_{collect_time}'

    docRef = collection.document(f'{document_id}')

    docRef.set({u'n_raw_data': crawled_news})

def data_to_dict(category, title, url, thumbs, cnt, code):

    data = {

        u'category': category,

        u'title': title,

        u'url': url,

        u'thumbs': thumbs,

        u'cnt': int(cnt),

        u'code': code

    }

    crawled_news.append(data)

# 연예, 스포츠 제외

def normal_news():

    for key in list(headline_url.keys())[:-2]:

        category = category_matching[key][0]

        matching = category_matching[key][1:]

        url = headline_url[key]

        driver.get(url)

        driver.implicitly_wait(5)

        driver.execute_script('window.scrollTo(0,800)')

        time.sleep(3)

        html = driver.page_source

        soup = BeautifulSoup(html, 'html.parser')

        li = soup.select('div._persist > div.section_headline > ul > li')

        try:

            # print(f'{category} news -----------------------')

            cnt = 0

            for idx in range(0, len(li)):

                content = li[idx]

                title_tag = content.select_one('div.sh_text > a')

                title_text = title_tag.text

                title_url = title_tag['href']

                thumbs_tag = content.select_one('div.sh_thumb > div > a > img')

                if thumbs_tag:

                    thumbs_url = thumbs_tag.get('src') or thumbs_tag.get('data-src')

                else:

                    thumbs_url = 'no-image'

                cnt_tag = content.select_one('div.sh_text > div.sh_text_info > a > span.sh_head_more_icon_num')

                cnt_int = int(cnt_tag.string)

                # print(

                #     f'category: {category}\ntitle: {title_text}\nthumbs: {thumbs_url}\nurl: {title_url}\ncnt: {cnt_int}\ncode: {matching}\n')

                data_to_dict(category, title_text, title_url, thumbs_url, cnt_int, matching)

                cnt += 1

        except:

            pass

        print(f'{category} 뉴스: {cnt}개')

# 연예 뉴스 - 랭킹 뉴스(조회수)

def entertainment_news_mobile():

    key = 'ent'

    category = category_matching[key][0]

    matching = category_matching[key][1:]

    url = headline_url[key]

    driver.get(url)

    driver.implicitly_wait(5)

    driver.execute_script('window.scrollTo(0,800)')

    time.sleep(3)

    html = driver.page_source

    soup = BeautifulSoup(html, 'html.parser')

    li = soup.select('ul.rank_lst > li')

    try:

        # print(f'{category} 뉴스 -----------------------')

        cnt = 0

        for idx in range(0, len(li)):

            content = li[idx]

            title_tag = content.select_one('p.tx')

            title_text = title_tag.text

            title_url = content.select_one('a')['href']

            thumbs_tag = content.select_one('img')

            if thumbs_tag:

                thumbs_url = thumbs_tag.get('src') or thumbs_tag.get('data-src')

            else:

                thumbs_url = 'no-image'

            cnt_tag = content.select_one('span.hit')

            cnt_int = int(cnt_tag.text.strip('조회수').replace(',', ''))

            # print(

            #     f'cate: {category}\ntitle: {title_text}\nthumbs: {thumbs_url}\nurl: {title_url}\ncnt: {cnt_int}\ncode: {matching}\n')

            data_to_dict(category, title_text, title_url, thumbs_url, cnt_int, matching)

            cnt += 1

    except:

        pass

    print(f'{category} 뉴스: {cnt}개')

# 스포츠 뉴스

def sports_news():

    key = 'spo'

    category = category_matching[key][0]

    matching = category_matching[key][1:]

    url = headline_url[key]

    driver.get(url)

    driver.implicitly_wait(5)

    driver.execute_script('window.scrollTo(0,800)')

    time.sleep(3)

    html = driver.page_source

    soup = BeautifulSoup(html, 'html.parser')

    li = soup.select('#_newsList > ul > li')

    try:

        # print(f'{category} 뉴스 -----------------------')

        cnt = 0

        for idx in range(0, len(li)):

            content = li[idx]

            title_tag = content.select_one('div.text > a > span')

            title_text = title_tag.text

            title_url = 'https://sports.news.naver.com' + content.select_one('div.text > a')['href']

            thumbs_tag = content.select_one('a > img')

            if thumbs_tag:

                thumbs_url = thumbs_tag.get('src') or thumbs_tag.get('data-src')

            else:

                thumbs_url = 'no-image'

            cnt_tag = content.select_one('div.text > div > span.visit')

            cnt_int = int(cnt_tag.text.strip('조회수').replace(',', ''))

            # print(

            #     f'cate: {category}\ntitle: {title_text}\nthumbs: {thumbs_url}\nurl: {title_url}\ncnt: {cnt_int}\ncode: {matching}\n')

            data_to_dict(category, title_text, title_url, thumbs_url, cnt_int, matching)

            cnt += 1

    except:

        pass

    print(f'{category} 뉴스: {cnt}개')

# 크롤링 코드 실행

def main():

    file_path = './naver_result.json'

    crawled_news.clear()

    normal_news()

    entertainment_news_mobile()

    sports_news()

    # save_to_firestore()

    with open(file_path, 'w', encoding="UTF-8") as outfile:

        json.dump(crawled_news, outfile, indent=3, ensure_ascii=False)

    print(f'\n총 뉴스 {len(crawled_news)}개')

if __name__ == '__main__':

    main()
