import requests
from bs4 import BeautifulSoup
import time
import datetime
import json
import urllib
import random
import sqlalchemy
from sqlalchemy import create_engine

DB_CONNECT_INFO = "postgresql://test:testpwd@localhost:5432/crawler"

class BlogCrawler(object):
    """
    ver 2.0 , 작성자 : 김동호@DMK
    개별 Naver Blog의 ID(blog_id_를 입력받아 현재~입력일(crawl_start_date)까지의
    블로그 정보(블로그 일자별 방문자, 게시물, 이웃 수) 및 포스트 정보(포스트아이디, 업로드일, 포스트제목,포스트 해시태그, 포스트URL, 포스트 내용, 공감 수, 댓글 수) 등을
    postgre SQL DB에 적재하는 클래스입니다.
    """

    def __init__(self, blog_id, crawl_start_date):
        """
        블로그 크롤러의 초기화 init 
        params : blog_id(블로그의 아이디), crawl_start_date(크롤링 시작 시점,YYYY-MM-DD~현재,ISO 8601 format 준수)
        """
        self.crawl_start_date = crawl_start_date  # 크롤링 시작일
        self.blog_id = blog_id  # 크롤링 대상 블로그 ID
        self.paging = 30  # 크롤링 하는 블로그의 POST ID 크롤링 페이징, 선택 범위(5,10,15,20,30)
        self.base_url = "https://blog.naver.com/"  # 네이버 블로그의 base url

        # convert ISO 8601 to datetime object
        temp = list(map(int, self.crawl_start_date.split("-")))
        self.crawl_start_date = datetime.date(temp[0], temp[1], temp[2])
        print(self.crawl_start_date)

    def crawl_blog_info(self, blog_id, crawl_start_date):
        """
        블로그 ID 를 입력 인자로 블로그의 일자별 정보를 받아오는 함수
        (날짜,블로그 url,이웃 수,방문자 수, 일자별 게시물 수)
        이웃 수 : 현재 날짜만 크롤링 / 방문자 수 : 최근 5일만 크롤링 / 일자별 게시물 수 : 전체 범위에 대해서 계산
        """
        try:
            url = self.base_url + f"NVisitorgp4Ajax.nhn?blogId={blog_id}"
            print("REQUEST : ", url)
            visitor_count_recent = []
            reseponse = requests.get(url)
            html = BeautifulSoup(reseponse.text, 'html.parser')
            for visitor in html.select('visitorcnt'):
                visitor_count_recent.append(visitor['cnt'])
            visitor_count_recent.reverse()
        except Exception as e:
            print("FAIL AT VISITOR GET")
            print(e)

        try:
            url = f"https://m.blog.naver.com/rego/BlogInfo.nhn?blogId={blog_id}&directAccess=true"
            print("REQUEST : ", url)
            response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0',
                                                'referer': f'https://m.blog.naver.com/PostList.nhn?blogId={blog_id}&directAccess=True'})  # referer를 필수 인자로 전달 해야함
            html = BeautifulSoup(response.text, 'html.parser')
            sc = str(html).split('subscriberCount":')
            today_neighbor_count = sc[1].split(",")[0]
            print('NEIGHBOR COUNT:', sc[1].split(",")[0])
        except Exception as e:
            print("FAIL TO NEIGHBOR GET")
            print(e)

        period = datetime.date.today() - crawl_start_date
        blog_url = f'http://blog.naver.com/{blog_id}'
        blog_info_list = []
        for date in range(0, period.days+1):

            if date == 0:  # 인덱스가 오늘일 경우
                blog_info = {'blog_id': blog_id, 'visitor_count': visitor_count_recent[date], 'buddy_count': today_neighbor_count,
                            'count_date': ((datetime.date.today()-datetime.timedelta(1*int(date))).isoformat()), 'post_count': 0, 'blog_url': blog_url}
            elif 0 < date and date < 5:  # 인덱스가 최근 5일에 해당할 경우
                blog_info = {'blog_id': blog_id, 'visitor_count': visitor_count_recent[date], 'buddy_count': 0,
                            'count_date': (datetime.date.today()-datetime.timedelta(1*int(date))).isoformat(), 'post_count': 0, 'blog_url': blog_url}
            else:  # 최근 5일에 해당하지 않을 경우
                blog_info = {'blog_id': blog_id, 'visitor_count': 0,
                            'count_date': (datetime.date.today()-datetime.timedelta(1*int(date))).isoformat(), 'post_count': 0, 'blog_url': blog_url, 'buddy_count': 0}
            blog_info_list.append(blog_info)

        for blog_info in blog_info_list:
            print(blog_info)

        return blog_info_list

    def crawl_post_info(self, blog_id, crawl_start_date):
        """
        블로그 ID 를 입력 인자로 블로그의 포스트를 크롤링하는 함수
        (포스트 제목,포스트 해시태그,포스트url,포스트 내용, 포스트 공감 수, 포스트 댓글 수) 크롤링
        params: blog_id(string), crawl_start_date(string,isoformat)
        """
        post_info_list = []
        page = 1
        flag = 0
        while True:
            try:
                url = self.base_url + \
                    f'PostTitleListAsync.nhn?blogId={blog_id}&viewdate=&currentPage={page}&countPerPage={self.paging}'
                print("REQUEST :", url)
                response = requests.get(url)
                response = (response.text).replace("\\", "")
                response = json.loads(response)
            except Exception as e:
                print('FAIL AT POST INFO GET')
                print(e)

            for i in range(len(response['postList'])):
                post_info = {}
                post = response['postList'][i]
                post_id = post['logNo']
                post_published_date = None
                post_title = urllib.parse.unquote(
                    post['title']).replace('+', ' ')
                post_hashtag = None
                post_url = f"https://blog.naver.com/{blog_id}/{post_id}"
                post_text = None
                post_comment_count = post['commentCount']
                post_sympathy_count = None
                post_crawl_date = datetime.date.today()

                try:  # 날짜 - "시간 전' 형태로 응답이 올 경우 핸들링
                    receive_date = list(
                        map(int, post['addDate'].replace('.', '').split(' ')))
                    post_published_date = datetime.date(
                        receive_date[0], receive_date[1], receive_date[2]).isoformat()
                except Exception as e:
                    print(e)
                    post_published_date = datetime.date.today().isoformat()

                try:  # 태그 - post 의 태그를 받아오는 부분, 해시태그가 없는 게시물의 경우 빈 문자열 반환
                    tag_url = self.base_url + \
                        f'BlogTagListInfo.nhn?blogId={blog_id}&logNoList={post_id}&logType=mylog'
                    print("REQUEST TAG : ", tag_url)
                    tag_response = requests.get(tag_url)
                    tag_response = tag_response.json()
                    post_hashtag = urllib.parse.unquote(
                        tag_response['taglist'][0]['tagName'])
                except Exception as e:
                    print(e)
                    post_hashtag = ""

                try:  # 텍스트 - post의 텍스트를 받아오는 부분, 텍스트를 받아올 수 없을 경우 빈 문자열 반환
                    text_url = self.base_url + \
                        f"PostView.nhn?blogId={blog_id}&logNo={post_id}&redirect=Dlog&widgetTypeCall=true&directAccess=true"
                    print("REQUEST TEXT: ", text_url)
                    text_response = requests.get(text_url)
                    text_response = BeautifulSoup(
                        text_response.text, 'html.parser')
                    post_text = ""
                    # blog post 중 텍스트 태그
                    for text in text_response.select("div.se-main-container p"):
                        post_text = post_text + str(text.text)
                except Exception as e:
                    print(e)
                    post_text = ""

                try:  # 공감 수 - post의 공감수를 받아오는 부분, 받아올 수 없을 경우 0 반환
                    sympathy_url = f'https://blog.like.naver.com/v1/search/contents?suppress_response_codes=true&q=BLOG[{blog_id}_{post_id}]'
                    print("REQUEST SYMPATHY:", sympathy_url)
                    sympathy_response = requests.get(sympathy_url)
                    sympathy_response = sympathy_response.json()
                    post_sympathy_count = sympathy_response['contents'][0]['reactions'][0]['count']
                except Exception as e:
                    print(e)

                if post_sympathy_count == '' or None:  # 정수형 처리
                    post_sympathy_count = 0
                else:
                    post_sympathy_count = int(post_sympathy_count)

                if post_comment_count == '' or None:  # 정수형 처리
                    post_comment_count = 0
                else:
                    post_comment_count = int(post_comment_count)

                post_info = {
                    'post_id': post_id, 
                    'blog_id': blog_id,
                    'post_published_date': post_published_date,
                    'post_title': post_title, 
                    'post_hashtag': post_hashtag,
                    'post_url': post_url,
                    'post_text': post_text,
                    'post_comment_count': post_comment_count, 
                    'post_sympathy_count': post_sympathy_count, 
                    'post_crawl_date': post_crawl_date}
                print("POST INFO :", post_info['post_id'])

                temp = list(map(int, post_published_date.split('-')))
                post_pub = datetime.date(temp[0], temp[1], temp[2]) # 시간비교를 위해서 date 비교
                if (post_pub <= crawl_start_date):  # ex) 1. 19 < 1.20 (now 2.1)
                    print(post_id, ": THIS POST PUB AT", post_pub, 'EARLIER THAN CRAWL START DATE. DROP')
                    flag = 1
                    break
                else:
                    post_info_list.append(post_info)

                time.sleep(random.randrange(1, 3) +random.random())  # 차단 회피를 위한 sleep
            if flag == 1:
                break
            else:
                page = page + 1

        return post_info_list

    def aggregate_post_count(self, blog_info_list, post_info_list):
        """
        일자별 게시물 수 계산을 위한 함수
        post_info_list 에서 날짜별 집계 후 blog_info_list의 해당날짜 저장
        params: blog_info_list(list of dict),post_info_list(list of dict)
        """
        date_list = dict() 
        for blog_date in blog_info_list:
            day = blog_date['count_date']  # 기간 값
            date_list[day] = 0

        for post in post_info_list:
            post_date = post['post_published_date']
            if post_date in date_list:
                date_list[post_date] = date_list[post_date] + 1
            else:
                print(post_date, "IS NOT IN CRAWLING DATE")

        for blog_date in blog_info_list:
            day = blog_date['count_date']
            if day in date_list:
                blog_date['post_count'] = date_list[day]

        return blog_info_list

    def db_manage_blog(self, blog_info_list):
        '''
        blog_info_list의 날짜별 블로그 정보를 DB에 저장해주는 함수
        params: blog_info_list(list of dict)
        '''
        db_engine = create_engine(DB_CONNECT_INFO)

        for blog in blog_info_list:
            try:
                query = '''
                    INSERT INTO blog_count_info (blog_id,count_date,blog_url,buddy_count,visitor_count,post_count)
                    VALUES (%(blog_id)s,%(count_date)s,%(blog_url)s,%(buddy_count)s,%(visitor_count)s,%(post_count)s)
                    ON CONFLICT (blog_id,count_date) DO UPDATE SET visitor_count = %(visitor_count)s, post_count = %(post_count)s;
                    '''
                params = {
                    'blog_id': blog['blog_id'],
                    'count_date': blog['count_date'],
                    'blog_url': blog['blog_url'],
                    'buddy_count': blog['buddy_count'],
                    'visitor_count': blog['visitor_count'],
                    'post_count': blog['post_count']
                }
                db_engine.execute(query, params)

            except Exception as e:
                print(e)
        db_engine.dispose()

        print('blog_count_info DB UPDATED!')

    def db_manage_post(self, post_info_list):
        '''
        post_info_list의 포스트 정보를 DB에 저장해주는 함수
        params: post_info_list(list of dict)
        '''
        db_engine = create_engine(DB_CONNECT_INFO)
        for post in post_info_list:
            try:
                query = '''
                INSERT INTO post_info  (post_id,blog_id,post_published_date,post_title,post_hashtag,post_url,post_text)
                VALUES ( %(post_id)s, %(blog_id)s,%(post_published_date)s,%(post_title)s,%(post_hashtag)s,%(post_url)s,%(post_text)s)
                ON CONFLICT (post_id) DO NOTHING;
                '''
                params = {
                    'post_id': post['post_id'],
                    'blog_id': post['blog_id'],
                    'post_published_date': post['post_published_date'],
                    'post_title': post['post_title'],
                    'post_hashtag': post['post_hashtag'],
                    'post_url': post['post_url'],
                    'post_text': post['post_text']
                }
                db_engine.execute(query, params)
            except Exception as e:
                print(e)

            try:
                query = '''
                INSERT INTO post_reaction_info (post_id,sympathy_count,comment_count,crawl_date)
                VALUES ( %(post_id)s, %(sympathy_count)s, %(comment_count)s, %(crawl_date)s)
                ON CONFLICT (post_id,crawl_date) DO UPDATE SET sympathy_count=%(sympathy_count)s, comment_count=%(comment_count)s;
                '''
                params = {
                    'post_id': post['post_id'],
                    'sympathy_count': post['post_sympathy_count'],
                    'comment_count': post['post_comment_count'],
                    'crawl_date': post['post_crawl_date']
                }
                db_engine.execute(query, params)

            except Exception as e:
                print(e)

        db_engine.dispose()

    def start_crawl(self):

        blog_info_list = self.crawl_blog_info(self.blog_id, self.crawl_start_date)
        post_info_list = self.crawl_post_info(self.blog_id, self.crawl_start_date)
        blog_info_list = self.aggregate_post_count(blog_info_list, post_info_list)

        self.db_manage_blog(blog_info_list)
        self.db_manage_post(post_info_list)


if __name__ == "__main__":

    print("INPUT BLOG ID : ")
    blog_id = input()
    print("INPUT CRAWLING START DATE(YYYY-MM-DD):")
    crawl_start_date = input()
    crawler = BlogCrawler(blog_id, crawl_start_date)
    crawler.start_crawl()
    print("ALL DONE")
