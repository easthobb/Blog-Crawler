import requests
from bs4 import BeautifulSoup
import time
import datetime
import json
import urllib
import random
#import pg_conn
import sqlalchemy
from sqlalchemy import create_engine  
from sqlalchemy import Column, String, Integer, Date, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker

import pandas as pd
## test
################ requirement ##############
# ### 데이터 수집 요구사항 정리(21.01.26)
# for 
#     국립중앙박물관 
#     고용노동부 블로그
# - 게시물에 대해 1일 단위로 데이터 수집 → 게시물이 3개월이 지나면 월 단위로 수집
# - 블로그 수집 요구사항
# (참고: [https://m.blog.naver.com/jhs941009/220763665432](https://m.blog.naver.com/jhs941009/220763665432))
# 블로그) 채널 id, 일자별 게시물 수, 방문자 수, url
# 블로그 게시물) 게시물 id, 제목, 내용, 해시태그, 공감, 댓글, 작성시간, url
# >> Youtube 수집 처럼 meta와 변화량 데이터를 구분해서 저장.


class BlogCrawler(object):

    def __init__(self,blog_id,crawl_start_date):
        self.crawl_start_date = crawl_start_date ##크롤링의 시작기간, 크롤링 기간 : crawl_start_date ~ 오늘 현재
        self.blog_id = blog_id
        self.paging = 30 # for test each query
        self.base_url = "https://blog.naver.com/"
        
        #사용자 입력 날자 자료형 변환 입력 예상 format YYYY-MM-DD -> datetime form 변환
        temp =list(map(int,self.crawl_start_date.split("-")))
        self.crawl_start_date = datetime.date(temp[0],temp[1],temp[2])
        print(self.crawl_start_date)

    def crawl_blog_info(self,blog_id,crawl_start_date):
        try:
            url = self.base_url + f"NVisitorgp4Ajax.nhn?blogId={blog_id}"
            print("REQUEST : ", url)
            visitor_count_recent = []
            reseponse = requests.get(url)
            lxml = BeautifulSoup(reseponse.text,'lxml')
            for visitor in lxml.select('visitorcnt'):
                visitor_count_recent.append(visitor['cnt'])
            visitor_count_recent.reverse() 
        except Exception as e:
            print("FAIL AT VISITOR GET")
            print(e)
        
        try:
            url = f"https://m.blog.naver.com/rego/BlogInfo.nhn?blogId={blog_id}&directAccess=true"
            print("REQUEST : ", url)
            response = requests.get(url,headers={'User-Agent':'Mozilla/5.0','referer':f'https://m.blog.naver.com/PostList.nhn?blogId={blog_id}&directAccess=True'})
            html = BeautifulSoup(response.text,'html.parser')
            sc = str(html).split('subscriberCount":')
            today_neighbor_count = sc[1].split(",")[0]
            print('NEIGHBOR COUNT:',sc[1].split(",")[0])
        except Exception as e:
            print("FAIL TO NEIGHBOR GET")
            print(e)

        # 블로그의 일자별 개괄 정보 리스트
        period = datetime.date.today() - crawl_start_date
        blog_url = f'http://blog.naver.com/{blog_id}'
        # blog_info = [blog_id, count_date ,buddy_count, visitor_count, post_count]
        blog_info_list = []
        for date in range(0,period.days+1):
            
            if date == 0: # 인덱스가 오늘일 경우
                blog_info = {'blog_id':blog_id,'visitor_count':visitor_count_recent[date],'buddy_count':today_neighbor_count,'count_date':((datetime.date.today()-datetime.timedelta(1*int(date))).isoformat()),'post_count':0,'blog_url':blog_url}
            elif 0<date and date<5: # 인덱스가 최근 5일에 해당할 경우
                blog_info = {'blog_id':blog_id,'visitor_count':visitor_count_recent[date],'buddy_count':0,'count_date':(datetime.date.today()-datetime.timedelta(1*int(date))).isoformat(),'post_count':0,'blog_url':blog_url}
            else: # 최근 5일에 해당하지 않을 경우
                blog_info = {'blog_id':blog_id,'visitor_count':0,'count_date':(datetime.date.today()-datetime.timedelta(1*int(date))).isoformat(),'post_count':0,'blog_url':blog_url,'buddy_count':0}
            blog_info_list.append(blog_info)
        
        for blog_info in blog_info_list:
            print(blog_info)
    
        return blog_info_list
        
    def crawl_post_info(self,blog_id,crawl_start_date):
        
        post_info_list = [] # 개별 포스트의 정보 담김 #post_info_list = [post_id, post_published_date, post_title, post_hashtag, post_url, post _text, post_comment_count,post_sympathy_count]
        page = 1 
        flag = 0
        #현재~입력일 까지의 게시물 ID, title, commentCount,addDate 
        while True:

            try:
                url = self.base_url + f'PostTitleListAsync.nhn?blogId={blog_id}&viewdate=&currentPage={page}&countPerPage={self.paging}'
                print("REQUEST :",url)
                response = requests.get(url)
                response = (response.text).replace("\\","")
                response = json.loads(response)
            except Exception as e:
                print('FAIL AT POST INFO GET')
                print(e)
                
            for i in range(len(response['postList'])):
                post_info = {}
                post = response['postList'][i]
                post_id = post['logNo'] 
                post_published_date = None
                post_title = urllib.parse.unquote(post['title']).replace('+',' ') # url form to str
                post_hashtag = None # BELOW
                post_url = f"https://blog.naver.com/{blog_id}/{post_id}"
                post_text = None # BELOW
                post_comment_count = post['commentCount']
                post_sympathy_count =  None # BELOW
                post_crawl_date = datetime.date.today()
                
                try: # 날짜 - "시간 전' 형태 방지 핸들링
                    receive_date = list(map(int,post['addDate'].replace('.','').split(' ')))
                    post_published_date = datetime.date(receive_date[0],receive_date[1],receive_date[2]).isoformat()
                except:
                    post_published_date = datetime.date.today().isoformat()
                
                try: # 태그 - post 의 태그를 받아오는 부분, 해시태그가 없는 게시물의 경우 빈 문자열 반환
                    tag_url = self.base_url + f'BlogTagListInfo.nhn?blogId={blog_id}&logNoList={post_id}&logType=mylog'
                    print("REQUEST TAG : ",tag_url)
                    tag_response = requests.get(tag_url)
                    tag_response = tag_response.json()
                    post_hashtag = urllib.parse.unquote(tag_response['taglist'][0]['tagName'])
                except:
                    print("POST DOESN'T HAVE TAGS POST ID: ",post_id)
                    post_hashtag = ""

                try: ## 텍스트 - post의 텍스트를 받아오는 부분, 텍스트를 받아올 수 없을 경우 빈 문자열 반환
                    text_url = self.base_url + f"PostView.nhn?blogId={blog_id}&logNo={post_id}&redirect=Dlog&widgetTypeCall=true&directAccess=true"
                    print("REQUEST TEXT: ",text_url)
                    text_response = requests.get(text_url)
                    text_response = BeautifulSoup(text_response.text,'html.parser')
                    post_text = ""
                    for text in text_response.select("div.se-main-container p"):
                        post_text = post_text + str(text.text)
                except:
                    print("CAN'T CRAWL POST TEXT POST ID:",post_id)
                    post_text = ""

                ## 공감 수를 받아오는 쿼리
                try:
                    sympathy_url = f'https://blog.like.naver.com/v1/search/contents?suppress_response_codes=true&q=BLOG[{blog_id}_{post_id}]'
                    print("REQUEST SYMPATHY:",sympathy_url)
                    sympathy_response = requests.get(sympathy_url)
                    sympathy_response = sympathy_response.json()
                    post_sympathy_count = sympathy_response['contents'][0]['reactions'][0]['count']
                except Exception as e:
                    print("FAIL AT POST SYMPATHY GET")
                    print(e)

                if post_sympathy_count == '' or None:
                    post_sympathy_count = 0
                else:
                    post_sympathy_count = int(post_sympathy_count)
                if post_comment_count == '' or None:
                    post_comment_count = 0
                else:
                    post_comment_count = int(post_comment_count)
                
                post_info = {'post_id':post_id,'blog_id':blog_id,'post_published_date':post_published_date,'post_title':post_title,'post_hashtag':post_hashtag,'post_url':post_url,'post_text':post_text,'post_comment_count':post_comment_count,'post_sympathy_count':post_sympathy_count,'post_crawl_date':post_crawl_date}
                print("POST INFO :",post_info['post_id'])
                
                #시간비교를 위해서 date 비교
                temp = list(map(int,post_published_date.split('-')))
                post_pub = datetime.date(temp[0],temp[1],temp[2])
                if (post_pub <= crawl_start_date ): ## ex) 1. 19 < 1.20 (now 2.1)
                    print(post_id,": THIS POST PUB AT",post_pub,'EARLIER THAN CRAWL START DATE. DROP')
                    flag = 1
                    break
                else:
                    post_info_list.append(post_info)
    
                time.sleep(random.randrange(1,3)+random.random())##차단 회피를 위한 sleep
            if flag == 1:
                break
            else:
                page = page + 1
    
        return post_info_list

    def aggregate_post_count(self,blog_info_list,post_info_list):
        
        ## 블로그 인포에 있는 날짜(크롤링 기간)을 카운팅해서 딕셔너리 형태로 넣는 단계
        date_list = dict()
        for blog_date in blog_info_list:
            day = blog_date['count_date'] # 기간 값
            date_list[day] = 0
        
        ## 포스트 인포 리스트에 있는 포스트들을 순회하며서 딕셔너리 값 증가 하는 단계
        for post in post_info_list:
            post_date = post['post_published_date']
            if post_date in date_list:
                date_list[post_date] = date_list[post_date] + 1
            else:
                print(post_date,"IS NOT IN CRAWLING DATE")

        ## 블로그 인포 리스트를 업데이트 하는 단계
        for blog_date in blog_info_list:
            day = blog_date['count_date']
            if day in date_list:
                blog_date['post_count'] = date_list[day]

        return blog_info_list
    
    def db_manage_blog(self,blog_info_list):
        '''
        이 함수에서 해주는 일 : DB 테이블 중 blog_info 연결->업데이트 or 쑤시기
        '''
        db_engine = create_engine(DB_CONNECT_INFO)
        for blog in blog_info_list:
            blog_id = blog['blog_id']
            count_date = blog['count_date']
            blog_url =blog['blog_url']
            buddy_count = blog['buddy_count']
            visitor_count = blog['visitor_count']
            post_count = blog['post_count']

            db_engine.execute(
                f"INSERT INTO blog_count_info (blog_id,count_date,blog_url,buddy_count,visitor_count,post_count) VALUES ('{blog_id}','{count_date}','{blog_url}','{buddy_count}','{visitor_count}','{post_count}') ON CONFLICT (blog_id,count_date) DO UPDATE SET visitor_count = {visitor_count},post_count = {post_count};"
                )
        
        db_engine.dispose()
            
        print('blog_count_info DB UPDATED!')
            
    def db_manage_post(self,post_info_list):
        db_engine = create_engine(DB_CONNECT_INFO)
        
        for post in post_info_list:
        #     post_id = post['post_id']
        #     blog_id = post['blog_id']
        #     post_published_date = post['post_published_date']
        #     post_title = post['post_title']
        #     post_hashtag = post['post_hashtag']
        #     post_url = post['post_url']
        #     post_text = post['post_text']
        #     post_text=post_text.replace("'","").replace('"',"")
        #     post_text = sqlalchemy.text(post_text)
        #     print(post_text)
        #     post_sympathy_count = post['post_sympathy_count']
        #     post_comment_count = post['post_comment_count']
        #     post_crawl_date = post['post_crawl_date']

        #     db_engine.execute(
        #         sqlalchemy.text(f"INSERT INTO post_info (post_id,blog_id,post_published_date,post_title,post_hashtag,post_url,post_text) VALUES ('{post_id}','{blog_id}','{post_published_date}','{post_title}','{post_hashtag}','{post_url}','{post_text}') ON CONFLICT (post_id) DO NOTHING;")
        #     )
            try:
                query = '''
                INSERT INTO post_info  (post_id,blog_id,post_published_date,post_title,post_hashtag,post_url,post_text)
                VALUES ( %(post_id)s, %(blog_id)s,%(post_published_date)s,%(post_title)s,%(post_hashtag)s,%(post_url)s,%(post_text)s)
                ON CONFLICT (post_id) DO NOTHING;
                '''
                params = {
                    'post_id': post['post_id'],
                    'blog_id' : post['blog_id'],
                    'post_published_Date' : post['post_published_date'],
                    'post_title' : post['post_title'],
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
                    'sympathy_count' :post['post_sympathy_count'],
                    'comment_count' : post['post_comment_count'],
                    'crawl_date' : post['post_crawl_date']
                }
                db_engine.execute(query, params)
            except Exception as e:
                print(e)
            #db_engine.execute(
            #    sqlalchemy.text(f"INSERT INTO post_reaction_info (post_id,sympathy_count,comment_count,crawl_date) VALUES ('{post_id}','{post_sympathy_count}','{post_comment_count}','{post_crawl_date}') ON CONFLICT (post_id,crawl_date) DO UPDATE SET sympathy_count={post_sympathy_count}, comment_count={post_comment_count};")
            #)

        
        db_engine.dispose()
    
    def start_crawl(self):

        blog_info_list = self.crawl_blog_info(self.blog_id,self.crawl_start_date)
        post_info_list = self.crawl_post_info(self.blog_id,self.crawl_start_date)
        blog_info_list = self.aggregate_post_count(blog_info_list,post_info_list)
        
        self.db_manage_blog(blog_info_list)
        self.db_manage_post(post_info_list)
        

if __name__=="__main__":
    print("INPUT BLOG ID : ")
    blog_id = input()
    print("INPUT CRAWLING START DATE(YYYY-MM-DD):")
    crawl_start_date = input()
    crawler = BlogCrawler(blog_id,crawl_start_date)
    crawler.start_crawl()
    print("ALL DONE")
    