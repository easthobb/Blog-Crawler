import requests
from bs4 import BeautifulSoup
import time
import datetime
import json
import urllib
import random
import pg_conn
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
        self.crawl_start_date = crawl_start_date
        self.blog_url = ""
        self.blog_id = blog_id
        self.blog_info_list = []
        self.post_id_list = []
        self.post_info_list = []
        self.paging = 30 # for test each query
        self.BASE_URL = "https://blog.naver.com/"
        
        #사용자 입력 날자 자료형 변환 입력 예상 format YYYY-MM-DD -> datetime form 변환
        temp =list(map(int,self.crawl_start_date.split("-")))
        self.crawl_start_date = datetime.date(temp[0],temp[1],temp[2])
        print(self.crawl_start_date)

    # https://m.blog.naver.com/rego/BlogInfo.nhn?blogId=100museum
    # 블로그의 일자별 개괄 정보를 반환하는 메소드
    # input output
    def crawl_blog_info(self,blog_id,crawl_start_date):
        
        # 방문자 수 요청과 응답 처리
        URL = self.BASE_URL + f"NVisitorgp4Ajax.nhn?blogId={blog_id}"
        print("REQUEST : ", URL)
        visitor_count_recent = []
        reseponse = requests.get(URL)
        lxml = BeautifulSoup(reseponse.text,'lxml')
        for visitor in lxml.select('visitorcnt'):
            visitor_count_recent.append(visitor['cnt'])
        visitor_count_recent.reverse() # 가장 과거부터 오기에 역순

        # 이웃 수 요청 쿼리
        URL = f"https://m.blog.naver.com/rego/BlogInfo.nhn?blogId={blog_id}&directAccess=true"
        print("REQUEST : ", URL)
        response = requests.get(URL,headers={'User-Agent':'Mozilla/5.0','referer':f'https://m.blog.naver.com/PostList.nhn?blogId={blog_id}&directAccess=True'})
        html = BeautifulSoup(response.text,'html.parser')
        sc = str(html).split('subscriberCount":')
        today_neighbor_count = sc[1].split(",")[0]
        print('NEIGHBOR COUNT:',sc[1].split(",")[0])

        # 블로그의 일자별 개괄 정보 리스트
        period = datetime.date.today() - crawl_start_date
        blog_url = f'http://blog.naver.com/{blog_id}'
        # blog_info = [blog_id, count_date ,buddy_count, visitor_count, post_count]
        blog_info_list = []
        for date in range(0,period.days+1):
            
            if date == 0: # 인덱스가 오늘일 경우
                blog_info = [blog_id,visitor_count_recent[date],today_neighbor_count,(datetime.date.today()-datetime.timedelta(1*int(date))).isoformat(),None,blog_url]
            elif 0<date and date<5: # 인덱스가 최근 5일에 해당할 경우
                blog_info = [blog_id,visitor_count_recent[date],None,(datetime.date.today()-datetime.timedelta(1*int(date))).isoformat(),None,blog_url]
            else: # 최근 5일에 해당하지 않을 경우
                blog_info = [blog_id,None,None,(datetime.date.today()-datetime.timedelta(1*int(date))).isoformat(),None,blog_url]
            blog_info_list.append(blog_info)
        
        for blog_info in blog_info_list:
            print(blog_info)
    
        return blog_info_list
        
    def crawl_post_info(self,blog_id,crawl_start_date):
        
        post_info_list = [] # 개별 포스트의 정보 담김 #post_info_list = [post_id, post_published_date, post_title, post_hashtag, post_url, post _text, post_comment_count,post_sympathy_count]
        #post_id_list = [] #블로그 포스트의 id를 저장하는 리스트
        page = 1 
        flag = 0
        #현재~입력일 까지의 게시물 ID, title, commentCount,addDate 
        while True:
            URL = self.BASE_URL + f'PostTitleListAsync.nhn?blogId={blog_id}&viewdate=&currentPage={page}&countPerPage={self.paging}'
            print("REQUEST :",URL)
            response = requests.get(URL)
            response = (response.text).replace("\\","")
            response = json.loads(response)
            for i in range(len(response['postList'])):
                post_info =[]
                post = response['postList'][i]
                post_id = post['logNo'] # post id
                post_published_date = None#post['addDate'].replace('.','').replace(' ','-') ## format YYYY-MM-DD
                post_title = urllib.parse.unquote(post['title']).replace('+',' ') # url form to str
                post_hashtag = None # BELOW
                post_url = f"https://blog.naver.com/{blog_id}/{post_id}"
                post_text = None # BELOW
                post_comment_count = post['commentCount']
                post_sympathy_count =  None # BELOW
                ## 날짜 ISO 형태 변환 - 오늘자 게시물 "시간" 표현 형태 방지
                try:
                    receive_date = list(map(int,post['addDate'].replace('.','').split(' ')))
                    post_published_date = datetime.date(receive_date[0],receive_date[1],receive_date[2]).isoformat()
                except:
                    post_published_date = datetime.date.today().isoformat()
                
                try:
                    ## 태그리스트를 받아오는 쿼리
                    tag_URL = self.BASE_URL + f'BlogTagListInfo.nhn?blogId={blog_id}&logNoList={post_id}&logType=mylog'
                    print("REQUEST TAG : ",tag_URL)
                    tag_response = requests.get(tag_URL)
                    tag_response = tag_response.json()
                    post_hashtag = urllib.parse.unquote(tag_response['taglist'][0]['tagName'])
                except:
                    print("POST DOESN'T HAVE TAGS POST ID: ",post_id)
                    post_hashtag = None
                try:
                    ## 텍스트를 받아오는 쿼리
                    text_URL = self.BASE_URL + f"PostView.nhn?blogId={blog_id}&logNo={post_id}&redirect=Dlog&widgetTypeCall=true&directAccess=true"
                    print("REQUEST TEXT: ",text_URL)
                    text_response = requests.get(text_URL)
                    text_response = BeautifulSoup(text_response.text,'html.parser')
                    post_text = ""
                    for text in text_response.select("div.se-main-container p"):
                        post_text = post_text + str(text.text)
                except:
                    print("CAN'T CRAWL POST TEXT POST ID:",post_id)
                    post_text = ""

                ## 공감 수를 받아오는 쿼리
                sympathy_URL = f'https://blog.like.naver.com/v1/search/contents?suppress_response_codes=true&q=BLOG[{blog_id}_{post_id}]'
                print("REQUEST SYMPATHY:",sympathy_URL)
                sympathy_response = requests.get(sympathy_URL)
                sympathy_response = sympathy_response.json()
                post_sympathy_count = sympathy_response['contents'][0]['reactions'][0]['count']
                if post_sympathy_count == '' or None:
                    post_sympathy_count = 0
                else:
                    post_sympathy_count = int(post_sympathy_count)
                if post_comment_count == '' or None:
                    post_comment_count = 0
                else:
                    post_comment_count = int(post_comment_count)                

                post_info = [post_id,post_published_date,post_title,post_hashtag,post_url,post_text,post_comment_count,post_sympathy_count]
                print("POST INFO :",post_info[0])
                

                #시간비교를 위해서 date 비교
                temp = list(map(int,post_published_date.split('-')))
                post_pub = datetime.date(temp[0],temp[1],temp[2])
                if (post_pub <= crawl_start_date ): ## ex) 1. 19 < 1.20 (now 2.1)
                    print(post_id,": THIS POST PUB AT",post_pub,'EARLIER THAN CRAWL START DATE. DROP')
                    flag = 1
                    break
                else:
                    post_info_list.append(post_info)
                ##차단 회피를 위한 sleep
                time.sleep(random.randrange(1,3)+random.random())
            if flag == 1:
                break
            else:
                page = page + 1
            
        # ##debug
        # for post in post_info_list:
        #     print(post)
        return post_info_list

    def aggregate_post_count(self,blog_info_list,post_info_list):
        
        ## 블로그 인포에 있는 날짜(크롤링 기간)을 카운팅해서 딕셔너리 형태로 넣는 단계
        date_list = dict()
        for blog_date in blog_info_list:
            day = blog_date[3] # 기간 값
            date_list[day] = 0
        
        ## 포스트 인포 리스트에 있는 포스트들을 순회하며서 딕셔너리 값 증가 하는 단계
        for post in post_info_list:
            post_date = post[1]
            if post_date in date_list:
                date_list[post_date] = date_list[post_date] + 1
            else:
                print(post_date,"IS NOT IN CRAWLING DATE")

        ## 블로그 인포 리스트를 업데이트 하는 단계
        for blog_date in blog_info_list:
            day = blog_date[3]
            if day in date_list:
                blog_date[4] = date_list[day]

        return blog_info_list
    
    
    def start_crawl(self):

        try: 
            ####################################DB 조회 및 기등록된 블로그인지 확인.
            blog_id_list= [] # 블로그의 아이디만 저장 
            db_blog = pg_conn.session.query(pg_conn.Blog) ## 받아온 블로그 정보
            for blog in db_blog:
                blog_id_list.append(blog.blog_id)
                #blog_date_list.append([blog.blog_id,blog.count_date])
        except:
            print("DB BLOG INFO CONNECT FAIL")
        ####### 등록된 블로그 : 
        if self.blog_id in blog_id_list: 
            
            print(list(set(blog_id_list)), "ARE IN DB")
            ####### 크롤링
            try:
                blog_info = self.crawl_blog_info(self.blog_id,self.crawl_start_date)
                post_info = self.crawl_post_info(self.blog_id,self.crawl_start_date)
                blog_info = self.aggregate_post_count(blog_info,post_info)
            except:
                print("CRAWLER GET FAIL(ALREADY REGISTERD BLOG)")
            
            ################# 블로그 테이블 업데이트 시퀀스
            try:
                fetch = pg_conn.session.query(pg_conn.Blog).filter(pg_conn.Blog.blog_id == self.blog_id)
                already_crawl_date_list = []
                for row in fetch:
                    already_crawl_date_list.append(row.count_date.isoformat()) ## iso 형식으로 날짜 받아옴
                
                for blog in blog_info: # 개별 날짜에 대해서 반복 검증
                    if blog[3] in already_crawl_date_list:
                        print('ALREADY CRAWLED ALL POST OF THE DAY :',blog[3])#이미 크롤링 한 날짜일 경우 방문자 수만 업데이트
                        db_update_register_blog = pg_conn.session.query(pg_conn.Blog).filter(
                            (pg_conn.Blog.blog_id==self.blog_id) and (pg_conn.Blog.count_date == blog[3])).first()
                        if (blog[1] != None) and (db_update_register_blog.visitor_count != None):
                            print(db_update_register_blog.visitor_count)
                            if db_update_register_blog.visitor_count < int(blog[1]):
                                db_update_register_blog.visitor_count = int(blog[1]) ## 당일 방문자 수가 늘었을 경우만 반영
                            else:
                                pass
                        else :
                            pass#print(blog[3],"DAY's VISITOR INFO ARE NOT AVAILABLE")
                    else: #크롤링 하지 않은 새로운 날짜일 경우
                        print('NEW DATE CRAWL :',blog[3])
                        db_new_register_blog = pg_conn.Blog(
                            blog_id=blog[0],
                            count_date=blog[3],
                            buddy_count=blog[2],
                            visitor_count=blog[1],
                            post_count=blog[4],
                            blog_url=blog[5])
                        pg_conn.session.add(db_new_register_blog)
                        
                    pg_conn.session.commit()

                print(already_crawl_date_list)
            except:
                print("DB BLOG INFO TABLE UPDATE SEQUENCE FAIL")
            ################# 포스트 테이블 업데이트 시퀀스
            try:
                fetch = pg_conn.session.query(pg_conn.Post).filter(
                    pg_conn.Post.blog_id == self.blog_id and pg_conn.Post.post_published_date >= self.crawl_start_date)
                already_crawl_post_list = []
                for row in fetch:
                    already_crawl_post_list.append(row.post_id)
                
                print(already_crawl_post_list)

                for post in post_info: # 개별 포스트에 대해 반복 검증
                    if post[0] in already_crawl_post_list:#이미 크롤링한 게시물이므로 리액션 테이블만 업데이트
                        print('ALREADY CRAWLED POST , UPDATE :',post[0])
                        
                        post_reaction_fetch = pg_conn.session.query(pg_conn.Reaction).filter(pg_conn.Reaction.post_id == post[0])
                        already_post_reaction=[]
                        for row in post_reaction_fetch:
                            already_post_reaction.append(row.crawl_date)

                        
                        ## 오늘 이미 크롤링 수행한 게시물 , 업데이트 (하루에 여러번 돌리는 경우)
                        if datetime.date.today() in already_post_reaction:
                            db_update_register_reaction = pg_conn.session.query(pg_conn.Reaction).filter(
                            pg_conn.Reaction.post_id==post[0] and pg_conn.Reaction.crawl_date == datetime.date.today()).first()
                            db_update_register_reaction.sympathy_count = post[7]
                            db_update_register_reaction.comment_count = post[6]
                            pg_conn.session.commit()
                        ##  오늘의 첫 스캔, 오늘의 리액션을 담는다. (하루에 처음 돌리는 경우)
                        else: 
                            db_new_register_reaction = pg_conn.Reaction(
                                post_id = post[0],
                                sympathy_count = post[6],
                                comment_count = post[7],
                                crawl_date = datetime.date.today()
                            )
                            pg_conn.session.add(db_new_register_reaction)
                    
                    else:#크롤링 하지 않은 새로운 게시물일 경우
                        print('NEW POST CRAWL : ',post[0])
                        
                        db_new_register_post = pg_conn.Post(
                        post_id=post[0],
                        blog_id= self.blog_id,
                        post_published_date=post[1],
                        post_title= post[2],
                        post_hashtag= post[3],
                        post_url= post[4],
                        post_text= post[5], 
                        )
                        pg_conn.session.add(db_new_register_post)
                        pg_conn.session.commit()
                        db_new_register_reaction = pg_conn.Reaction(
                        post_id = post[0],
                        sympathy_count = post[6],
                        comment_count = post[7],
                        crawl_date = datetime.date.today()
                        )
                        pg_conn.session.add(db_new_register_reaction)
                        pg_conn.session.commit()
                
                print(already_crawl_post_list)
            except:
                print("DB POST INFO AND REACTION INFO TABLE UPDATE FAIL")

            
        
        #################################### 미등록된 블로그 : 
        else:
            try:
                blog_info = self.crawl_blog_info(self.blog_id,self.crawl_start_date)
                post_info = self.crawl_post_info(self.blog_id,self.crawl_start_date)
                blog_info = self.aggregate_post_count(blog_info,post_info)
            except:
                print("CRAWLER GET FAIL(NEW REGISTERD BLOG)")
            ##크롤링한 블로그 정보 삽입
            try:
                for blog in blog_info:
                    for i in range(len(blog)): # None 방지
                        if blog[i] is None:
                            blog[i] = 0
                    db_new_register_blog = pg_conn.Blog(blog_id=blog[0],count_date=blog[3],buddy_count=blog[2],visitor_count=blog[1],post_count=blog[4],blog_url=blog[5]) 
                    pg_conn.session.add(db_new_register_blog)
                
                pg_conn.session.commit()
                
                for post in post_info:
                    #크롤링한 포스트 정보 삽입
                    db_new_register_post = pg_conn.Post(
                        post_id=post[0],
                        blog_id= self.blog_id,
                        post_published_date=post[1],
                        post_title= post[2],
                        post_hashtag= post[3],
                        post_url= post[4],
                        post_text= post[5], 
                    )
                    pg_conn.session.add(db_new_register_post)
                    pg_conn.session.commit()
                    #크롤링한 리액션 정보 삽입
                    db_new_register_reaction = pg_conn.Reaction(
                        post_id = post[0],
                        sympathy_count = post[6],
                        comment_count = post[7],
                        crawl_date = datetime.date.today()
                    )
                    
                    pg_conn.session.add(db_new_register_reaction)
                    pg_conn.session.commit()
            except:
                print("DB POST&BLOG INFO INSERT FAIL")
        
        pg_conn.session.close()
    
if __name__=="__main__":
    print("INPUT BLOG ID :")
    blog_id = input()
    print("INPUT CRAWLING START DATE(YYYY-MM-DD):")
    crawl_start_date = input()
    crawler = BlogCrawler(blog_id,crawl_start_date)
    crawler.start_crawl()
    print("ALL DONE")
    