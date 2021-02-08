# Blog-Crawler


본 레포지토리는 네이버 블로그-포스트 크롤링을 위한 것으로, API를 사용하지 않고 개별 블로그의 날짜별 포스트 데이터를 크롤링합니다. 블로그 크롤러는 파라미터로 네이버 블로그의 아이디(URL을 통해 확인 가능)와, 크롤링 시작일을 입력 인자로 받습니다. crawler.py를 통해 크롤러를 실행할 경우 크롤링한 데이터는 Postgre DB에 적재됩니다. 크롤러가 크롤링 한 데이터를 적재하는 DB 스키마는 첨부된 ERD 및 tables.txt와 같습니다. BlogCrawler의 메소드를 퍼블릭으로 사용해도 작동 가능합니다.

This repository is for Naver blog-post crawl and does not use API to crawl post data for individual blogs by date. The blog crawler receives the Naver blog's ID (which can be checked through the URL) as a parameter and the start date of crawling as an input factor. When you run a crawler through crawler.py, the crawled data is loaded into the Postgre DB. The DB schema that loads the data crawled by the crawler is the same as the attached ERD and tables.txt. BlogCrawler's method can also be used as public. 

You can see dev log here!
[https://www.notion.so/hobbeskim/Naver-Blog-Crawler-eed6272e0c3446eb833297735ee02c9b]

## Have To Prepare
>postgresql server or other db

## Requirements
- 아래 항목에 대해 개별 게시물에 대해 1일 단위로 데이터 수집 및 적재
- 블로그 수집 요구사항
    - blog
        - 블로그 id
        - 일자별 게시물 수
        - 방문자 수
        - url
    - post
        - 게시물 id
        - 게시물 제목
        - 게시물 내용
        - 게시물 해시태그
        - 게시물 공감
        - 게시물 댓글
        - 게시물 작성시간
        - 게시물 url

## Schema and SQL querys
![DBERD](https://user-images.githubusercontent.com/57410044/107179282-800d1600-6a19-11eb-9bdc-2614bfed9928.png)

    create table public.blog_count_info
    (
	    blog_id varchar(50),
	    count_date date,
	    blog_url varchar(100),
	    buddy_count integer,
	    visitor_count integer,
	    post_count integer,
	    primary key(blog_id,count_date)
    );

    create table public.post_info
    (
	    post_id varchar(50) primary key,
	    blog_id varchar(50),
	    post_published_date date,
	    post_title varchar(100), 
	    post_hashtag varchar(500),
	    post_url varchar(100),
	    post_text text,
	    foreign key (blog_id,post_published_date) references blog_count_info(blog_id,count_date)
    );

    create table public.post_reaction_info
    (
	    post_id varchar(50) primary key REFERENCES post_info(post_id),
	    sympathy_count integer,
	    comment_count integer
    );

## Execution
    #### CLI : at venv
    pip3 install -r requirments.txt

    #### set DB_CONNECT_INFO

    #### CLI 
    python crawler.py

    #### enter
    > blog_id
    > crawl_start_date

![Run](https://user-images.githubusercontent.com/57410044/107179720-95cf0b00-6a1a-11eb-92af-b4d434adf505.png)

![Result1](https://user-images.githubusercontent.com/57410044/107179907-fbbb9280-6a1a-11eb-8ffa-23f5af4c13a5.png)

![Result2](https://user-images.githubusercontent.com/57410044/107179927-08d88180-6a1b-11eb-9ec6-5cdd5608947f.png)
