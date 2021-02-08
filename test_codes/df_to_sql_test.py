"""
this file doesn't use at blog crawler 2.0
"""


from sqlalchemy import create_engine  
from sqlalchemy import Column, String, Integer, Date, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker
import pandas as pd
import datetime




def db_manage_blog(blog_info_list):
    '''
    이 함수에서 해주는 일 : DB 테이블 중 blog_info 연결->업데이트 or 쑤시기
    '''
    #blog_info = {'blog_id':'id', 'count_date' }
    db_engine = create_engine('postgresql://hobbes:6132@localhost:5432/crawler')
    for blog in blog_info_list:
        blog_id = blog['blog_id']
        count_date = blog['count_date']
        blog_url =blog['blog_url']
        buddy_count = blog['buddy_count']
        visitor_count = blog['visitor_count']
        post_count = blog['post_count']

        db_engine.execute(
            f"INSERT INTO blog_count_info (blog_id,count_date,blog_url,buddy_count,visitor_count,post_count) VALUES ('{blog_id}','{count_date}','{blog_url}','{buddy_count}','{visitor_count}','{post_count}') ON CONFLICT (blog_id,count_date) DO UPDATE SET buddy_count = {buddy_count},visitor_count = {visitor_count},post_count = {post_count};"
            )
    
    db_engine.dispose()
        
    print('DB')

if __name__=="__main__":
    blog_info_list = [
        {
            "blog_id" :"100museum",
            "count_date" : "2021-01-30",
            "blog_url" : "test",
            "buddy_count": "11",
            "visitor_count":"11",
            "post_count":"11"

        },
        {
            "blog_id" :"100museum",
            "count_date" : "2021-01-29",
            "blog_url" : "test",
            "buddy_count": "22",
            "visitor_count":"22",
            "post_count":"22"

        }
    ]
    db_manage_blog(blog_info_list)

    
