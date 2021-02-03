# from sqlalchemy import create_engine  
# from sqlalchemy import Column, String, Integer, Date, ForeignKey, Text
# from sqlalchemy.ext.declarative import declarative_base  
# from sqlalchemy.orm import sessionmaker
import pg_conn

#db_string = "postgres://dmk_temp_dhk:kdh0126$$@culture.cka0sue4nsid.ap-northeast-2.rds.amazonaws.com:5432/CULTURE_REPORTS"
#db = create_engine(db_string)  
#base = declarative_base()


#Session = sessionmaker(db)  
#session = Session()

#base.metadata.create_all(db)

test_blog1 = pg_conn.Blog(blog_id='main5',count_date='2021-02-18',blog_url ='T!!!!!!!!!!!!!!!!')
test_blog2 = pg_conn.Blog(blog_id='main6',count_date='2021-02-18',blog_url ='T!!!!!!!!!!!!!!!!')
# test_blog2 = Blog(blog_id='test5',count_date='2021-02-02',blog_url ='TEST')
pg_conn.session.add(test_blog1)
pg_conn.session.add(test_blog2)

test = pg_conn.session.query(pg_conn.Blog)
for row in test:
    print(row.blog_id, row.count_date)


# #session.add(test_blog2)
pg_conn.session.commit()
pg_conn.session.close()