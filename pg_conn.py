from sqlalchemy import create_engine  
from sqlalchemy import Column, String, Integer, Date, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker


db = create_engine(db_string)  
base = declarative_base()
print('db engine created!!!!')

class Blog(base):  
    __tablename__ = 'blog_count_info'

    blog_id = Column(String(50),primary_key=True) # varchar(50)
    count_date = Column(String(50), primary_key=True) #varchar(50) - 설계상 Date lean 하게 설정
    blog_url = Column(String(100))
    buddy_count = Column(Integer)
    visitor_count = Column(Integer)
    post_count = Column(Integer)

class Post(base):
    __tablename__ = 'post_info'

    post_id = Column(String(50),primary_key=True) # varchar(50)
    blog_id = Column(String(50), ForeignKey(Blog.blog_id)) #varchar(50) - 설계상 Date lean 하게 설정
    post_published_date = Column(String(50), ForeignKey(Blog.count_date))
    post_title = Column(String(500))
    post_hashtag = Column(String(500))
    post_url = Column(String(100))
    post_text = Column(Text)

class Reaction(base):
    __tablename__ = 'post_reaction_info'

    post_id = Column(String(50),ForeignKey(Post.post_id),primary_key=True)
    sympathy_count = Column(Integer)
    comment_count = Column(Integer)
    crawl_date = Column(Date,primary_key=True)

Session = sessionmaker(db)  
session = Session()
base.metadata.create_all(db)
print('db session created!!')

#### ORM CRUD EXAMPLEs ####
# Create 
# test_blog1 = Blog(blog_id='test8',count_date='2021-02-18',blog_url ='TEST!!!!!!!!!!!!!!!!!!!!')
# session.add(test_blog1)  
# session.commit()

# Read
# tests = session.query(Blog)
# for te in tests:  
#     print(te.blog_id)

# Update
# test_blog1.blog_id = 'testNop12e'
# test = session.query(Blog).filter(Blog.visitor_count== 459).first()
# test.visitor_count += 1
# session.commit()

# Delete
# session.delete(test_blog1)  
# session.commit()  