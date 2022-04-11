from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float, Text
Base = declarative_base()

class YahooMovie(Base):
    __tablename__ = 'yahoomovie'
    id = Column(Integer, primary_key=True)
    url = Column(String)
    movie_id = Column(Integer)
    name_ch = Column(String)
    name_en = Column(String)
    genres = Column(String)
    release_date = Column(Date)
    company = Column(String)
    imdb_score = Column(Float)
    directors = Column(String)
    actors = Column(String)
    img_url = Column(String)
    content = Column(Text)
    yahoo_score = Column(Float)
    vote_count = Column(Integer) 
