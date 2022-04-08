from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float, Text
Base = declarative_base()

class YahooMovie(Base):
    __tablename__ = 'yahoomovie'
    id = Column(Integer, primary_key=True)
    