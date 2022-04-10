from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
Base = declarative_base()

class Underarmour(Base):
    __tablename__ = 'underarmour'
    id = Column(Integer, primary_key=True)
    title = Column(String)
    url = Column(String)
    image = Column(String)
    prod_id = Column(String)
    price = Column(Integer)
