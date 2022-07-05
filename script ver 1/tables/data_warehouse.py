#!python3
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime, MetaData

Base = declarative_base()

class dimUsers(Base):
    __tablename__ = 'dim_users'
    id = Column(Integer,primary_key=True)
    name = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    city = Column(String)
    country = Column(String)
    segmented_age = Column(String)
    gender = Column(String)
    
class dimProducts(Base):
    __tablename__ = 'dim_products'
    id = Column(Integer,primary_key=True)
    category = Column(String)
    sale_price = Column(Float)
    product_name = Column(String)

class factTransactions(Base):
    __tablename__ = 'fact_transactions'
    id = Column(Integer,primary_key=True)
    order_id = Column(Integer)
    user_id = Column(Integer)
    product_id = Column(Integer)
    purchased_time = Column(DateTime)