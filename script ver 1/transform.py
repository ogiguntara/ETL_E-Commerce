#!python3

from numpy import outer, product
import pandas as pd

def transform_users(data):
    """
    tranform data and saving it to data warehouse
    """
    column_start = ['id','first_name','last_name','longitude','latitude','city','country','gender','age']
    column_end = ['id','name','longitude','latitude','city','country','gender','segmented_age']
    data = data[column_start]
    data = data.drop_duplicates(column_start)
    #lower all string
    data= data.applymap(lambda x:x.lower() if type(x) == str else x)
    #gabung nama
    data['name'] = data['first_name'] + " " +data['last_name']
    #segmentasi usia
    data['segmented_age'] = data['age'].apply(lambda x: 'remaja' if x <= 20 else ('dewasa' if x > 20 and x < 60 else 'usia lanjut'))
    data=data[column_end]
    return data

def transform_products(products,order_items):
    column_end = ['id','category','sale_price']
    products = pd.merge(products,order_items,left_index=True,right_on='product_id',suffixes=("","_x"))
    return products
    
def transform_order_items(data):
    column_start = ['id','order_id','user_id','product_id','created_at']
    column_end= ['id','order_id','user_id','product_id','purchased_time']
    #slice string date
    data['created_at'] = data['created_at'].str.slice(0, 9)
    #select row with status complete
    data = data[data['status']== 'Complete']
    data = data[column_start]
    data.columns = column_end
    return data

    