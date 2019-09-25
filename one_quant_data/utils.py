import datetime
import sys
import pandas as pd
from multiprocessing import Process  
import psutil
import gc
import os

def show_sys_mem(info):
    gc.collect()
    rss = psutil.Process(os.getpid()).memory_info().rss
    data = psutil.virtual_memory()
    total = data.total #总内存,单位为byte
    free = data.available #可以内存
    print('|{}| mem usage:{}G({}%)'.format(info,int(rss/1024/1024/1024),(rss/total*100)))

def get_func_name():
    try:
        raise Exception
    except:
        exc_info = sys.exc_info()        
        traceObj = exc_info[2]      
        frameObj = traceObj.tb_frame 
        #print frameObj.f_code.co_name,frameObj.f_lineno
        Upframe = frameObj.f_back                        
        #print Upframe.f_code.co_name, Upframe.f_lineno  
        #return (Upframe.f_code.co_filename, Upframe.f_code.co_name, Upframe.f_lineno)
        #return '{}/{}'.format(Upframe.f_code.co_filename, Upframe.f_code.co_name)
        return '{}'.format(Upframe.f_code.co_name)

def change(x,y):
    return round(float((y-x)/x*100),2)

def format_price(df,price=0,suffix=''):
    if price==0:
        price = float(df.tail(1)['close'+suffix])
    for col in ['open','close','high','low']:
        df[col+suffix] =  df[col+suffix]/price
    return df,price
    
def format_volume(df,volume=0,suffix=''):
    if volume==0:
        volume = float(df.tail(1)['volume'+suffix])
    for col in ['volume']:
        df[col+suffix] =  df[col+suffix]/volume
    return df,volume

def format_date_ts_pro(date): 
    if isinstance(date,str):
        return date.replace('-','')   
    return date

def date_delta(date,delta):
    if date.find('-')==-1:
        date_format='%Y%m%d'
    else:
        date_format='%Y-%m-%d'
    t = datetime.datetime.strptime(date, date_format)
    if delta>0:
        t = t + datetime.timedelta(delta) 
    else:
        t = t - datetime.timedelta(-delta) 
    return t.strftime(date_format)
