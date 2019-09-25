import tushare as ts
import sys
import os
import random
import pandas as pd
import progressbar
import numpy as np
import sys
import pickle
import multiprocessing
import time
import progressbar

from one_quant_data.utils import format_date_ts_pro
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import NVARCHAR, Float, Integer
import datetime



#def change(x,y):
#    return round(float((y-x)/x*100),2)
        
#使用创业板开板时间作为起始时间
START_DATE='2010-06-01'


Yesterday = (datetime.date.today()-datetime.timedelta(days=1)).strftime('%Y%m%d')

def load_data_config(config_file):
    if isinstance(config_file,str):
        config_json = json.load(open(config_file))
    else:
        config_json = config_file
    print('config file')
    print(config_file)
    assert config_json.get("start") is not None
    assert config_json.get("end") is not None
    #stocks = ts.get_stock_basics()
    #stock_pool = list(stocks.index)
    num_str = config_json.get("num")
    num = None if num_str is None else int(num_str)
    #if num_str is not None:
    #    stock_pool = stock_pool[:int(num_str)]    

    index_pool=["000001.SH","399001.SZ","399005.SZ","399006.SZ"]
    return (num,index_pool,config_json.get("start"),config_json.get("end"))

def pro_opt_stock_k(df):
    if df is None:
        return None
    df = df.astype({
            'open': np.float16,
            'high': np.float16,
            'low': np.float16,
            'close': np.float16,
            'pre_close': np.float16,
            'change': np.float16,
            'pct_chg': np.float16,
            'vol': np.float32,
            'amount': np.float32
        },copy=False)
    #df.rename(columns={'trade_date':'date'},inplace=True)
    df.sort_values(by=["trade_date"],inplace=True)
    return df

def pro_opt_stock_basic(df):
    df = df.astype({
            'close':np.float32,
            'turnover_rate':np.float16,   
            'turnover_rate_f':np.float16, 
            'volume_ratio':np.float16,    
            'pe':np.float16,              
            'pe_ttm':np.float16,          
            'pb':np.float16,              
            'ps':np.float16,              
            'ps_ttm':np.float16,          
            'total_share':np.float32,     
            'float_share':np.float32,     
            'free_share':np.float32,      
            'total_mv':np.float32,        
            'circ_mv':np.float32
    },copy=False)
    #df.rename(columns={'trade_date':'date'},inplace=True)
    return df

class DataEngine():
    def __init__(self,config_file='./config.json'):
        self.offline=False
        self.cache = None
        self.api = 'tushare_pro' 
        config_json = json.load(open(config_file)) 
        assert config_json.get('data_engine')
        config = config_json['data_engine']
        api = config['api']
        cache = config['cache']
        print(cache)
        if cache.get('db')=='mysql':
            self.cache='mysql'
            user=cache.get('user')
            password=cache.get('password')
            host =cache.get('host')
            port =cache.get('port')
            schema =cache.get('schema')
            self.conn = create_engine('mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format(user,password,host,port,schema))
            print('use mysql as data cache')
            self.tables = {}
        if api.get('name')=='tushare_pro':
            token = api.get('token')
            self.api = 'tushare_pro'
            self.pro = ts.pro_api(token) 
            ts.set_token(token)
            print(token)
            print('use tushare as data api')
            self.tables = {
                "stock_trade_daily":"pro_stock_k_daily",
                "index_trade_daily":"pro_index_k_daily",
                "stock_basic_daily":"pro_stock_basic_daily"
            }
        if api.get('name')=='tushare':
            self.tables = {
                "stock_trade_daily":"trade_daily",
                "index_trade_daily":"trade_daily"
            }
        if api.get('name')=='offline':
            self.offline=True
            self.tables = {
                "stock_trade_daily":"pro_stock_k_daily",
                "index_trade_daily":"pro_index_k_daily",
                "stock_basic_daily":"pro_stock_basic_daily"
            }


    def preview_cache(self):
        return

    #获得所有股票列表
    def get_all_stocks(self):
        if self.api=='tushare_pro':
            self.stock_info = self.pro.query('stock_basic')
            return list(self.stock_info.ts_code)
        if self.api=='tushare':
            self.stock_info = ts.get_stock_basics()
            return list(self.stock_info.code)
    
    def get_all_stock_basics(self):
        if self.api=='tushare_pro':
            self.stock_info = self.pro.query('stock_basic')
            return self.stock_info
        if self.api=='tushare':
            self.stock_info = ts.get_stock_basics()
            return self.stock_info

    def get_stock_basics(self,code,start=None,end=None):
        if self.api=='tushare_pro':
            return self.pro_get_stock_basic(code,start,end)
        return None

    def pro_get_stock_basic(self,code,start=None,end=None):
        table = self.tables['stock_basic_daily']
        query_stock = "select * from {} where ts_code='{}';".format(table,code)
        df = pd.read_sql_query(query_stock,self.conn)
        df = df.astype({
            'close': np.float16,
            'turnover_rate':np.float16,
            'turnover_rate_f':np.float16,
            'volume_ratio':np.float16,
            'pe':np.float16,
            'pe_ttm':np.float16,
            'pb':np.float16,
            'ps':np.float16,
            'ps_ttm':np.float16,
            'total_share':np.float32,
            'float_share':np.float32,
            'free_share':np.float32,
            'total_mv':np.float32,
            'circ_mv':np.float32,
            },copy=False)
        df.rename(columns={'close':'close_bfq'},inplace=True)
        #df.rename(columns={'close':'close_bfq','trade_date':'date'},inplace=True)
        df.drop(['ts_code'],axis=1,inplace=True)
        return df 

        
    def get_k_data(self,code,index=False,ktype='D',start=None,end=None):
        if ktype=='D':
            if self.api=='tushare':
                return self.get_k_data_daily(code,index,start,end)
            if self.api=='tushare_pro':
                return self.pro_get_k_data_daily(code,index,start,end)

    def get_market_data(self,date):
        df = self.get_market_data_cached(date)
        if df is None or df.shape[0]==0:
            df = self.pro.daily(trade_date=format_date_ts_pro(date))
        return df

    def get_market_data_cached(self,date):
        table = self.tables['stock_trade_daily']
        date = format_date_ts_pro(date)
        query= "select * from {} where trade_date='{}'".format(table,date)
        k_df = pd.read_sql_query(query,self.conn)
        table = self.tables['stock_basic_daily']
        query_basic = "select * from {} where trade_date='{}';".format(table,date)
        basic_df = pd.read_sql_query(query_basic,self.conn)
        return k_df.merge(basic_df,on=['ts_code'],how='inner')
        

    def pro_get_k_data_daily(self,code,index,start,end):
        df,cached_start,cached_end = self.get_k_data_daily_cached(code,index,start,end)
        if self.offline==True:
            return pro_opt_stock_k(df)
        #print(cached_start)
        #print(cached_end)
        #print(Yesterday)
        if cached_end is None or cached_end < Yesterday:
            begin_date = START_DATE if cached_end is None else cached_end
            #print('load from internet')
            #print(cached_end)
            #print(begin_date)
            if index:
                df = self.pro.index_daily(ts_code=code, start_date=format_date_ts_pro(begin_date)) 
            else:
                df = ts.pro_bar(ts_code=code, adj='qfq', start_date=format_date_ts_pro(begin_date))
            time.sleep(0.1)
            if df is None:
                return None
            if df is not None:
                self.pro_cache_data_daily(df,index,cached_end)
            df,cached_start,cached_end = self.get_k_data_daily_cached(code,index,start,end)
        return pro_opt_stock_k(df)

       
    def get_k_data_daily(self,code,index,start,end): 
        df,cached_start,cached_end = self.get_k_data_daily_cached(code,index,start,end)
        #print(cached_start,cached_end)
        if df is None:
            begin_date = START_DATE if cached_end is None else cached_end
            df = ts.get_k_data(code,index=index,ktype='D',start=begin_date)
            if df is not None:
                self.cache_data_daily(df,cached_end)
            if df.shape[0]==0:
                return df
            else:
                return df[(df.date>=start)&(df.date<=end)]
        else:
            return df
    
    def get_k_data_daily_cached(self,code,index,start,end):
        if self.api=='tushare':
            return self.ts_get_k_data_daily_cached(code,index,start,end)
        if self.api=='tushare_pro':
            return self.pro_get_k_data_daily_cached(code,index,start,end)

    def pro_get_k_data_daily_cached(self,code,index,start,end):
        start = format_date_ts_pro(start)
        end = format_date_ts_pro(end)
        if index==True:
            table = self.tables['index_trade_daily']
        else:
            table = self.tables['stock_trade_daily']
        query_min_max = "select min(trade_date),max(trade_date) from {} where ts_code='{}'".format(table,code)
        DBSession = sessionmaker(self.conn)
        session = DBSession()
        try:
            res = list(session.execute(query_min_max))
        except:
            res=[(None,None)] 
        #print(res)
        session.close()
        if res[0]==(None,None):
            return None,None,None
        cached_start,cached_end = res[0]
        if end is None:
            df = pd.read_sql_query("select * from {} where trade_date>='{}' and ts_code='{}';".format(table,start,code),self.conn)
            return df,cached_start,cached_end 
        elif end<=cached_end:
            df = pd.read_sql_query("select * from {} where trade_date>='{}' and trade_date<='{}' and ts_code='{}';".format(table,start,end,code),self.conn)
            return df,cached_start,cached_end 
        else:
            return None,cached_start,cached_end
        

    def ts_get_k_data_daily_cached(self,code,index,start,end):
        if index==True:
            if code.startswith('0'):
                code ='sh'+code
            if code.startswith('3'):
                code='sz'+code
         
        query_min_max = "select min(date),max(date) from trade_daily where code='{}';".format(code)
        DBSession = sessionmaker(self.conn)
        session = DBSession()
        res = list(session.execute(query_min_max))
        session.close()
        #print(list(res))
        if res[0]==(None,None):
            return None,None,None
        cached_start,cached_end = res[0]
        if end<=cached_end:
            df = pd.read_sql_query("select * from trade_daily where date>='{}' and date<='{}' and code='{}';".format(start,end,code),self.conn)
            return df,cached_start,cached_end 
        else:
            return None,cached_start,cached_end

    def cache_data_daily(self,df,cached_end):
        if cached_end is None:
            #print('######## cache new trade daily ############')
            new = df
        else:
            new = df[df.date>cached_end]
        new.to_sql(self.tables['stock_trade_daily'],con=self.conn,if_exists='append',index=False)
    
    def pro_cache_data_daily(self,df,index,cached_end):
        dtypedict = {
            'ts_code': NVARCHAR(length=10),
            'trade_date': NVARCHAR(length=8),
            'open': Float(),
            'high': Float(),
            'low': Float(),
            'close': Float(),
            'pre_close': Float(),
            'change': Float(),
            'pct_chg': Float(),
            'vol': Float(),
            'amount': Float()
        }
        if cached_end is None:
            #print('######## cache new trade daily ############')
            new = df
        else:
            new = df[df.trade_date>cached_end]
        if index:
            new.to_sql(self.tables['index_trade_daily'],con=self.conn,if_exists='append',index=False,dtype=dtypedict)
        else:
            new.to_sql(self.tables['stock_trade_daily'],con=self.conn,if_exists='append',index=False,dtype=dtypedict)

    def pro_stock_basic_of_stock(self,code):
        return

    def pro_stock_basic_on_the_date(self,date):
        dtypedict = {
            'ts_code': NVARCHAR(length=10),
            'trade_date': NVARCHAR(length=8),
            'close':Float(),
            'turnover_rate':Float(),   
            'turnover_rate_f':Float(), 
            'volume_ratio':Float(),    
            'pe':Float(),              
            'pe_ttm':Float(),          
            'pb':Float(),              
            'ps':Float(),              
            'ps_ttm':Float(),          
            'total_share':Float(),     
            'float_share':Float(),     
            'free_share':Float(),      
            'total_mv':Float(),        
            'circ_mv':Float()
        }
        table = self.tables['stock_basic_daily']
        query = "select * from {} where trade_date='{}'".format(table,format_date_ts_pro(date))
        try:
            df = pd.read_sql_query(query,self.conn)
        except:
            df = None
        if df is None or df.shape[0]==0:
            df = self.pro.daily_basic(trade_date=format_date_ts_pro(date))
            print('save stock basic on the date:{}'.format(date))
            df.to_sql(self.tables['stock_basic_daily'],con=self.conn,if_exists='append',index=False,dtype=dtypedict)
        #gl_float = df.select_dtypes(include=['float'])
        #df = gl_float.apply(pd.to_numeric,downcast='float')
        return pro_opt_stock_basic(df)

    def get_basic_on_the_date(self,date):
        if self.api=='tushare_pro':
            return self.pro_stock_basic_on_the_date(date)

    def get_trade_dates(self,start,end):
        dates = list(self.pro.index_daily(ts_code='000001.SH', start_date=format_date_ts_pro(start),end_date=format_date_ts_pro(end)).trade_date)
        return dates

    def update_cache_stock_basic(self):
        table = self.tables['stock_basic_daily']
        dates = list(self.pro.index_daily(ts_code='000001.SH', start_date=format_date_ts_pro(START_DATE)).trade_date)
        query_dates = 'SELECT trade_date FROM {} group by trade_date'.format(table);
        query_table = "SELECT table_name FROM information_schema.TABLES WHERE table_name ='{}';".format(table)
        DBSession = sessionmaker(self.conn)
        session = DBSession()
        table = list(session.execute(query_table))
        if len(table)==0:
            cached_dates=[]
        else:	
            cached_dates = list(map(lambda x:x[0],session.execute(query_dates)))
        #print(res)
        session.close()
        uncached = list(set(dates).difference(set(cached_dates)))
        print('stock of the dates need to be cached {}'.format(uncached))
        uncached = list(sorted(uncached))
        for date in uncached:
            #print(date)
            self.get_basic_on_the_date(date)
        
    def update_cache_stock_k(self):
        pbar = progressbar.ProgressBar().start()
        stock_pool = self.get_all_stocks()
        total=len(stock_pool)
        for i in range(total):
            pbar.update(int((i / (total - 1)) * 100))
            code = stock_pool[i]
            #print('update stock {}'.format(code))
            self.get_k_data(code,start=START_DATE)
        pbar.finish()

def update_cache_stock_basic():
    engine = DataEngine()
    engine.update_cache_stock_basic()


def update_cache_stock_k():
    engine = DataEngine()
    engine.update_cache_stock_k()

if __name__=="__main__":
    #update_cache_stock_k()
    #print(format_date_ts_pro('2010-07-01'))
    #exit(0)
    #engine = DataEngine()
    #df = engine.get_market_data('2017-07-03')
    #print(df)
    #df = engine.get_k_data('000319.SZ',start='2010-07-01',end='2017-07-01')
    #df = engine.get_k_data('000333.SZ',start='2010-07-01',end='2017-07-01')
    #df = engine.get_k_data('000651.SZ',start='2010-07-01',end='2017-07-01')
    #df = engine.get_basic_on_the_date('2010-06-01')
    #print(df)
    #print(df.info(memory_usage='deep'))
    update_cache_stock_basic()


    
    
