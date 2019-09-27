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


        
#使用创业板开板时间作为默认起始时间
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
    return df.round(2)

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
    return df.round(2)

class DataEngine():
    def __init__(self,config_file='./config.json'):
        self.offline=False
        self.cache = None
        self.api = 'offline' 
        config_json = json.load(open(config_file)) 
        assert config_json.get('data_engine')
        config = config_json['data_engine']
        api = config['api']
        cache = config['cache']
        self.tables = {
                "stock_trade_daily":"pro_stock_k_daily",
                "stock_fq_daily":"pro_stock_fq_daily",
                "index_trade_daily":"pro_index_k_daily",
                "stock_basic_daily":"pro_stock_basic_daily",
                "stock_basic_info":"pro_stock_basic_info"
        }
        if cache.get('db')=='mysql':
            self.cache='mysql'
            user=cache.get('user')
            password=cache.get('password')
            host =cache.get('host')
            port =cache.get('port')
            schema =cache.get('schema')
            self.conn = create_engine('mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format(user,password,host,port,schema))
            print('use mysql as data cache')
            self.START_DATE=cache.get('start_date',START_DATE)
        if api.get('name')=='tushare_pro':
            token = api.get('token')
            self.api = 'tushare_pro'
            self.pro = ts.pro_api(token) 
            ts.set_token(token)
            print(token)
            print('use tushare as data api')
            DBSession = sessionmaker(self.conn)
            session = DBSession()
            table_name=self.tables["stock_trade_daily"]
            session.execute("CREATE TABLE IF NOT EXISTS {} (\
                            `ts_code` VARCHAR(10),\
                            `trade_date` VARCHAR(8),\
                            `open` Float(7,2),\
                            `high` Float(7,2),\
                            `low` Float(7,2),\
                            `close` Float(7,2),\
                            `pre_close` Float(7,2),\
                            `change` Float(7,2),\
                            `pct_chg` Float(7,2),\
                            `vol` Float(16,2),\
                            `amount` Float(16,2),\
                            PRIMARY KEY (ts_code,trade_date),\
                            INDEX qkey (ts_code,trade_date))".format(table_name))
            table_name=self.tables["stock_fq_daily"]
            session.execute("CREATE TABLE IF NOT EXISTS {} (\
                            `ts_code` VARCHAR(10),\
                            `trade_date` VARCHAR(8),\
                            `adj_factor` Float(7,2),\
                            PRIMARY KEY (ts_code,trade_date),\
                            INDEX qkey (ts_code,trade_date))".format(table_name))
            table_name=self.tables["stock_basic_daily"]
            session.execute("CREATE TABLE IF NOT EXISTS {} (\
                            `ts_code` VARCHAR(10),\
                            `trade_date` VARCHAR(8),\
                            `close` Float(7,2),\
                            `turnover_rate` Float(7,2),\
                            `turnover_rate_f` Float(7,2),\
                            `volume_ratio` Float(7,2),\
                            `pe` Float(7,2),\
                            `pe_ttm` Float(7,2),\
                            `pb` Float(7,2),\
                            `ps` Float(7,2),\
                            `ps_ttm` Float(7,2),\
                            `total_share` Float(16,2),\
                            `float_share` Float(16,2),\
                            `free_share` Float(16,2),\
                            `total_mv` Float(16,2),\
                            `circ_mv` Float(16,2),\
                            PRIMARY KEY (ts_code,trade_date),\
                            INDEX qkey (ts_code,trade_date))".format(table_name))
            session.close()
        ### init engine
        self.__generic_init_engine()
        if self.api=="tushare_pro":
            self.__pro_init_engine()
        if self.api=="offline":
            self.__offline_init_engine()

    def get_trade_dates(self,start,end):
        dates = list(self.pro.index_daily(ts_code='000001.SH', start_date=format_date_ts_pro(start),end_date=format_date_ts_pro(end)).trade_date)
        return dates
    
    def __check_date_range(self,start_date,end_date):
        start_date = self.cached_start if start_date is None else start_date
        end_date = self.cached_end if end_date is None else end_date 
        if start_date < self.cached_start:
            print('WARNING: query date {} before cached date {}'.format(start_date,self.cached_start))
        if end_date > self.cached_end:
            print('WARNING: query date {} after cached date {}'.format(end_date,self.cached_end))
        return start_date,end_date

    '''
        daily_basic仅返回缓存中的数据，如果需要使用最新的数据，使用self.pro的tushare接口去访问
    '''
    def daily_basic(self,ts_code=None,trade_date=None,start_date=None,end_date=None):
        assert (ts_code is not None) or (trade_date is not None)
        if ts_code is not None:
            start_date,end_date = self.__check_date_range(start_date,end_date)
            df = pd.read_sql_query("select * from {} where trade_date>='{}' and trade_date<='{}' and ts_code='{}' order by trade_date;".format(self.tables['stock_basic_daily'],start_date,end_date,ts_code,ts_code),self.conn)
            return pro_opt_stock_basic(df)
        if trade_date is not None:
            df = pd.read_sql_query("select * from {} where trade_date='{}' order by ts_code;".format(self.tables['stock_basic_daily'],trade_date),self.conn)
            return pro_opt_stock_basic(df)
            

    '''
        pro_bar仅返回缓存中的数据，如果需要使用最新的数据，使用self.pro的tushare接口去访问
    '''
    def pro_bar(self,ts_code,start_date=None,end_date=None,asset='E',adj=None,freq='D'):
        assert asset=='E'
        start_date = self.cached_start if start_date is None else start_date
        end_date = self.cached_end if end_date is None else end_date 
        if start_date < self.cached_start:
            print('WARNING: query date {} before cached date {}'.format(start_date,self.cached_start))
        if end_date > self.cached_end:
            print('WARNING: query date {} after cached date {}'.format(end_date,self.cached_end))
        df_k = pd.read_sql_query("select * from {} where trade_date>='{}' and trade_date<='{}' and ts_code='{}' order by trade_date;".format(self.tables['stock_trade_daily'],start_date,end_date,ts_code),self.conn)
        if adj is None:
            return pro_opt_stock_k(df_k)
        df_fq = pd.read_sql_query("select * from {} where trade_date>='{}' and trade_date<='{}' and ts_code='{}' order by trade_date;".format(self.tables['stock_fq_daily'],start_date,end_date,ts_code),self.conn)
        df = df_k.merge(df_fq,on=['ts_code','trade_date'],how='inner')
        if adj=='qfq':
            latest_adj=float(df.tail(1).adj_factor)
            df.close = df.close*df.adj_factor/latest_adj 
            df.high = df.high*df.adj_factor/latest_adj 
            df.low = df.low*df.adj_factor/latest_adj 
            df.open= df.open*df.adj_factor/latest_adj 
        if adj=='hfq':
            df.close = df.close*df.adj_factor 
            df.high = df.high*df.adj_factor 
            df.low = df.low*df.adj_factor 
            df.open= df.open*df.adj_factor 
        return pro_opt_stock_k(df)
        

    def pro_sync_stock_info_by_date(self,date):
        df_k = self.pro.daily(trade_date=format_date_ts_pro(date))
        df_adj = self.pro.adj_factor(trade_date=format_date_ts_pro(date))
        df_basic = self.pro.daily_basic(trade_date=format_date_ts_pro(date))
        #print('sync stock trade data on the date:{} with data:{}'.format(date,df_k.shape[0]))
        try:
            df_k.to_sql(self.tables['stock_trade_daily'],con=self.conn,if_exists='append',index=False)
        except TypeError as res:
            print(res)
        try:
            df_adj.to_sql(self.tables['stock_fq_daily'],con=self.conn,if_exists='append',index=False)
        except TypeError as res:
            print(res)
        try:
            df_basic.to_sql(self.tables['stock_basic_daily'],con=self.conn,if_exists='append',index=False)
        except TypeError as res:
            print(res)
    
    def sync_stock_info_by_date(self,date):
        if self.api=='tushare_pro':
            return self.pro_sync_stock_info_by_date(date)

    def __get_cached_trade_dates(self):
        DBSession = sessionmaker(self.conn)
        session = DBSession()
        query_cached_trade_dates = 'SELECT trade_date FROM {} group by trade_date'.format(self.tables['stock_trade_daily']);
        cached_trade_dates = list(map(lambda x:x[0],session.execute(query_cached_trade_dates)))
        query_cached_basic_dates = 'SELECT trade_date FROM {} group by trade_date'.format(self.tables['stock_basic_daily']);
        cached_basic_dates= list(map(lambda x:x[0],session.execute(query_cached_basic_dates)))
        query_cached_fq_dates = 'SELECT trade_date FROM {} group by trade_date'.format(self.tables['stock_fq_daily']);
        cached_fq_dates= list(map(lambda x:x[0],session.execute(query_cached_fq_dates)))
        #print(res)
        session.close()
        cached_dates=set(cached_trade_dates).intersection(set(cached_basic_dates)).intersection(set(cached_fq_dates))
        cached_dates = list(sorted(cached_dates,reverse=True))
        return cached_dates
    
    '''
        按日期来同步所有股票数据
    '''
    def sync_stock_info(self):
        table = self.tables['stock_basic_daily']
        dates = list(self.pro.index_daily(ts_code='000001.SH', start_date=format_date_ts_pro(self.START_DATE)).trade_date)
        cached_dates = self.__get_cached_trade_dates()
        uncached = list(set(dates).difference(set(cached_dates)))
        print('dates of need to be cached: {}'.format(len(uncached)))
        uncached = list(sorted(uncached,reverse=True))
        total = len(uncached)
        if total==0:
            print('Already latest data, no data need to be sync')
            return
        print('To sync stock info from Yesterday to {}'.format(self.START_DATE))
        print('Dates to be synced from {} to {}, total {} days'.format(uncached[0],uncached[-1],total))
        pbar = progressbar.ProgressBar().start()
        for i in range(total):
            pbar.update(int((i / (total - 1)) * 100))
            #print(date)
            self.sync_stock_info_by_date(uncached[i])
            time.sleep(0.3)
        pbar.finish()

    def __generic_init_engine(self):
        cached_dates = self.__get_cached_trade_dates()
        self.cached_start = min(cached_dates)
        self.cached_end = max(cached_dates)
        print('NOTICE: trade data is available from {} to {}'.format(self.cached_start,self.cached_end))

    def __pro_init_engine(self):
        assert self.api=="tushare_pro"
        self.stock_basic = self.pro.stock_basic() 
        self.stock_basic.to_sql(self.tables['stock_basic_info'],con=self.conn,if_exists='replace',index=False)
        return

    def __offline_init_engine(self):
        assert self.api=="offline"
        query_stock = "select * from {};".format(self.tables['stock_basic_info'])
        self.stock_basic = pd.read_sql_query(query_stock,self.conn)
        return
        
    def stock_basic(self):
        return self.stock_basic
        


if __name__=="__main__":
    engine = DataEngine('../config.json')
    #engine.sync_stock_k_by_date('2017-07-03')
    #engine.sync_stock_info()
    df=engine.pro_bar('000651.SZ',adj='qfq')
    df=engine.daily_basic(trade_date='20190926')
    #df=engine.daily_basic('000651.SZ')
    print(df)


    
    
