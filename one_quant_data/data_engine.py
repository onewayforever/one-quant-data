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

import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import NVARCHAR, Float, Integer
import datetime
import pymysql
np.set_printoptions(suppress=True)

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
        
#使用创业板开板时间作为默认起始时间
START_DATE='2010-06-01'
MIN_DATE='1990-01-01'

#TODAY=datetime.date.today().strftime('%Y%m%d')

def format_date_ts_pro(date):
    if isinstance(date,str):
        return date.replace('-','')
    return date

def dataframe_to_db_replace(df,table_name,conn):
    DBSession = sessionmaker(conn)
    session = DBSession()
    d = df.to_dict(orient='index')
    try:
        for lnum in d.keys():
            line = d[lnum]
            keys = ','.join(list(line.keys()))
            values = ','.join(map(lambda x:str(x) if not isinstance(x,str) else '\''+pymysql.escape_string(x)+'\'',list(line.values())))
            query = 'REPLACE INTO {} ({}) VALUES ({})'.format(table_name,keys,values)
        #print(query)
            session.execute(query)
        session.commit()
    except TypeError as res:
        print(res)
        session.rollback()
    session.close()
    


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
            'dv_ratio':np.float16,
            'dv_ttm':np.float16,
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
        self.stock_names = None
        self.offline=False
        self.conn = None
        self.cache = None
        self.cached_start = None
        self.cached_end = None
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
                "index_basic_daily":"pro_index_basic_daily",
                "stock_basic_info":"pro_stock_basic_info",
                "fina_mainbz_product":"pro_fina_mainbz_product",
                "fina_mainbz_district":"pro_fina_mainbz_district"
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
                            `vol` Float,\
                            `amount` Float,\
                            PRIMARY KEY (ts_code,trade_date),\
                            INDEX qkey (ts_code,trade_date))".format(table_name))
            table_name=self.tables["stock_fq_daily"]
            session.execute("CREATE TABLE IF NOT EXISTS {} (\
                            `ts_code` VARCHAR(10),\
                            `trade_date` VARCHAR(8),\
                            `adj_factor` Float,\
                            PRIMARY KEY (ts_code,trade_date),\
                            INDEX qkey (ts_code,trade_date))".format(table_name))
            table_name=self.tables["stock_basic_daily"]
            session.execute("CREATE TABLE IF NOT EXISTS {} (\
                            `ts_code` VARCHAR(10),\
                            `trade_date` VARCHAR(8),\
                            `close` Float(7,2),\
                            `turnover_rate` Float,\
                            `turnover_rate_f` Float,\
                            `volume_ratio` Float,\
                            `pe` Float,\
                            `pe_ttm` Float,\
                            `pb` Float,\
                            `ps` Float,\
                            `ps_ttm` Float,\
                            `dv_ratio` Float,\
                            `dv_ttm` Float,\
                            `total_share` Float,\
                            `float_share` Float,\
                            `free_share` Float,\
                            `total_mv` Float,\
                            `circ_mv` Float,\
                            PRIMARY KEY (ts_code,trade_date),\
                            INDEX qkey (ts_code,trade_date))".format(table_name))
            table_name=self.tables["index_trade_daily"]
            session.execute("CREATE TABLE IF NOT EXISTS {} (\
                            `ts_code` VARCHAR(10),\
                            `trade_date` VARCHAR(8),\
                            `close` Float(7,2),\
                            `open` Float(7,2),\
                            `high` Float(7,2),\
                            `low` Float(7,2),\
                            `pre_close` Float(7,2),\
                            `change` Float(7,2),\
                            `pct_chg` Float(7,2),\
                            `vol` Float,\
                            `amount` Float,\
                            PRIMARY KEY (ts_code,trade_date),\
                            INDEX qkey (ts_code,trade_date))".format(table_name))
            table_name=self.tables["index_basic_daily"]
            session.execute("CREATE TABLE IF NOT EXISTS {} (\
                            `ts_code` VARCHAR(10),\
                            `trade_date` VARCHAR(8),\
                            `total_mv` Float,\
                            `float_mv` Float,\
                            `total_share` Float,\
                            `float_share` Float,\
                            `free_share` Float,\
                            `turnover_rate` Float,\
                            `turnover_rate_f` Float,\
                            `pe` Float,\
                            `pe_ttm` Float,\
                            `pb` Float,\
                            PRIMARY KEY (ts_code,trade_date),\
                            INDEX qkey (ts_code,trade_date))".format(table_name))
            table_name=self.tables["fina_mainbz_product"]
            session.execute("CREATE TABLE IF NOT EXISTS {} (\
                            `ts_code` VARCHAR(10),\
                            `end_date` VARCHAR(8),\
                            `bz_item` VARCHAR(512),\
                            `bz_sales` Float,\
                            `bz_profit` Float,\
                            `bz_cost` Float,\
                            `curr_type` VARCHAR(4),\
                            `sync_date` VARCHAR(8),\
                            PRIMARY KEY (ts_code,end_date,bz_item,bz_sales),\
                            INDEX qkey (ts_code,end_date))".format(table_name))
            table_name=self.tables["fina_mainbz_district"]
            session.execute("CREATE TABLE IF NOT EXISTS {} (\
                            `ts_code` VARCHAR(10),\
                            `end_date` VARCHAR(8),\
                            `bz_item` VARCHAR(512),\
                            `bz_sales` Float,\
                            `bz_profit` Float,\
                            `bz_cost` Float,\
                            `curr_type` VARCHAR(4),\
                            `sync_date` VARCHAR(8),\
                            PRIMARY KEY (ts_code,end_date,bz_item,bz_sales),\
                            INDEX qkey (ts_code,end_date))".format(table_name))
            session.close()
        ### init engine
        #self.__generic_init_engine()

    def __del__(self):
        if self.conn is not None:
            self.conn.dispose()



    
    def __check_date_range(self,start_date,end_date):
        start_date = self.cached_start if start_date is None else start_date
        end_date = self.cached_end if end_date is None else end_date 
        #if start_date < self.cached_start:
        #    print('WARNING: query date {} before cached date {}'.format(start_date,self.cached_start))
        #if end_date > self.cached_end:
        #    print('WARNING: query date {} after cached date {}'.format(end_date,self.cached_end))
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
        if start_date is None:
            start_date = MIN_DATE
        if end_date is None:
            TODAY=datetime.date.today().strftime('%Y%m%d')
            end_date = TODAY
        #if start_date < self.cached_start:
        #    print('WARNING: query date {} before cached date {}'.format(start_date,self.cached_start))
        #if end_date > self.cached_end:
        #    print('WARNING: query date {} after cached date {}'.format(end_date,self.cached_end))
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
    
    '''
        index_dailybasic仅返回缓存中的数据，如果需要使用最新的数据，使用self.pro的tushare接口去访问
    '''
    def index_dailybasic(self,ts_code=None,trade_date=None,start_date=None,end_date=None):
        assert (ts_code is not None) or (trade_date is not None)
        if ts_code is not None:
            start_date,end_date = self.__check_date_range(start_date,end_date)
            df = pd.read_sql_query("select * from {} where trade_date>='{}' and trade_date<='{}' and ts_code='{}' order by trade_date;".format(self.tables['index_basic_daily'],start_date,end_date,ts_code,ts_code),self.conn)
            return df
        if trade_date is not None:
            df = pd.read_sql_query("select * from {} where trade_date='{}' order by ts_code;".format(self.tables['index_basic_daily'],trade_date),self.conn)
            return df
    '''
        index_daily仅返回缓存中的数据，如果需要使用最新的数据，使用self.pro的tushare接口去访问
    '''
    def index_daily(self,ts_code,start_date=None,end_date=None):
        start_date = self.cached_start if start_date is None else start_date
        end_date = self.cached_end if end_date is None else end_date 
        if start_date is None:
            start_date = MIN_DATE
        if end_date is None:
            TODAY=datetime.date.today().strftime('%Y%m%d')
            end_date = TODAY
        #if start_date < self.cached_start:
        #    print('WARNING: query date {} before cached date {}'.format(start_date,self.cached_start))
        #if end_date > self.cached_end:
        #    print('WARNING: query date {} after cached date {}'.format(end_date,self.cached_end))
        df_k = pd.read_sql_query("select * from {} where trade_date>='{}' and trade_date<='{}' and ts_code='{}' order by trade_date;".format(self.tables['index_trade_daily'],start_date,end_date,ts_code),self.conn)
        return df_k
    
    '''
        fina_mainbz 仅返回缓存中的数据，如果需要使用最新的数据，使用self.pro的tushare接口去访问
    '''

    def fina_mainbz(self,ts_code,period=None,type='P',start_date=None,end_date=None):
        start_date = '19900101' if start_date is None else start_date
        end_date = datetime.date.today().strftime('%Y%m%d') if end_date is None else end_date 
        if type=='P':
            table_name='fina_mainbz_product'
        elif type=='D':
            table_name='fina_mainbz_district'
        df= pd.read_sql_query("select * from {} where end_date>='{}' and end_date<='{}' and ts_code='{}' order by end_date;".format(self.tables[table_name],start_date,end_date,ts_code),self.conn)
        return df

    '''
        fina_mainbz_vip 仅返回缓存中的数据，如果需要使用最新的数据，使用self.pro的tushare接口去访问
    '''

    def fina_mainbz_vip(self,period,type='P'):
        if type=='P':
            table_name='fina_mainbz_product'
        elif type=='D':
            table_name='fina_mainbz_district'
        df= pd.read_sql_query("select * from {} where end_date='{}' order by ts_code;".format(self.tables[table_name],period),self.conn)
        return df

    def __prefetch_data(self):
        assert self.api=="tushare_pro"
        latest_date = sorted(self.trade_dates)[-2]
        df_index_basic = self.pro.index_dailybasic(trade_date=format_date_ts_pro(latest_date))
        index_codes=list(df_index_basic.ts_code)
        self.index_k_daily = {}
        #print('__prefetch_data {}'.format(latest_date))
        #print(index_codes)
        for index in index_codes:
            #print('-----prefetch index {}'.format(index))
            df=self.pro.index_daily(ts_code=index,start_date=format_date_ts_pro(self.START_DATE))
            self.index_k_daily[index] = df


    def pro_sync_data_by_date(self,date):
        #print(self.cached_trade_dates)
        if date not in self.cached_trade_dates['stock_trade_daily']:
            df_k = self.pro.daily(trade_date=format_date_ts_pro(date))
            try:
                df_k.to_sql(self.tables['stock_trade_daily'],con=self.conn,if_exists='append',index=False)
            except TypeError as res:
                pass; #print(res)
        if date not in self.cached_trade_dates['stock_fq_daily']:
            df_adj = self.pro.adj_factor(trade_date=format_date_ts_pro(date))
            try:
                df_adj.to_sql(self.tables['stock_fq_daily'],con=self.conn,if_exists='append',index=False)
            except TypeError as res:
                pass; #print(res)
        if date not in self.cached_trade_dates['stock_basic_daily']:
            df_basic = self.pro.daily_basic(trade_date=format_date_ts_pro(date))
            try:
                df_basic.to_sql(self.tables['stock_basic_daily'],con=self.conn,if_exists='append',index=False)
            except TypeError as res:
                pass; #print(res)
        if date not in self.cached_trade_dates['index_basic_daily'] or date not in self.cached_trade_dates['index_trade_daily']:
            df_index_basic = self.pro.index_dailybasic(trade_date=format_date_ts_pro(date))
            index_codes=list(df_index_basic.ts_code)
            if date not in self.cached_trade_dates['index_basic_daily']:
                try:
                    df_index_basic.to_sql(self.tables['index_basic_daily'],con=self.conn,if_exists='append',index=False)
                except TypeError as res:
                    pass; #print(res)
            if date not in self.cached_trade_dates['index_trade_daily']:
                #time.sleep(0.3)
                #print(self.index_k_daily)
                for index in index_codes:
                    df = self.index_k_daily[index]
                    df = df[df.trade_date==format_date_ts_pro(date)]
                    #df=self.pro.index_daily(ts_code=index,trade_date=format_date_ts_pro(date))
                    try:
                        df.to_sql(self.tables['index_trade_daily'],con=self.conn,if_exists='append',index=False)
                    except TypeError as res:
                        pass; #print(res)
    
    def sync_data_by_date(self,date):
        self.cached_range()
        if self.api=='tushare_pro':
            return self.pro_sync_data_by_date(date)
    
    def __get_cached_cmd_groupby_stock(self,table_name,cmd):
        DBSession = sessionmaker(self.conn)
        session = DBSession()
        query = 'SELECT ts_code,{} FROM {} group by ts_code'.format(cmd,self.tables[table_name]);
        #res = list(map(lambda x:x[0],session.execute(query)))
        res = list(session.execute(query))
        ret = dict(zip(map(lambda x:x[0],res),map(lambda x:x[1],res)))
        session.close()
        return ret
        

    def __get_cached_trade_dates(self):
        DBSession = sessionmaker(self.conn)
        session = DBSession()
        self.cached_trade_dates={}
        query_cached_stock_trade_dates = 'SELECT trade_date FROM {} group by trade_date'.format(self.tables['stock_trade_daily']);
        cached_stock_trade_dates = list(map(lambda x:x[0],session.execute(query_cached_stock_trade_dates)))
        self.cached_trade_dates['stock_trade_daily']=set(cached_stock_trade_dates)
        query_cached_stock_basic_dates = 'SELECT trade_date FROM {} group by trade_date'.format(self.tables['stock_basic_daily']);
        cached_stock_basic_dates= list(map(lambda x:x[0],session.execute(query_cached_stock_basic_dates)))
        self.cached_trade_dates['stock_basic_daily']=set(cached_stock_basic_dates)
        query_cached_fq_dates = 'SELECT trade_date FROM {} group by trade_date'.format(self.tables['stock_fq_daily']);
        cached_fq_dates= list(map(lambda x:x[0],session.execute(query_cached_fq_dates)))
        self.cached_trade_dates['stock_fq_daily']=set(cached_fq_dates)
        query_cached_index_trade_dates = 'SELECT trade_date FROM {} group by trade_date'.format(self.tables['index_trade_daily']);
        cached_index_trade_dates = list(map(lambda x:x[0],session.execute(query_cached_stock_trade_dates)))
        self.cached_trade_dates['index_trade_daily']=set(cached_index_trade_dates)
        query_cached_index_basic_dates = 'SELECT trade_date FROM {} group by trade_date'.format(self.tables['index_basic_daily']);
        cached_index_basic_dates= list(map(lambda x:x[0],session.execute(query_cached_stock_basic_dates)))
        self.cached_trade_dates['index_basic_daily']=set(cached_index_basic_dates)
        #print(res)
        session.close()
        cached_dates=set(cached_stock_trade_dates).intersection(set(cached_stock_basic_dates)).intersection(set(cached_fq_dates)).intersection(set(cached_index_trade_dates)).intersection(set(cached_index_basic_dates))
        cached_dates = list(sorted(cached_dates,reverse=True))
        return cached_dates
    
    '''
        按日期来同步所有股票数据
    '''
    def sync_data_iterate_date(self):
        if self.api == "offline":
            print('you should use tushare_pro to sync data')
            return
        table = self.tables['stock_basic_daily']
        #dates = list(self.pro.index_daily(ts_code='000001.SH', start_date=format_date_ts_pro(self.START_DATE)).trade_date)
        #dates = self.get_trade_dates(format_date_ts_pro(self.START_DATE))
        self.__stock_basic = self.pro.stock_basic() 
        self.__stock_basic.to_sql(self.tables['stock_basic_info'],con=self.conn,if_exists='replace',index=False)
        self.trade_dates = self.get_trade_dates(format_date_ts_pro(self.START_DATE))
        dates = self.trade_dates
        cached_dates = self.__get_cached_trade_dates()
        uncached = list(set(dates).difference(set(cached_dates)))
        print('-- SYNC DATA FROM TUSHARE --')
        print('Dates need to be cached: {}'.format(len(uncached)))
        uncached = list(sorted(uncached,reverse=True))
        total = len(uncached)
        if total==0:
            print('Already latest data, no data need to be sync')
            return
        print('To sync stock info from {} to today'.format(self.START_DATE))
        print('Dates to be synced from {} to {}, total {} days'.format(uncached[0],uncached[-1],total))
        start_time=datetime.datetime.now()
        print('sync start at {}'.format(start_time.strftime("%H:%M:%S")))
        self.__prefetch_data()
        pbar = progressbar.ProgressBar().start()
        if total>1:
            for i in range(total):
                pbar.update(int((i / (total - 1)) * 100))
                #print(date)
                self.sync_data_by_date(uncached[i])
                time.sleep(0.3)
        else:
            self.sync_data_by_date(uncached[0])
        pbar.finish()
        end_time=datetime.datetime.now()
        print('sync end at {}'.format(end_time.strftime("%H:%M:%S")))
    
    '''
        按股票来同步所有股票数据
    '''
    def sync_data_iterate_stock(self):
        if self.api == "offline":
            print('you should use tushare_pro to sync data')
            return
        mainbz_product_dates = self.__get_cached_cmd_groupby_stock('fina_mainbz_product','max(sync_date)')
        mainbz_district_dates = self.__get_cached_cmd_groupby_stock('fina_mainbz_district','max(sync_date)')
        pbar = progressbar.ProgressBar().start()
        stock_codes = list(self.stock_basic().ts_code)
        total = len(stock_codes)
        print('To sync data of {} stocks'.format(total))
        update_date = (datetime.date.today()-datetime.timedelta(days=10)).strftime('%Y%m%d')
        if total>1:
            for i in range(total):
                pbar.update(int((i / (total - 1)) * 100))
                #print(date)
                if mainbz_product_dates.get(stock_codes[i]) is None or mainbz_product_dates.get(stock_codes[i])<update_date:
                    self.__sync_mainbz_by_stock(stock_codes[i],'P',mainbz_product_dates.get(stock_codes[i]))
                    time.sleep(1)
                if mainbz_district_dates.get(stock_codes[i]) is None or mainbz_district_dates.get(stock_codes[i])<update_date:
                    self.__sync_mainbz_by_stock(stock_codes[i],'D',mainbz_district_dates.get(stock_codes[i]))
                    time.sleep(1)
        else:
            self.__sync_mainbz_by_stock(stock_codes[0],'P',mainbz_product_dates.get(stock_codes[0]))
            self.__sync_mainbz_by_stock(stock_codes[0],'D',mainbz_district_dates.get(stock_codes[0]))
            #self.sync_data_by_date(uncached[0])
            pass
        pbar.finish()

    def __sync_mainbz_by_stock(self,ts_code,by,after_date=None):
        if by=='P':
            table_name = 'fina_mainbz_product'
            df = self.pro.fina_mainbz(ts_code=ts_code, type='P')
        elif by=='D':
            table_name = 'fina_mainbz_district'
            df = self.pro.fina_mainbz(ts_code=ts_code, type='D')
        else:
            return
        #print('filter:{} {}'.format(ts_code,after_date))
        df.dropna(inplace=True)
        df.drop_duplicates(subset=['ts_code','end_date','bz_item','bz_sales'],inplace=True)
        TODAY=datetime.date.today().strftime('%Y%m%d')
        df['sync_date'] = TODAY
        dataframe_to_db_replace(df,self.tables[table_name],self.conn)
        #if after_date is not None:
            #print('do filter')
            #df = df[df.end_date>after_date]
        #print((ts_code,after_date))
        #df.to_sql(self.tables[table_name],con=self.conn,if_exists='append',index=False)

    def __generic_init_engine(self):
        cached_dates = self.__get_cached_trade_dates()
        self.cached_start = None if len(cached_dates)==0 else min(cached_dates) 
        self.cached_end = None if len(cached_dates)==0 else max(cached_dates)
        if self.cached_start is None or self.cached_end is None:
            print('ERROR: db is empty, please use sync_data to sync data first')
            exit(0)
        else:
            print('NOTICE: trade data is available from {} to {}'.format(self.cached_start,self.cached_end))
            #query_stock = "select * from {};".format(self.tables['stock_basic_info'])
            #self.__stock_basic = pd.read_sql_query(query_stock,self.conn)
            #query_stock = "SELECT ts_code FROM {} group by ts_code;".format(self.tables['index_basic_daily'])
            #self.__index_codes = list(pd.read_sql_query(query_stock,self.conn).ts_code)

    def cached_range(self):
        if self.cached_start is not None and self.cached_end is not None:
            return (self.cached_start,self.cached_end)
        cached_dates = self.__get_cached_trade_dates()
        self.cached_start = None if len(cached_dates)==0 else min(cached_dates) 
        self.cached_end = None if len(cached_dates)==0 else max(cached_dates)
        if self.cached_start is None or self.cached_end is None:
            print('ERROR: db is empty, please use sync_data to sync data first')
        else:
            print('NOTICE: trade data is available from {} to {}'.format(self.cached_start,self.cached_end))
        return (self.cached_start,self.cached_end)



        
    def stock_basic(self):
        #if self.__stock_basic is None:
        #    query_stock = "select * from {};".format(self.tables['stock_basic_info'])
        #    self.__stock_basic = pd.read_sql_query(query_stock,self.conn)
        #return self.__stock_basic
        query_stock = "select * from {};".format(self.tables['stock_basic_info'])
        df = pd.read_sql_query(query_stock,self.conn)
        self.stock_names = df[['ts_code','name']]
        return df

    '''
        自定义的api
    '''
    def get_cached_trade_dates(self):
        return self.__get_cached_trade_dates()

    def get_trade_dates(self,start=START_DATE):
        if self.api=='tushare_pro':
            return list(sorted(self.pro.index_daily(ts_code='000001.SH', start_date=format_date_ts_pro(start)).trade_date,reverse=True))
        else:
            return list(filter(lambda x:x>=start,self.__get_cached_trade_dates()))

    def index_codes(self):
        #if self.__index_codes is None:
        #    query_stock = "SELECT ts_code FROM {} group by ts_code;".format(self.tables['index_basic_daily'])
        #    self.__index_codes = list(pd.read_sql_query(query_stock,self.conn).ts_code)
        #return self.__index_codes
        query_stock = "SELECT ts_code FROM {} group by ts_code;".format(self.tables['index_basic_daily'])
        return list(pd.read_sql_query(query_stock,self.conn).ts_code)

    def attach_stock_name(self,df):
        if self.stock_names is None:
            self.stock_basic() 
        assert 'ts_code' in df.columns
        if 'name' in df.columns:
            return df
        else:
            df = df.merge(self.stock_names,on='ts_code',how='left')
            columns = list(df.columns)
            columns.remove('name')
            columns.insert(0,'name')
            return df[columns]

    def stock_daily_all(self):
        start_time=datetime.datetime.now()
        df = pd.read_sql_query("select * from {} trade_date;".format(self.tables['stock_trade_daily']),self.conn)
        end_time=datetime.datetime.now()
        print('{}->{}'.format(start_time,end_time))
        return pro_opt_stock_k(df)
        


        

def test_sync():
    print('Test data sync')
    engine = DataEngine('../config.json')
    engine.sync_data_iterate_stock()
    engine.sync_data_iterate_date()


def test_api():
    print('Test api')
    engine = DataEngine('../config.json')
    #res=engine.__get_cached_cmd_groupby_stock('stock_trade_daily','max(trade_date)')
    #print(res)
    #engine.sync_data_iterate_stock()
    df = engine.fina_mainbz_vip(period='20190630', type='P')
    df = engine.fina_mainbz(ts_code='000627.SZ', type='P')
    #engine.sync_data_by_date('2017-07-03')
    print(engine.stock_basic())
    print(engine.index_codes())
    df=engine.pro_bar('000651.SZ',adj='qfq')
    df_daily=engine.daily_basic(trade_date='20190926')
    df=engine.daily_basic('000651.SZ')
    df=engine.index_dailybasic(trade_date='20190926')
    df=engine.index_dailybasic('000001.SH')
    df=engine.index_daily('000001.SH')
    print(df)
    print(engine.attach_stock_name(df_daily))
    df=engine.stock_daily_all()
    print(df)
    print(df.shape)



if __name__=="__main__":
    #test_sync()
    test_api()

    
    
