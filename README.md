# one-quant-data

one_quant_data是A股量化数据引擎，用于为量化工具提供数据支撑

目前，本库的数据来源是基于tushare.pro接口，可以认为是tushare的数据接口封装

由于tushare的数据访问有一定的频率限制，为避免tushare的访问限制，将数据缓存到自己的数据库中
同时，由于数据在指定数据库中，因此又可以提供一些新的使用数据的角度


### 安装方法
```
    $ pip install one_quant_data 
```


### 使用方法
 1. 先建立配置文件config.json
```
{
    "data_engine":{
        "api":{
            "name":"tushare_pro",
            "token":"your_tusharepro_token"
        },
        "cache":{
            "db":"mysql",
            "host":"yourdbhost",
            "port":3306,
            "user":"yourusername",
            "password":"yourpasswd",
            "schema":"yourdbname"
            "start_date":"20190101"  #需要缓存数据的起始日期
        }
    }
}
```
  说明：
   - api的name可选tushare_pro或者offline，offline表示只使用数据库里的数据
 2. 引用数据包
```
    from one_quant_data import DataEngine
```  
 3. 初始化类DataEngine
```
    engine = DataEngine('../config.json')
```
 4. 同步数据，可以启动定时任务进行调用，第一次运行会建立数据库并缓存从起始日期到今天的所有数据，以后再调用时只会增量添加数据。同步数据时必须使用tushare_pro接口 
     同步数据的接口有两个：
       sync_data_iterate_date是按日期同步，适合每天运行同步交易数据
       sync_data_iterate_stock是按股票同步，用于更新不频繁的数据，可以隔一段时间再调用一次
```
    engine.sync_data_iterate_date()
```
 5. 使用数据，使用接口与tushare.pro保持一致,区别在于只会返回缓存的数据
```    
    df = engine.stock_basic()
    df = engine.pro_bar('000651.SZ',adj='qfq')
    df = engine.daily_basic(trade_date='20190926')
    df = engine.daily_basic('000651.SZ')
    df = engine.index_dailybasic(trade_date='20190926')
    df = engine.index_dailybasic('000001.SH')
    df = engine.index_daily('000001.SH')
    df = engine.fina_mainbz(ts_code='000627.SZ', type='P')
    df = engine.fina_mainbz_vip(period='20190630', type='P')
```
 6. 需要使用tushare接口时，使用DataEngine的pro字段来访问tushare
```
    pro = engine.pro
    df = pro.query()
```
 7. 自定义的接口

    | 接口 | 功能 | 输入参数 | 输出格式 | 说明 |
    | ---- | ---- | -------- | -------- | ---- |
    |index_codes| 返回缓存的指数代码| | list | |  
    |cached_range| 返回缓存的数据的开始，结束时间| | (cached_start,cached_end) |输出格式为tuple|
    |attach_stock_name| 根据一个dataframe的ts_code，添加股票名称|df|带name列的dataframe|必须有ts_code字段|
    |get_cached_trade_dates|获得所有缓存的数据日期| |list | |
