# one-quant-data

one_quant_data是A股量化数据引擎，用于为量化工具提供数据支撑

目前，本库的数据来源是基于tushare.pro接口，可以认为是tushare的数据接口封装

one_quant_data的数据访问接口和tushare.pro保持一致，由于tushare的数据访问有一定的频率限制，因此相当于在访问tushare的数据时将数据缓存到指定的数据库中，从而避免tushare的访问限制
同时，由于数据在指定数据库中，因此又可以提供一些新的使用数据的角度


使用时，需要在当前目录下建立配置文件config.json
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
        }
    }
}
```
