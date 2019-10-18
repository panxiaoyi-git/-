
create table dws_release.dws_bidding(
release_session         string,
release_status_con      string,
device_num              string,
device_type             string,
sources                 string,
bidding_status_con      string,
bidding_type_con        string,
bidding_price_con       double,
aid_con                 string,
idcard_cn               string,
matter_id_con           string,
model_code_con          string,
model_version_con       string,
register                int   ,
ct                      bigint,
ctmax                   bigint,
success                 string,
paysuccess              string,
pv                      bigint,
dt7                     int   ,
dt15                    int   ,
dt180                   int   
)
partitioned by (bdp_day string)
stored as orc;


insert into dws_release.dws_bidding partition(bdp_day='${hiveconf:params}')

select
release_session      ,
release_status_con   ,
device_num           ,
device_type          ,
sources              ,

bidding_status_con   ,
bidding_type_con     ,
bidding_price_con    ,
aid_con              ,
idcard_cn            ,
matter_id_con        ,
model_code_con       ,
model_version_con    ,
case when user_register_con is not null then 1 end as  register  ,
ct                   ,
ctmax                ,
'success' as success,
'success' as paysuccess,
pv,
case when bdp_day >= date_format(date_sub('${hiveconf:params}',6),'yyyyMMdd') and bdp_day <= date_format('${hiveconf:params}','yyyyMMdd') then 1 end dt7,
case when bdp_day >= date_format(date_sub('${hiveconf:params}',14),'yyyyMMdd') and bdp_day <= date_format('${hiveconf:params}','yyyyMMdd') then 1 end dt15,
case when bdp_day >= date_format(date_sub('${hiveconf:params}',179),'yyyyMMdd') and bdp_day <= date_format('${hiveconf:params}','yyyyMMdd') then 1 end dt180
     
from dwd_release.dwd_01_release_session
where bidding_type_con='PMP'

union all

select
release_session      ,
release_status_con   ,
device_num           ,
device_type          ,
sources              ,

bidding_status_con   ,
bidding_type_con     ,
bidding_price_con    ,
aid_con              ,
idcard_cn            ,
matter_id_con        ,
model_code_con       ,
model_version_con    ,
case when user_register_con is not null then 1 end as  register  ,
ct                   ,
ctmax                ,
'failure' as success,
'failure' as paysuccess,
pv,
case when bdp_day >= date_format(date_sub('${hiveconf:params}',6),'yyyyMMdd') and bdp_day <= date_format('${hiveconf:params}','yyyyMMdd') then 1 end dt7,
case when bdp_day >= date_format(date_sub('${hiveconf:params}',14),'yyyyMMdd') and bdp_day <= date_format('${hiveconf:params}','yyyyMMdd') then 1 end dt15,
case when bdp_day >= date_format(date_sub('${hiveconf:params}',179),'yyyyMMdd') and bdp_day <= date_format('${hiveconf:params}','yyyyMMdd') then 1 end dt180
       
from dwd_release.dwd_01_release_session
where bidding_type_con='RTB' and release_status_con like '%02'

union all

select
release_session      ,
release_status_con   ,
device_num           ,
device_type          ,
sources              ,

bidding_status_con   ,
bidding_type_con     ,
bidding_price_con    ,
aid_con              ,
idcard_cn            ,
matter_id_con        ,
model_code_con       ,
model_version_con    ,
case when user_register_con is not null then 1 end as  register  ,
ct        ,
ctmax                ,
'success' as success,
case when release_status_con like '%04%' then 'success' else 'failure' end as paysuccess,
pv,
case when bdp_day >= date_format(date_sub('${hiveconf:params}',6),'yyyyMMdd') and bdp_day <= date_format('${hiveconf:params}','yyyyMMdd') then 1 end dt7,
case when bdp_day >= date_format(date_sub('${hiveconf:params}',14),'yyyyMMdd') and bdp_day <= date_format('${hiveconf:params}','yyyyMMdd') then 1 end dt15,
case when bdp_day >= date_format(date_sub('${hiveconf:params}',179),'yyyyMMdd') and bdp_day <= date_format('${hiveconf:params}','yyyyMMdd') then 1 end dt180
       
from dwd_release.dwd_01_release_session
where bidding_type_con='RTB' and release_status_con like '01-02-02%';

