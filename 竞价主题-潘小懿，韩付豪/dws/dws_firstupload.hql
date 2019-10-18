
create table dws_release.dws_bidding_tmp as 

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
case when bdp_day >= date_format(date_sub(current_date(),6),'yyyyMMdd') and bdp_day <= date_format(current_date(),'yyyyMMdd') then 1 end dt7,
case when bdp_day >= date_format(date_sub(current_date(),14),'yyyyMMdd') and bdp_day <= date_format(current_date(),'yyyyMMdd') then 1 end dt15,
case when bdp_day >= date_format(date_sub(current_date(),179),'yyyyMMdd') and bdp_day <= date_format(current_date(),'yyyyMMdd') then 1 end dt180,
bdp_day        
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
case when bdp_day >= date_format(date_sub(current_date(),6),'yyyyMMdd') and bdp_day <= date_format(current_date(),'yyyyMMdd') then 1 end dt7,
case when bdp_day >= date_format(date_sub(current_date(),14),'yyyyMMdd') and bdp_day <= date_format(current_date(),'yyyyMMdd') then 1 end dt15,
case when bdp_day >= date_format(date_sub(current_date(),179),'yyyyMMdd') and bdp_day <= date_format(current_date(),'yyyyMMdd') then 1 end dt180,
bdp_day        
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
case when bdp_day >= date_format(date_sub(current_date(),6),'yyyyMMdd') and bdp_day <= date_format(current_date(),'yyyyMMdd') then 1 end dt7,
case when bdp_day >= date_format(date_sub(current_date(),14),'yyyyMMdd') and bdp_day <= date_format(current_date(),'yyyyMMdd') then 1 end dt15,
case when bdp_day >= date_format(date_sub(current_date(),179),'yyyyMMdd') and bdp_day <= date_format(current_date(),'yyyyMMdd') then 1 end dt180,
bdp_day        
from dwd_release.dwd_01_release_session
where bidding_type_con='RTB' and release_status_con like '01-02-02%';


insert into table dws_release.dws_bidding partition(bdp_day)
select 
release_session       ,
release_status_con    ,
device_num            ,
device_type           ,
sources               ,
bidding_status_con    ,
bidding_type_con      ,
bidding_price_con     ,
aid_con               ,
idcard_cn             ,
matter_id_con         ,
model_code_con        ,
model_version_con     ,
register              ,
ct                    ,
ctmax                 ,
success               ,
paysuccess            ,
pv                    ,
dt7                   ,
dt15                  ,
dt180                 ,
bdp_day               
from dws_release.dws_bidding_tmp

















------------------------------------错误数据
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
user_register_con    ,
ct        ,
'success' as success,
bdp_day        
from dwd_release.dwd_01_release_session
where bidding_type_con='RTB' and release_status_con like '01-02-03%'



