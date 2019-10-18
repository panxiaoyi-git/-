
create table dwd_release.dwd_01_release_session(
release_session         string,
release_status_con      string,
device_num              string,
device_type             string,
sources                 string,
channels                string,
bidding_status_con      string,
bidding_type_con        string,
bidding_price_con       double,
aid_con                 string,
idcard_cn               string,
matter_id_con           string,
model_code_con          string,
model_version_con       string,
user_register_con       string,
ct                    bigint,
ctmax                    bigint,
pv                    bigint
)
partitioned by (bdp_day string)
stored as orc;


insert into table dwd_release.dwd_01_release_session partition(bdp_day='${hiveconf:params}')
select 
release_session,
concat_ws('-',collect_list(release_status)) as release_status_con,
device_num,
device_type,
sources,
channels,
concat_ws('-',collect_list(bidding_status))as bidding_status_con,
min(bidding_type) as bidding_type_con,
sum(bidding_price) as bidding_price_con,
min(aid) as aid_con,
min(idcard) as idcard_cn,
min(matter_id) as matter_id_con,
min(model_code) as model_code_con,
min(model_version) as model_version_con,
min(user_register) as user_register_con,
min(ct) as ct,
max(ct) as ctmax,
sum(bidding_status) as pv
from (
select
release_session,
release_status,
device_num,
device_type,
sources,
channels,
get_json_object(exts,'$.bidding_status') as bidding_status,
get_json_object(exts,'$.bidding_type') as bidding_type,
get_json_object(exts,'$.bidding_price') as bidding_price,
get_json_object(exts,'$.aid') as aid,
get_json_object(exts,'$.idcard') as idcard ,
get_json_object(exts,'$.matter_id') as matter_id ,
get_json_object(exts,'$.model_code') as model_code ,
get_json_object(exts,'$.model_version') as model_version ,
get_json_object(exts,'$.user_register') as user_register,
ct,bdp_day
from ods_release.ods_01_release_session
where bdp_day='${hiveconf:params}')tmp
group by 
release_session,
device_num,
device_type,
sources,
channels

