create table if not exists dwd_release.dwd_bidding_user (
release_session string,
release_status string,
idcard string,
matter_id string,
model_code string,
model_version string,
aid string
)
partitioned by (bdp_day string)
stored as orc;

insert into table dwd_release.dwd_bidding_user partition(bdp_day='${hiveconf:params}')
select 
release_session,
release_status,
get_json_object(exts,'$.idcard') as idcard ,
get_json_object(exts,'$.matter_id') as matter_id ,
get_json_object(exts,'$.model_code') as model_code ,
get_json_object(exts,'$.model_version') as model_version ,
get_json_object(exts,'$.aid') as aid 
from ods_release.ods_01_release_session
where release_status in('00','01');