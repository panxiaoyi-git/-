create table if not exists dwd_release.dwd_bidding_user_tmp as 
select 
release_session,release_status,
get_json_object(exts,'$.idcard') as idcard,
get_json_object(exts,'$.matter_id') as matter_id,
get_json_object(exts,'$.model_code') as model_code,
get_json_object(exts,'$.model_version') as model_version,
get_json_object(exts,'$.aid') as aid,
bdp_day
from ods_release.ods_01_release_session
where release_status in('00','01');



insert into dwd_release.dwd_bidding_user partition(bdp_day) 
select 
release_session,
release_status,
idcard,
matter_id,
model_code,
model_version,
aid,bdp_day
from dwd_release.dwd_bidding_user_tmp;
 
 
drop table if exists dwd_release.dwd_bidding_user_tmp;
