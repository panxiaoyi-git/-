create table if not exists dwd_release.dwd_bidding_user_register
(release_session string,
release_status string,
user_register string)
partitioned by (bdp_day string)
stored as orc;

insert into table dwd_release.dwd_bidding_user_register partition(bdp_day='${hiveconf:params}')
select 
release_session,release_status,
get_json_object(exts,'$.user_register') as user_register
from ods_release.ods_01_release_session
where bdp_day='${hiveconf:params}' and release_status='06';