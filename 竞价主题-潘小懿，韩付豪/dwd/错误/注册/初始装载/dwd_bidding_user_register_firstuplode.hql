
create table if not exists dwd_release.dwd_bidding_user_register_tmp as 
select 
release_session,release_status,
get_json_object(exts,'$.user_register') as user_register,bdp_day
from ods_release.ods_01_release_session;


insert into dwd_release.dwd_bidding_user_register partition(bdp_day) select release_session,release_status,user_register,bdp_day
from dwd_release.dwd_bidding_user_register_tmp where release_status='06';
 
 
drop table if exists dwd_release.dwd_bidding_user_register_tmp;

