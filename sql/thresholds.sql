create or replace view view_bridges_50 as(
select * from bridges_joined bj 
where unofficial_sufficiency_rating < 50
);


create or replace view view_bridges_80 as(
select * from bridges_joined bj 
where unofficial_sufficiency_rating < 80
);

create or replace view view_pavement_eval_40 as(
select * from pavement_evaluation 
where pci < 40
);

create or replace view view_pavement_eval_70 as(
select * from pavement_evaluation 
where pci < 70
);

create or replace view view_vulnerable_users_involved as(
select * from safety_voyager sv 
where pedestrian_involved > 0
or cyclist_involved > 0
);

create or replace view view_ksi as(
select * from safety_voyager sv 
where severity_rating_code = 'Fatal Injury' 
or severity_rating_code = 'Suspected Serious Injury' 
);

create or replace view view_sw_gaps as(
select * from sidewalk_gaps_clipped sgc 
where sw_ratio < .5);


create or replace view view_vc_85 as (
select * from model_vol_2025_am_link mval 
where "volcapra~2"::int > 85);

create or replace view view_vc_100 as (
select * from model_vol_2025_am_link mval 
where "volcapra~2"::int > 100);

create or replace view view_tti as(
select * from dvrpcnj_inrixxdgeo22_1_jointraveltime1min_mercer dijmm 
where ttiwkd0910 > 1.5
);

create or replace view view_pti as(
select * from dvrpcnj_inrixxdgeo22_1_jointraveltime1min_mercer dijmm 
where ptiwkd0910 > 3
);

