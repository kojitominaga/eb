select 
       mm.ya0 as y0, 
       mm.yb1 as y1, 
       mm.rcma as rcm, 
       t1.ebint, 
       round(t1.JulyMixDepth::numeric, 2) as JulyMixDepth, 
       round(t1.AprilTemp::numeric, 2) as AprilTemp
from 
     (select floor(sim_id / 2e7) as mm, 
     sim_id % 2e7 as ebint, 
     avg(pcd07) as JulyMixDepth,
     avg(tws04) as AprilTemp
     from mylake.sim 
     group by sim_id) t1, 
     mylake.mm   
where mm.mm = t1.mm 
order by ebint, mm.mm
limit 30
;


select 
       mm.ya0 as y0, 
       mm.yb1 as y1, 
       mm.rcma as rcm, 
       t1.ebint, 
       round(t1.August05m::numeric, 2) as August05mDepth, 
       round(t1.August15m::numeric, 2) as August15mDepth
from 
     (select floor(sim_id / 2e7) as mm, 
     sim_id % 2e7 as ebint, 
     avg(f05t08) as August05m,
     avg(f15t08) as August15m
     from mylake.sim2 
     group by sim_id) t1, 
     mylake.mm   
where mm.mm = t1.mm 
order by ebint, mm.mm
limit 30
;

select 
       mm.ya0 as y0, 
       mm.yb1 as y1, 
       mm.rcma as rcm, 
       round(avg(t1.August05m)::numeric, 2) as August05mDepth, 
       round(avg(t1.August15m)::numeric, 2) as August15mDepth
from 
     (select floor(sim_id / 2e7) as mm, 
     sim_id % 2e7 as ebint, 
     avg(f05t08) as August05m,
     avg(f15t08) as August15m
     from mylake.sim2 
     group by sim_id) t1, 
     mylake.mm   
where mm.mm = t1.mm 
group by y0, y1, rcm, mm.mm
order by mm.mm
limit 30
;








select 
       mm.ya0 as y0, 
       mm.yb1 as y1, 
       mm.rcma as rcm, 
       -- t1.ebint, 
       round(avg(t1.JulyMixDepth)::numeric, 2) as JulyMixDepth, 
       round(avg(t1.AprilTemp)::numeric, 2) as AprilTemp, 
       round(avg(t1.JulyTemp)::numeric, 2) as JulyTemp
from 
     (select floor(sim_id / 2e7) as mm, 
     sim_id % 2e7 as ebint, 
     avg(pcd07) as JulyMixDepth,
     avg(tws04) as AprilTemp, 
     avg(tws07) as JulyTemp
     from sim 
     group by sim_id) t1, 
     mm   
where mm.mm = t1.mm 
group by y0, y1, rcm
limit 30
;



   select scenario_id, rcm_id from sim, mm where sim_id / 1e8 = mm





create view temporary.lakes_for_kim as select 
lakes.ebint as ebint,
lakes.vatn_lnr as vatn_lnr
from 
     temporary.aure_lakes_km kim, 
     ecco_biwa_lakes_v_0_1 lakes 
where kim.vatn_lnr = lakes.vatn_lnr 
;

create view temporary.lakes_for_kim_already_done as select 
lakes.ebint as ebint,
lakes.vatn_lnr as vatn_lnr 
from 
     temporary.aure_lakes_km kim, 
     ecco_biwa_lakes_v_0_1 lakes
where kim.vatn_lnr = lakes.vatn_lnr 
and lakes.ccluster30 = 1;


create view temporary.lakes_for_kim_mylake_monthly_rounded as
 select 
       mm.ya0 as y0, 
       mm.yb1 as y1, 
       mm.rcma as rcm, 
lakes.vatn_lnr as vatn_lnr, 
t1.*
from 
     temporary.aure_lakes_km kim, 
     ecco_biwa_lakes_v_0_1 lakes, 
    (select floor(sim_id / 2e7) as mm, 
     sim_id % 2e7 as ebint, 
     round(avg(tws01)::numeric, 2) as t01,
     round(avg(tws02)::numeric, 2) as t02,
     round(avg(tws03)::numeric, 2) as t03,
     round(avg(tws04)::numeric, 2) as t04,
     round(avg(tws05)::numeric, 2) as t05,
     round(avg(tws06)::numeric, 2) as t06,
     round(avg(tws07)::numeric, 2) as t07,
     round(avg(tws08)::numeric, 2) as t08,
     round(avg(tws09)::numeric, 2) as t09,
     round(avg(tws10)::numeric, 2) as t10,
     round(avg(tws11)::numeric, 2) as t11,
     round(avg(tws12)::numeric, 2) as t12
     from mylake.sim 
     group by sim_id) t1, 
     mylake.mm mm
where kim.vatn_lnr = lakes.vatn_lnr and t1.ebint = lakes.ebint and mm.mm = t1.mm
and lakes.ccluster30 = 1;

create view temporary.lakes_for_kim_mylake_monthly_yearly as
 select 
       -- mm.ya0 as y0, 
       -- mm.yb1 as y1, 
       t1.y + mm.ya0 as year,
       mm.rcma as rcm, 
lakes.vatn_lnr as vatn_lnr, 
t1.*
from 
     temporary.aure_lakes_km   kim, 
     ecco_biwa_lakes_v_0_1     lakes, 
    (select 
     floor(sim_id / 2e7) as mm, 
     sim_id % 2e7 as ebint, 
     y as y,
     tws01 as t01,
     tws02 as t02,
     tws03 as t03,
     tws04 as t04,
     tws05 as t05,
     tws06 as t06,
     tws07 as t07,
     tws08 as t08,
     tws09 as t09,
     tws10 as t10,
     tws11 as t11,
     tws12 as t12
     from mylake.sim)         t1, 
     mylake.mm                mm
where kim.vatn_lnr = lakes.vatn_lnr 
and t1.ebint = lakes.ebint 
and mm.mm = t1.mm
and lakes.ccluster30 = 1;


select 
       mm.ya0 as y0, 
       mm.yb1 as y1, 
       mm.rcma as rcm, 
       t1.ebint, 
       round(t1.MixDepth07::numeric, 2) as MixDepthjuly, 
       round(t1.SurfTemp04::numeric, 2) as surfTempapr,
       round(t1.FrodePAR15m02::numeric, 2) as FrodePAR15mfeb,
       round(t1.FrodePAR05m02::numeric, 2) as FrodePAR05mfeb,
       round(t1.FrodeT15m08::numeric, 2) as FrodeT15maug,
       round(t1.FrodeT05m08::numeric, 2) as FrodeT05maug
from 
     (select floor(sim_id / 2e7) as mm, 
     sim_id % 2e7 as ebint, 
     avg(pcd07) as MixDepth07,
     avg(tws04) as SurfTemp04, 
     avg(f15p02) as FrodePAR15m02, 
     avg(f05p02) as FrodePAR05m02, 
     avg(f15t08) as FrodeT15m08, 
     avg(f05t08) as FrodeT05m08
     from mylake.sim2
     group by sim_id) t1, 
     mylake.mm   
where mm.mm = t1.mm 
order by ebint, mm.mm
limit 30
;

select 
       mm.ya0 as y0, 
       mm.yb1 as y1, 
       mm.rcma as rcm, 
       -- t1.ebint, 
       round(t1.MixDepth07::numeric, 2) as MixDepthjuly, 
       round(t1.SurfTemp04::numeric, 2) as surfTempapr,
       round(t1.FrodePAR15m02::numeric, 2) as FrodePAR15mfeb,
       round(t1.FrodePAR05m02::numeric, 2) as FrodePAR05mfeb,
       round(t1.FrodeT15m08::numeric, 2) as FrodeT15maug,
       round(t1.FrodeT05m08::numeric, 2) as FrodeT05maug
from 
     (select floor(sim_id / 2e7) as mm, 
     -- sim_id % 2e7 as ebint, 
     avg(pcd07) as MixDepth07,
     avg(tws04) as SurfTemp04, 
     avg(f15p02) as FrodePAR15m02, 
     avg(f05p02) as FrodePAR05m02, 
     avg(f15t08) as FrodeT15m08, 
     avg(f05t08) as FrodeT05m08
     from mylake.sim2
     group by mm) t1, 
     mylake.mm   
where mm.mm = t1.mm 
order by mm.mm
;

select 
       min(mm.ya0) as y0, 
       min(mm.yb1) as y1, 
       min(mm.rcma) as rcm, 
       -- t1.ebint, 
       round(avg(t1.MixDepth07)::numeric, 2) as MixDepthjuly, 
       round(avg(t1.SurfTemp04)::numeric, 2) as surfTempapr,
       round(avg(t1.SurfTemp08)::numeric, 2) as surfTempAug,
       round(avg(t1.FrodePAR15m02)::numeric, 2) as FrodePAR15mfeb,
       round(avg(t1.FrodePAR05m02)::numeric, 2) as FrodePAR05mfeb,
       round(avg(t1.FrodePAR15m08)::numeric, 2) as FrodePAR15maug,
       round(avg(t1.FrodePAR05m08)::numeric, 2) as FrodePAR05maug,
       round(avg(t1.FrodeT15m08)::numeric, 2) as FrodeT15maug,
       round(avg(t1.FrodeT05m06)::numeric, 2) as FrodeT05mjun, 
       round(avg(t1.FrodeT05m07)::numeric, 2) as FrodeT05mjul, 
       round(avg(t1.FrodeT05m08)::numeric, 2) as FrodeT05maug, 
       round(avg((t1.FrodeT05m06 + t1.FrodeT05m07 + t1.FrodeT05m08)/3)::numeric, 2) as frodet05m_jja
from 
     (select floor(sim_id / 2e7) as mm, 
     -- sim_id % 2e7 as ebint, 
     avg(pcd07) as MixDepth07,
     avg(tws04) as SurfTemp04, 
     avg(tws08) as SurfTemp08, 
     avg(f15p02) as FrodePAR15m02, 
     avg(f05p02) as FrodePAR05m02, 
     avg(f15p08) as FrodePAR15m08, 
     avg(f05p08) as FrodePAR05m08, 
     avg(f15t08) as FrodeT15m08, 
     avg(f05t06) as FrodeT05m06,
     avg(f05t07) as FrodeT05m07,
     avg(f05t08) as FrodeT05m08
     from mylake.sim2
     group by mm) t1, 
     mylake.mm, 
     bathymetry_predicted bath
where 
mm.mm = t1.mm and 
bath.lmmaxdepth > 10 and 
bath.lmmaxdepth < 20
group by mm.mm
order by mm.mm






copy (
select 
corine_2000_peat_bogs_area_km2 as peat_area, 
corine_2000_peat_bogs_area_km2 / st_area(c.geom) as peat_fraction, 
l.ebint as ebint, 
l.vatn_lnr as vatn_lnr, 
l.lake_id as old_lake_id

from 

public.ecco_biwa_lake_catchments c,
public.eccO_biwa_lakes_v_0_1 l

where 
c.ebint = l.ebint and  
l.ccomsat = 1)
To '/home/koji/peat_for_hong.csv' With CSV;



select 
       min(mm.ya0) as y0, 
       min(mm.yb1) as y1, 
       min(mm.rcma) as rcm, 
       min(categories.cat_longname) as category,
       min(categories.cat_id) as cat_id, 
       count(t1.cat_id) as frequency
from 
     (select floor(sim_id / 2e7) as mm, 
     	     -- sim_id % 2e7 as ebint, 
	     category as cat_id 
     from mylake.sim3) t1, 
     mylake.mm, 
     mylake.categories
where 
mm.mm = t1.mm and t1.cat_id = categories.cat_id
group by mm.mm, t1.cat_id
order by mm.mm, t1.cat_id




--ice

copy(

select
       min(mm.ya0) as y0, 
       min(mm.yb1) as y1, 
       min(mm.rcma) as rcm, 
       min(t1.ebint) as ebint, 
       median(icedu)::int as med_ice_duration, 
       median(iceon)::int as med_ice_on, 
       median(iceof)::int as med_ice_off, 
       count(CASE WHEN icedu = 0 THEN 1 ELSE null END) as num_winter_without_ice, 
       count(case when icedu > 364 then 1 else null end) as num_whole_year_ice
from
     (select floor(sim_id / 2e7) as mm, 
     	     sim_id % 2e7 as ebint, 
	     icedu as icedu, 
	     case when ((icedu > 364) or (icedu = 0)) then null else iceon end as iceon, 
	     case when ((icedu > 364) or (icedu = 0)) then null else iceof end as iceof
     from mylake.sim3 
     where (not (y = 0))
     ) t1, 	
     mylake.mm
where 
     mm.mm = t1.mm 
group by t1.ebint, mm.mm 
order by t1.ebint, mm.mm

)
To '/home/koji/median_ice_9_winters.csv' With CSV;





-- lakes that stratifies all the time
-- get id

select counts.ebint from 
(select 
       sim_id % 2e7 as ebint, 
       count(mixaut0) as ok_years
from mylake.sim3
where (not (mixaut0 is null)) and (not (mixspr1 is null))
group by ebint) counts
where counts.ok_years = 40
;





select
       min(mm.ya0) as y0, 
       min(mm.yb1) as y1, 
       min(mm.rcma) as rcm, 
       min(t1.ebint) as ebint, 
       median(maxdoy)::int as maximum_head_doy, 
       median(mixspr0)::int as spr_turn_start, 
       median(mixspr1)::int as spr_turn_end, 
       median(mixaut0)::int as aut_turn_start, 
       median(mixaut1)::int as aut_turn_end, 
       median(ndmix)::int as num_intermittent_mix_days, 
       median(nemix)::int as num_intermittent_mix_events
from
     (select floor(sim_id / 2e7) as mm, 
     	     sim_id %% 2e7 as ebint, 
	     maxdoy as maxdoy, 
	     mixspr0 as mixspr0, 
	     mixspr1 as mixspr1, 
	     mixaut0 as mixaut0, 
	     mixaut1 as mixaut1, 
	     ndmix as ndmix, 
	     nemix as nemix
     from mylake.sim3 
     where (not (y = 0))
     ) t1, 	
     mylake.mm
where 
     mm.mm = t1.mm 
     and 
     t1.ebint in -- all lakes that are always stratified
         (select counts.ebint from 
     	    (select 
     	       sim_id %% 2e7 as ebint, 
       	       count(mixaut0) as ok_years
	     from mylake.sim3
	     where (not (mixaut0 is null)) and (not (mixspr1 is null))
	     group by ebint) counts
          where counts.ok_years = 40)
group by t1.ebint, mm.mm 
order by t1.ebint, mm.mm

;

