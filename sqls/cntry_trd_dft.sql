with cmd_int_exp as (
select cur.cntry_cd, cur.country, cur.exp_inr_cr, prv.exp_inr_cr as prv_exp_inr_cr, cur.year,
case 
  when prv.exp_inr_cr is null then null
  when prv.exp_inr_cr = 0 then null
  else round((cur.exp_inr_cr - prv.exp_inr_cr)*100/prv.exp_inr_cr,2) 
end as exp_pct_change,
round(
  100* cur.exp_inr_cr/sum(cur.exp_inr_cr) over(), 4
) as exp_pct_share
from {cntry_int_exp_tb} cur
left join {cntry_int_exp_tb} prv 
on cur.cntry_cd = prv.cntry_cd
and prv.year = {prv_yr}
where cur.year = {run_yr}
),

cmd_int_imp as (
select cur.cntry_cd, cur.country, cur.imp_inr_cr, prv.imp_inr_cr as prv_imp_inr_cr, cur.year,
case 
  when prv.imp_inr_cr is null then null
  when prv.imp_inr_cr = 0 then null
  else round((cur.imp_inr_cr - prv.imp_inr_cr)*100/prv.imp_inr_cr,2) 
end as imp_pct_change,
round(
  100* cur.imp_inr_cr/sum(cur.imp_inr_cr) over(), 4
) as imp_pct_share
from {cntry_int_imp_tb} cur
left join {cntry_int_imp_tb} prv 
on cur.cntry_cd = prv.cntry_cd
and prv.year = {prv_yr}
where cur.year = {run_yr}
),

cmd_trd_deficit as(
  select exp.cntry_cd, exp.country, 
  exp.exp_inr_cr, exp.exp_pct_change, exp.exp_pct_share, exp.year, 
  imp.imp_inr_cr, imp.imp_pct_share, 
  imp.imp_pct_change,
  exp.exp_inr_cr - imp.imp_inr_cr as trd_dft_inr_cr,
  exp.prv_exp_inr_cr - imp.prv_imp_inr_cr as prv_trd_dft_inr_cr
  from cmd_int_exp exp
  join cmd_int_imp imp
  on exp.cntry_cd = imp.cntry_cd
  and exp.year = imp.year
  where exp.year = {run_yr}
)

select cur.cntry_cd, cur.country, cur.exp_inr_cr, cur.exp_pct_change, cur.exp_pct_share,
cur.imp_inr_cr, cur.imp_pct_change, cur.imp_pct_share,
cur.trd_dft_inr_cr,
case 
  when cur.prv_trd_dft_inr_cr is null then null
  when cur.prv_trd_dft_inr_cr = 0 then null
  else round(100*(cur.trd_dft_inr_cr-cur.prv_trd_dft_inr_cr)/cur.prv_trd_dft_inr_cr, 2)
end as trd_dft_pct_change,
round(
  100*cur.trd_dft_inr_cr/sum(cur.trd_dft_inr_cr) over(), 4
) as trd_dft_pct_share,
cur.year, {pct_change_yr_compr} as pct_change_yr_compr
from cmd_trd_deficit cur