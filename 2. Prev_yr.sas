`/* A. Assigning libraries and defining macro variables  */

options compress=yes;

/* Input File Path */
Libname inp_CL "/sasdata/Acturial/Reserving/Mar 22/Revised";
Libname inp_CL2 "/sasdata/Acturial/Reserving/Claims Data";


/* Output File Path */
Libname output "/sasdata/Acturial/Reserving/Reporting/Annual Reports/FY 22/Data";

/* Macro variables */
/* 1. Start date of FY */
%Let yrstdate= mdy(04,01,2021);
/* 2. Current valuation Month year */
%Let vmy=0322;
/* 3. Previous valuation Month year*/
%Let pvmy= 0321;
/* 4. Output file location*/
%Let filepath=/sasdata/Acturial/Reserving/Reporting/Annual Reports/FY 22/Data;

/*OS as on current quater end*/

proc sql;
create table OS_&VMY as
select FINAL_CLAIM_ID,
claim_id, 
pre_auth_id, 
main_claim_number,
product,
lob_int,
policyno,
uni_mmbr_key,
min_of_date_of_admission as incurreddate,
min_of_claim_reported_date as reportdate_int,
sum(gross_incurred_claim) as GrossClaimAmt,
sum(Net_Incurred_Claims) as Netclaim_wo_vqs,
sum(Net_Incurred_Claims_VQS) as Netclaimamt,
sum(final_claim_count) as claimcount
from Inp_CL2.final_claims_data_new_&pvmy
where upcase(final_status_used)='OUTSTANDING' 
group by 1,2,3,4,5,6,7,8,9,10;
quit;
/* PROC SORT DATA=output.TPA_CL_PY nodupkey; */
/* BY FINAL_CLAIM_ID; */
/*  */
/* PROC SORT DATA=OS_&VMY; */
/* BY FINAL_CLAIM_ID; */
/* data OS1_&VMY; */
/* merge OS_&VMY (in=x) output.TPA_CL_PY (in=y); */
/* by final_claim_id; */
/* if x=1; */
/* run; */
data OS2_&VMY;
set OS_&VMY;
if maximus_cl_id ne . then claim_id=maximus_cl_id;
if maximus_preauthid ne . then pre_auth_id=maximus_preauthid;
run;
data OS3_&VMY;
set OS2_&VMY;
drop maximus_cl_id maximus_preauthid;
run;
proc sort data=OS3_&VMY;
by pre_auth_id;
proc sql;
create table reportdate as
select distinct pre_auth_id , claim_reported_date as reportdate_int2
from inp_CL2.claims_total_&pvmy
where file_flag_o='preauth_tracker' and pre_auth_id ne .;
quit;
proc sort data=reportdate nodupkey;
by pre_auth_id;
data OS4_&VMY;
merge OS3_&VMY (in=x) reportdate (in=y);
by pre_auth_id;
if x=1;
run;

/*Other reqd fields*/
data OS_1_&VMY;
set OS4_&VMY;
format LOB $50.;
if LOB_INT='B2C' then LOB='Health Insurance - Individual';
else if LOB_INT='B2B' then LOB='Group Health  - Employer/Employee';
else if LOB_INT='B2O' then LOB='Group  - Other Schemes';
else if LOB_INT='GPA' then LOB='Group Personal Accident';
/* Incurredyear= year(incurreddate); */
/* Incurredyearmo= cats(Incurredyear,put(month(incurreddate),z2.)); */
/* Incurredyearqtr= cats(Incurredyear,'Q',qtr(incurreddate)); */
/* Reportyear= year(reportdate); */
/* Reportyearmo= cats(Reportyear,put(month(reportdate),z2.)); */
/* Reportyearqtr= cats(Reportyear,'Q',qtr(reportdate)); */
finalstatus='Outstanding';
format key$30.;
format claim_id_n $30.;
claim_id_n= compress(claim_id);
preauthID_n= compress(pre_auth_ID);
if preauthID_n='' or preauthID_n='.'   then key= claim_id_n;
else key=cats(preauthID_n,'P');
if index(final_claim_id,'G') gt 0 then key=final_claim_id;
if key='' or key='.' OR KEY='.P' then key=final_claim_id;
drop claim_id;
rename claim_id_n=claim_id;
run;

data OS_2;
set OS_1_&VMY;
format reportdate date9.;
reportdate= min(reportdate_int2,reportdate_int);
Incurredyear= year(incurreddate);
Incurredyearmo= cats(Incurredyear,put(month(incurreddate),z2.));
Incurredyearqtr= cats(Incurredyear,'Q',qtr(incurreddate));
Reportyear= year(reportdate);
Reportyearmo= cats(Reportyear,put(month(reportdate),z2.));
Reportyearqtr= cats(Reportyear,'Q',qtr(reportdate));
run;

/*query for required fields*/
proc sql;
create table output.os_&pvmy as
select 
key,
final_claim_id,
claim_id,
pre_auth_ID,
main_claim_number,

incurreddate,
reportdate,


finalstatus,
policyno,
LOB,
PRODUCT,
incurredyear,
Incurredyearmo,
Incurredyearqtr,
reportyear,
reportyearmo,
Reportyearqtr,
grossclaimamt,
netclaim_wo_vqs,
netclaimamt,
claimcount
from OS_2;
quit;
/*Merging status current year*/
proc sort data=output.os_&pvmy;
by key;

/*OS and paid current year*/
/*  Only file used for making current year data will be used as there can be claims paid before and not in current year
will be present in itd file , it cannot be used as paid> os > then it will be paid only when paid in current year */

proc sql;
create table OS_paid_&VMY as
select distinct FINAL_CLAIM_ID,
claim_id, 
pre_auth_id, 
key,
finalstatus
from output.paid_os_&vmy;
quit;
data status_current_yr;
set OS_paid_&VMY (keep=key finalstatus );
if finalstatus= 'Outstanding' then status_current_yr='OUTSTANDING';
else status_current_yr='PAID';
run;
proc sort data=status_current_yr nodupkey;
by key;
data status_current_yr_merge;
merge output.os_&pvmy (in=x) status_current_yr(in=y drop=finalstatus);
by key;
if x=1;
run;
/*FY and status to be used*/
Proc format;
Value Financial_Year
'01apr2008'D - '31Mar2009'D   =      '2008_09'
'01apr2009'D - '31Mar2010'D   =      '2009_10'
'01apr2010'D - '31Mar2011'D   =      '2010_11'
'01apr2011'D - '31Mar2012'D   =      '2011_12'
'01apr2012'D - '31Mar2013'D   =      '2012_13'
'01apr2013'D - '31Mar2014'D   =      '2013_14'
'01apr2014'D - '31Mar2015'D   =      '2014_15'
'01apr2015'D - '31Mar2016'D   =      '2015_16'
'01apr2016'D - '31Mar2017'D   =      '2016_17'
'01apr2017'D - '31Mar2018'D   =      '2017_18'
'01apr2018'D - '31Mar2019'D   =      '2018_19'
'01apr2019'D - '31Mar2020'D   =      '2019_20'
'01apr2020'D - '31Mar2021'D   =      '2020_21'
'01apr2021'D - '31Mar2022'D   =      '2021_22';

run;
/*Status to be used*/
data status_claim;
set status_current_yr_merge;
Occurrence_FY = put(incurreddate, financial_year.);
run;
data output.cl_wo_ct_adj_&pvmy;
set status_claim;
run;

/*  */
/*  */
/* Manually  adjusted  */
/*  */
/* Amount  */
/* data set1; */
/* set output.cl_wo_ct_adj_&pvmy; */
/* if key in ('403933','410535') then do; */
/* grossclaimamt=0; */
/* Netclaim_wo_vqs=0; */
/* Netclaimamt=0; */
/* end; */
/* run; */
/* PROC SORT DATA=set1; */
/* BY KEY; */
/*  */
/*  */
/* Count */
/*  */
/* Importing TPA previous year  */
/* proc import datafile="&filepath\Manual_CT_PY" */
/* out=output.ct_adj_manual_PY */
/* DBMS=xlsx */
/* replace; */
/* sheet="Sheet1"; */
/* ; */
/* run; */
/* proc sort data =output.ct_adj_manual_PY nodupkey; */
/* by key; */
/* proc sort data =output.cl_wo_ct_adj_&pvmy ; */
/* by key; */
/* data set2; */
/* merge output.cl_wo_ct_adj_&pvmy (in=x) output.ct_adj_manual_PY(in=y); */
/* by key; */
/* if x=1; */
/* run; */
/* data set3; */
/* set set2; */
/* if count_manual ne . then claimcount=count_manual; */
/* run; */
/* data set4; */
/* set set3; */
/* run; */
/*  */
/* data output.cl_final_&pvmy; */
/* set set4; */
/* run; */
/* data output.cl_final_&pvmy; */
/* set output.cl_final_&pvmy; */
/* if product='MHP_Indem' then status_current_yr=""; */
/* run; */

data output.cl_final_&pvmy ;
set output.cl_wo_ct_adj_&pvmy;
if final_claim_id='145131920924460' then claimcount=1;
run;

data output.cl_final_&pvmy;
set output.cl_final_&pvmy ;
if key='2000001325' then do;
product='MHP_CI';
lob='Group  - Other Schemes';
end;

if key='2000001603' then do;
product='MHP_Indem';
lob='Group  - Other Schemes';
end;
run;

proc export data=output.cl_final_&pvmy                         outfile="&filepath/Data_PM"
                          dbms=xlsx replace;
                           sheet="Mar'20 - Mar'21";
run;
