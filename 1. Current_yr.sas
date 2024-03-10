/* A. Assigning libraries and defining macro variables  */

options compress=yes;

/* Input File Path */
Libname inp_CL "/sasdata/Acturial/Reserving/Mar 23";
Libname inp_CL2 "/sasdata/Acturial/Reserving/Claims Data";


/* Output File Path */
Libname output "/sasdata/Acturial/Reserving/Reporting/Annual Reports/FY 23/Data";

/* Macro variables */
/* 1. Start date of FY */
%Let yrstdate= mdy(04,01,2022);
/* 2. Current valuation Month year */
%Let vmy=0323;
/* 3. Previous valuation Month year*/
%Let pvmy= 0322;
/* 4. Output file location*/
%Let filepath=/sasdata/Acturial/Reserving/Reporting/Annual Reports/FY 23/Data;


/* ------------------------------------------------------------------------------------------------------------*/


/* B. Working  */

/* 1. Paid data */

/* Summary:Paid data will be prepared from processed claim file as it contains 
           paid date, columns which are not available in paid data are mapped 
           from final claims data using claim id */

/* processed cl consists of the following cols: 
preauthid, paiddate, gic, nic, nic_vqs, claimid, incurreddate, product, lob, count,
paymentmode, liitgationstatus, incurredyearmonth*/
data paid_set;
set inp_CL.processed_claims_paid_v2_&vmy (where= (paiddate ge &yrstdate));run;

/* Mapping other reqd cols: main claim no, final cl id, reportdate in claims data,policyno, uni mmbr key */
proc sort data=paid_set ;
by claim_id;
proc sql;
create table set1 as
select distinct claim_id,
main_claim_number,
final_claim_id,
MIN_of_Claim_Reported_Date as ReportDate_int,
policyno,
uni_mmbr_key
from inp_cl.final_claims_data_new_&vmy.
where upcase(final_status_used)='PAID' ;  
quit;
data set1;
set set1;
format claim_ID_N $30.;
claim_ID_N= COMPRESS(claim_id);
if index(upcase(final_claim_id),'G') gt 0 then  claim_ID_N=final_claim_id;
if claim_id=. then claim_id_N=final_claim_id;
run;
data set1;
set set1;
drop claim_id final_claim_id;
run;
data set1;
set set1;
rename claim_ID_N= claim_id;
run;
proc sort data=set1 nodupkey;
by claim_id;
/* Dropping incurredyearmo- will be created later in paid and os appended data */
data fields_merged;
merge paid_set (in=x drop= incurredyearmo) set1(in=y) ;
by claim_id;
if x=1;
run;

/*Other reqd fields and renaming as per os data to append */
data paid_f;
set fields_merged;
format key$30.;
if Pre_Auth_ID=. then key=claim_id;
else key=cats(Pre_Auth_ID,"P");
format finalstatus$20.;
finalstatus='Paid';
rename incurred_date=incurreddate;
rename FINAL_CLAIM_COUNT=claimcount;
run;
/*  */
/*  Append TPA Paid data during the year */
/*  */
/* Importing TPA previous year  */
/* proc import datafile="&filepath\TPA_Working" */
/* out=output.TPA_PrevYr */
/* DBMS=xlsx */
/* replace; */
/* sheet="Prev_Yr"; */
/* ; */
/* run; */
/* data TPA_PrevYR; */
/* set output.TPA_PrevYr ; */
/* rename claim_id=final_claim_id; */
/* run; */
/* proc sort data=TPA_PrevYr; */
/* by final_claim_id; */
/* Importing paid ids  */
/* proc import datafile="&filepath\TPA_Working" */
/* out=output.TPA_Paid_cy */
/* DBMS=xlsx */
/* replace; */
/* sheet="Paid_tpa_finance"; */
/* ; */
/* run; */
/* data TPA_Paid_cy; */
/* set output.TPA_Paid_cy ; */
/* rename VMC_claim_id=final_claim_id; */
/* run; */
/* proc sort data=TPA_Paid_cy NODUPKEY; */
/* by final_claim_id; */
/*  */
/* Paid during the period */
/* data TPA_Paid; */
/* merge TPA_PrevYr (in=x) TPA_Paid_cy (in=y); */
/* by final_claim_id; */
/* if x=1 and y=1; */
/* run; */
/* data TPA_Paid2; */
/* set TPA_Paid; */
/* if paiddate= . then paiddate=max(&yrstdate,min_of_claim_reported_date,min_of_date_of_admission); */
/* RENAME KEY=UNI_MMBR_KEY; */
/* DROP MEMBER_NUMBER; */
/* run; */
/* Renaming as per paid data */
/* data TPA_Paid_F; */
/* set TPA_Paid2; */
/* rename MIN_of_Date_of_Admission= incurreddate; */
/* rename Final_claim_count= claimcount; */
/* rename MIN_of_Claim_Reported_Date=reportdate_int; */
/* if claim_type='Cashless' then paymentmode='DS'; else paymentmode='RM'; */
/* LitigationStatus='NotLitigated'; */
/* key=final_claim_id; */
/* drop claim_type; */
/* run; */

data output.paid_set;
set paid_f ;
/* if key in ('CKP2','CKP4','CKP6') then delete; */
run;

/* 2. Outstanding data */

/* Summary:OS data will be prepared from final claim data file as it contains all reqd columns,
           comb_os_summary cannot be used as data is not available at claim level*/
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
billed_amount as Billedamount ,
approved_amount as Approvedamount,
claim_type_main_cl as paymentmode ,
min_of_date_of_admission as incurreddate,
min_of_claim_reported_date as reportdate_int,
sum(gross_incurred_claim) as GrossClaimAmt,
sum(Net_Incurred_Claims) as Netclaim_wo_vqs,
sum(Net_Incurred_Claims_VQS) as Netclaimamt,
sum(final_claim_count) as claimcount
from inp_CL.final_claims_data_new_&vmy
where upcase(final_status_used)='OUTSTANDING' 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13;
quit;
data OS_&VMY;
set OS_&VMY;
if claim_id=641954
then incurreddate='29Jan2021'd;

if claim_id=2000001426
then reportdate_int='15Jan2020'd;
run;
/*Creating Other reqd fields and formatting as per paid data*/
data OS_1_&VMY;
set OS_&VMY;
if paymentmode='Direct Settlement' then paymentmode='DS';
if paymentmode='Reimbursement' then paymentmode='RM';
format LOB $50.;
if LOB_INT='B2C' then LOB='Health Insurance - Individual';
else if LOB_INT='B2B' then LOB='Group Health  - Employer/Employee';
else if LOB_INT='B2O' then LOB='Group  - Other Schemes';
else if LOB_INT='GPA' then LOB='Group Personal Accident';
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


/*3. Appending paid and oustanding data*/
data paid_os;
set OS_1_&VMY output.paid_set ;
run;

/*4. Claim reported date in claims data */
/* Claims reported date should be equal to pre auth reported date for DS cases,
 this date is incorrect wherever claim has been attached to preauth
 correcting this using raw trackers */
proc sql;
create table reportdate as
select distinct pre_auth_id , claim_reported_date as reportdate_int2
from inp_CL2.claims_total_&vmy
where file_flag_o='preauth_tracker' and pre_auth_id ne .;
quit;
proc sort data=reportdate nodupkey;
by pre_auth_id;
proc sort data=paid_os ;
by pre_auth_id;
data paid_os_2;
merge paid_os (in=x) reportdate (in=y);
by pre_auth_id;
if x=1;
run;
/*  */
/* data paid_os_3; */
/* set paid_os_2; */
/* if key ='409417' then do; */
/* incurreddate='02Jan13'd; */
/* reportdate_int2='02Jan13'd; */
/* reportdate_int='02Jan13'd; */
/* end; */
/* if key='172880P' then do; */
/* reportdate_int2='20Jan18'd; */
/* reportdate_int='20Jan18'd; */
/* end; */
/* if key='187546P' then do; */
/* reportdate_int2='14May18'd; */
/* reportdate_int='14May18'd; */
/* end; */
/* if key='203759P' then do; */
/* reportdate_int2='06Sep18'd; */
/* reportdate_int='06Sep18'd; */
/* end; */
/* if key='224169P' then do; */
/* reportdate_int2='20Jan19'd; */
/* reportdate_int='20Jan19'd; */
/* end; */
/* if key='233650P' then do; */
/* reportdate_int2='28Mar19'd; */
/* reportdate_int='28Mar19'd; */
/* end; */
/* if key='259540P' then do; */
/* reportdate_int2='07Sep19'd; */
/* reportdate_int='07Sep19'd; */
/* end; */
/* if key='265846P' then do; */
/* reportdate_int2='12Oct19'd; */
/* reportdate_int='12Oct19'd; */
/* end; */
/* if key='274881P' then do; */
/* reportdate_int2='02Dec19'd; */
/* reportdate_int='02Dec19'd; */
/* end; */
/* if key='282999P' then do; */
/* reportdate_int2='25Jan20'd; */
/* reportdate_int='25Jan20'd; */
/* end; */
/* run; */

/*5. Creating other required columns in the data*/
data paid_os_f;
set paid_os_2;
format reportdate date9.;
reportdate= min(reportdate_int2,reportdate_int);
Incurredyear= year(incurreddate);
Incurredyearmo= cats(Incurredyear,put(month(incurreddate),z2.));
Incurredyearqtr= cats(Incurredyear,'Q',qtr(incurreddate));
Paidyear= year(paiddate);
Paidyearmo= cats(Paidyear,put(month(paiddate),z2.));
Paidyearqtr= cats(Paidyear,'Q',qtr(paiddate));
Reportyear= year(reportdate);
Reportyearmo= cats(Reportyear,put(month(reportdate),z2.));
Reportyearqtr= cats(Reportyear,'Q',qtr(reportdate));
format claim_type $30.;
if main_claim_number=. then claim_type='Main Claim';
else claim_type='Pre/Post Claim';

report_delay_m= intck('month', incurreddate,reportdate,'C' );
report_delay_q=intck('qtr', incurreddate,reportdate,'C' );
paid_delay_m=intck('month', incurreddate,paiddate,'C' );
paid_delay_q=intck('qtr', incurreddate,paiddate,'C' );
run;

/*6. Selecting required columns from the data*/
proc sql;
create table output.paid_os_1 as
select 
key,
final_claim_id,
claim_id,
pre_auth_ID,
main_claim_number,

incurreddate,
reportdate,
paiddate,

paymentmode,
claim_type,
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
paidyear,
paidyearmo,
paidyearqtr,
report_delay_m,
report_delay_q,
paid_delay_m,
paid_delay_q,

Approvedamount,
Billedamount,
grossclaimamt,
netclaim_wo_vqs,
netclaimamt,
claimcount
from paid_os_f;
quit;

/*7. Mapping status of previous year*/
proc sort data=output.paid_os_1;
by key;
/*  */
/* DATA output.paid_os_1; */
/* SET output.paid_os_1; */
/* IF KEY='2000001025' THEN DO; */
/* PRODUCT='MHP_Indem'; */
/* LOB='Group  - Other Schemes'; */
/* END; */
/* RUN; */

/*OS last year*/
proc sql;
create table OS_&PVMY as
select FINAL_CLAIM_ID,
claim_id, 
pre_auth_id, 
product,
lob_int,
final_status_used,
min_of_date_of_admission as incurreddate,
min_of_claim_reported_date as reportdate,
RCD,
sum(gross_incurred_claim) as GrossClaimAmt,
sum(Net_Incurred_Claims) as Netclaim_wo_vqs,
sum(Net_Incurred_Claims_VQS) as Netclaimamt,
sum(final_claim_count) as claimcount
from inp_CL2.final_claims_data_new_&pvmy
where final_status_used ='OUTSTANDING'
group by 1,2,3,4,5,6,7,8,9;
quit;

/* TPA Claim id mapping */
/* proc sql; */
/* create table a as */
/* select final_claim_id, policyno, membership_number */
/* from inp_CL2.final_claims_data_new_&pvmy */
/* where final_status_used ='OUTSTANDING' and claim_id=. and pre_auth_id=.; */
/* quit; */
/* proc export data=a */
/*                            outfile="&filepath/TPA_CL_PY" */
/*                           dbms=xlsx replace; */
/*                            sheet="TPA_CL"; */
/* run; */
/* proc sql; */
/* create table a as */
/* select claim_id, policyno, membership_number */
/* from inp_CL.final_claims_data_new_&vmy */
/* where claim_id in (523466, */
/* 523584, */
/* 538876, */
/* 514747); */
/* quit; */
/*  */
/* proc export data=a */
/*                            outfile="&filepath\TPA_pol_chk" */
/*                           dbms=xlsx replace; */
/*                            sheet="TPA_CL"; */
/* run; */
/* proc import datafile="&filepath\TPA_CL_PY" */
/* out=output.TPA_CL_PY */
/* DBMS=xlsx */
/* replace; */
/* sheet="Mapping"; */
/* ; */
/* run; */
/* PROC SORT DATA=output.TPA_CL_PY nodupkey; */
/* BY FINAL_CLAIM_ID; */
/*  */
/* PROC SORT DATA=OS_&PVMY; */
/* BY FINAL_CLAIM_ID; */
/* data OS1_&PVMY; */
/* merge OS_&PVMY (in=x) output.TPA_CL_PY (in=y); */
/* by final_claim_id; */
/* if x=1; */
/* run; */
data OS2_&PVMY;
set OS_&PVMY;
if maximus_cl_id ne . then claim_id=maximus_cl_id;
if maximus_preauthid ne . then pre_auth_id=maximus_preauthid;
run;
data OS3_&PVMY;
set OS2_&PVMY;
drop maximus_cl_id maximus_preauthid;
run;

data OS_1_&PVMY;
set OS3_&PVMY;

Incurredyear= year(incurreddate);
Incurredyearmo= cats(Incurredyear,put(month(incurreddate),z2.));
Incurredyearqtr= cats(Incurredyear,'Q',qtr(incurreddate));
Reportyear= year(reportdate);
Reportyearmo= cats(Reportyear,put(month(reportdate),z2.));
Reportyearqtr= cats(Reportyear,'Q',qtr(reportdate));
claim_id_n= compress(claim_id);
preauthID_n= compress(pre_auth_ID);
format key$30.;
if preauthID_n='' or preauthID_n='.'   then key= claim_id_n;
else key=cats(preauthID_n,'P');
if index(final_claim_id,'G') gt 0 then key=final_claim_id;
if key='' or key='.' OR KEY='.P' then key=final_claim_id;
run;
data status_last_yr;
set OS_1_&PVMY (keep=key final_status_used);
if final_status_used= 'OUTSTANDING' then status_last_yr='OUTSTANDING';
else status_last_yr='PAID';
run;
proc sort data=status_last_yr nodupkey;
by key;
proc sort data=output.paid_os_1;
by key;

data status_last_yr_merge;
merge output.paid_os_1 (in=x) status_last_yr(in=y drop=final_status_used);
by key;
if x=1;
run;

/*8. Creating FY and status to be used */
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
'01apr2021'D - '31Mar2022'D   =      '2021_22'
'01apr2022'D - '31Mar2023'D   =      '2022_23';
run;
data status_claim;
set status_last_yr_merge;
format status_to_be_used $50.;
if status_last_yr="" then status_last_yr="NA";
if status_last_yr="OUTSTANDING" then status_to_be_used="OPENING";
else if status_last_yr="PAID" then status_to_be_used="PAID";
else if status_last_yr="NA" and (incurreddate ge &yrstdate or reportdate ge &yrstdate) then status_to_be_used="NEW INTIMATION";
else status_to_be_used="REOPENED";
Occurrence_FY = put(incurreddate, financial_year.);
run;
data output.cl_wo_ct_adj;
set status_claim;
run;

/*9. Count adjustment */
/* Adjustment1: Wherever GIC is 0 in data count was 0, so we are correcting it to be 1
 for all main claims 
 Adjustment 2 : If key is getting repeated count should be populated against only 1 row*/
proc sql;
create table gic_key as
select key, sum(claimcount) as ct_key, sum(GrossClaimAmt) as gic_key
from status_claim
group by key
order by key;
quit;
proc sort data=status_claim;
by key;
proc sort data=gic_key;
by key;
data count_adj;
merge status_claim (in=x) gic_key (in=y);
by key;
if x=1;
run;
data count_adj2;
set count_adj;
if claim_type='Main Claim' and gic_key=0 and ct_key=0
then claimcount=1;
run;

/* data count_adj3; */
/* set count_adj2; */
/* by key descending claimcount; */
/* if first.key =0 then claimcount=0;run; */
/*  */
/* proc sort data=count_adj3 ; */
/* by key; */
/* run; */

/* Manual adjustment count and amount- to be checked if it has to be done for
 coming years */
/*  */
/* data count_adj4; */
/* set count_adj3; */
/* if product='GoActive' then do; */
/* if key in ('340GHC', */
/* '520GHC', */
/* '558GHC', */
/* '559GHC' */
/* ) then do; */
/* grossclaimamt= 600; */
/* netclaimamt= 360; */
/* end; */
/*  */
/* if key in ('200GHC', */
/* '24GHC', */
/* '534GHC' */
/* ) then do; */
/* grossclaimamt= 775; */
/* netclaimamt= 465; */
/* end; */
/* end; */
/* run; */
/*  */
/* Manual count mapping */
/* proc sort data=count_adj4; */
/* by key product finalstatus; */
/*  */
/* proc import datafile="&filepath\Manual_CT_CY" */
/* out=output.ct_adj_manual */
/* DBMS=xlsx */
/* replace; */
/* sheet="sheet1"; */
/* ; */
/* run; */
/*  */
/* proc sort data=output.ct_adj_manual nodupkey; */
/* by key finalstatus; */
/*  */
/* data ct_merge_manual; */
/* merge count_adj3 (in=x) output.ct_adj_manual (in=y); */
/* if x=1; */
/* by key finalstatus; */
/* run; */
/* data ct_merge_manual2; */
/* set ct_merge_manual; */
/* if count_manual ne "" and count_manual ne . then claimcount=count_manual; */
/* run; */

/* PROC SQL; */
/* 	CREATE TABLE WORK.QUERY_FOR_CT_MERGE_MANUAL23 AS */
/* 		SELECT */
/* 			t1.finalstatus, */
/* 			t1.'key'n, */
/* 			t1.count_manual, */
/* 			t1.claimcount */
/* 		FROM */
/* 			WORK.CT_MERGE_MANUAL2 t1 */
/* 		WHERE */
/* 			UPPER(t1.'key'n) = '496195' */
/* 	; */
/* QUIT; */
/* RUN; */
/*  */
/* proc sort data=count_adj2; */
/* by key descending claimcount;; */
/* run; */

/*  */
/* data count_adj_f; */
/* set count_adj2 ; */
/* if first.key =0  then claimcount=0;run; */

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
'01apr2021'D - '31Mar2022'D   =      '2021_22'
'01apr2022'D - '31Mar2023'D   =      '2022_23';

run;



data output.paid_os_&vmy;
set count_adj2;
drop ct_key gic_key count_manual
;
if GrossClaimAmt gt 500000 then indicator_large_cl='Y' ;else indicator_large_cl='N';
Reported_FY = put(reportdate, financial_year.);
Paid_FY= put(paiddate, financial_year.);
run;
data OUTPUT.paid_os_&vmy;
set output.paid_os_&vmy;

if key='2000001603' then do;
product='MHP_Indem';
lob='Group  - Other Schemes';
end;

if key='513030' then do;
product='GPA_NEE';
lob='Group Personal Accident';
end;
if key='KCP192RM' then do; LOB='Health Insurance - Individual'; Product='Corona Kavach'; incurreddate='08-Oct-20'; reportdate='27-Jan-21'; Occurrence_FY='2020_21';end;
if key in ('TPACKP423_1','TPACKP424_1') then do; LOB='Group Health  - Employer/Employee'; Product='CK Group';end;
if key ='TPACKP423_1' then do; incurreddate='09-Mar-21'; reportdate='21-Jun-21'; Occurrence_FY='2020_21';end;
if key ='TPACKP424_1' then do; incurreddate='12-May-21'; reportdate='29-May-21'; Occurrence_FY='2021_22';end;



run;
/*  */
/* Manual Adj */
/* DATA output.paid_os_&vmy; */
/* SET output.paid_os_&vmy; */
/* IF PRODUCT='MHP_PA' THEN DO; */
/* PRODUCT='MHP_Indem'; */
/* LOB='Group  - Other Schemes'; */
/* END; */
/* RUN; */

proc export data=output.paid_os_&vmy
                           outfile="&filepath/Data"
                          dbms=xlsx replace;
                           sheet="Mar'22 - Mar'23";
run;

