/* A. Assigning libraries and defining macro variables  */

options compress=yes;

/* Input File Path */
Libname inp_CL "/sasdata/Acturial/Reserving/Mar 23";
Libname inp_CL2 "/sasdata/Acturial/Reserving/Claims Data";

libname inp_pr "/dataarchive/MAR";


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


/* Summary:This data can only be prepared using raw trackers as other files
           have only paid and/or OS data*/

/*1. Creating min of date of admission and report date using claim unique ID  */
DATA CLAIMS_TOTAL;
SET Inp_CL2.CLAIMS_TOTAL_&vmy (encoding=any);
IF PRE_AUTH_ID NE '.' THEN PREAUTH_U = strip(PRE_AUTH_ID) || 'P'; ELSE PREAUTH_U = strip(PRE_AUTH_ID); 
RUN; 
DATA CLAIMS_TOTAL_F;
SET CLAIMS_TOTAL;
IF CLAIM_ID NE '.' THEN CLAIM_UNIQ_ID = strip(CLAIM_ID); ELSE CLAIM_UNIQ_ID = STRIP(PREAUTH_U);
RUN;
PROC SORT DATA=CLAIMS_TOTAL_F;
BY CLAIM_UNIQ_ID;
RUN;




/*DOA_reserving for Preauth*/
data CLAIMS_TOTAL_Preauth;
set claims_total_f;
format doa_reserving_1 date9.;
if FILE_FLAG_O = "preauth";
doa_reserving_1 = min (Date_of_Admission,Claim_Reported_Date);
format reportdate date9.;
reportdate=claim_reported_date;
run;

PROC SQL;
   CREATE TABLE WORK.QUERY_FOR_CLAIMS_TOTAL_PREAUTH AS 
   SELECT DISTINCT t1.CLAIM_UNIQ_ID, 
          t1.doa_reserving_1,
		t1.reportdate
      FROM WORK.CLAIMS_TOTAL_PREAUTH t1;
QUIT;
proc sort data=QUERY_FOR_CLAIMS_TOTAL_PREAUTH;
by claim_uniq_id;
run;






/*DOA_reserving for Claim Tracker*/
data CLAIMS_TOTAL_CT;
set claims_total_f;
format doa_reserving_1 date9.;
if FILE_FLAG_O = "claim_t";
doa_reserving_1 = min (Date_of_Admission,Claim_Reported_Date);
format reportdate date9.;
reportdate=claim_reported_date;
run;
PROC SQL;
   CREATE TABLE WORK.QUERY_FOR_CLAIMS_TOTAL_CT AS 
   SELECT DISTINCT t1.CLAIM_UNIQ_ID, 
          t1.doa_reserving_1,
t1.reportdate
      FROM WORK.CLAIMS_TOTAL_CT t1;
QUIT;
proc sort data=QUERY_FOR_CLAIMS_TOTAL_CT;
by claim_uniq_id;
run;



/*Appending the two DOA_reserving mapping files (CT and Preauth)*/
data QUERY_FOR_CLAIMS_TOTAL_x;
set QUERY_FOR_CLAIMS_TOTAL_PREAUTH QUERY_FOR_CLAIMS_TOTAL_CT;
run;
/*In case of claim attached to preauth, pre auth dates will be retained 
by keeping pre auth dates file before claims date file in set statement in above code*/
proc sort data=QUERY_FOR_CLAIMS_TOTAL_x nodupkey;
by claim_uniq_id;
run;
proc sort data=CLAIMS_TOTAL_F;
by claim_uniq_id;
run;
data claim_total_x;
merge CLAIMS_TOTAL_F (in=x) QUERY_FOR_CLAIMS_TOTAL_x (in=y);
by claim_uniq_id;
if x=1;
run;
data claims_total_f;
set claim_total_x;
run;
PROC SORT DATA=CLAIMS_TOTAL_F;
BY claim_uniq_id;
run;


/* Dates adjustment 
The dates and other fields against the pre-auth claim and the corresponding claim are different. 
Accordingly, the claim tracker date (except doa_reserving) created above and cols are taken. 
Preauth entries with claims against them are also removed in the following step
*/ 
data dates_adj_1;
set work.claims_total_f;
count+1;
by claim_uniq_id;
if first.claim_uniq_id=1 then count=1;
else count=0;
run;
data dates_adjustment_1;
set dates_adj_1;
if count=1;
/*keep claim_uniq_id  Date_of_Admission Date_of_discharge Claim_Reported_Date;*/
/*rename Date_of_Admission=MIN_of_Date_of_Admission;*/
rename Date_of_discharge=MIN_of_Date_of_discharge;
rename Claim_Reported_Date=MIN_of_Claim_Reported_Date;
run;

/*Since duplicates removed in above step, below steps of taking min against claim_uni_id are redundant*/
PROC SQL;
   CREATE TABLE WORK.dates_adjustment_r AS 
   SELECT t1.CLAIM_UNIQ_ID, 
                           (MIN(t1.doa_reserving_1)) FORMAT=DATE9. AS doa_reserving
      FROM WORK.CLAIMS_TOTAL_F t1
      GROUP BY t1.CLAIM_UNIQ_ID;
QUIT;
data CLAIMS_TOTAL_F_3;
merge dates_adjustment_1 (in=a) dates_adjustment_r (in=b);
by claim_uniq_id;
if a=1;
drop doa_reserving_1;
run;

/*1. Creating final claim id and claim type */
data CLAIMS_TOTAL_F_3;
set CLAIMS_TOTAL_F_3;
format MIN_of_Date_of_Admission date9.;
MIN_of_Date_of_Admission=doa_reserving;

FORMAT FINAL_CLAIM_ID $CHAR20.;
IF FILE_FLAG_O NE 'preauth'
         THEN IF MAIN_CLAIM_NUMBER = '' 
                  THEN FINAL_CLAIM_ID = STRIP(CLAIM_ID); 
         ELSE FINAL_CLAIM_ID = STRIP(MAIN_CLAIM_NUMBER);
ELSE FINAL_CLAIM_ID = CLAIM_UNIQ_ID;

if FINAL_CLAIM_ID = "." then FINAL_CLAIM_ID = CLAIM_UNIQ_ID;

format Claim_Type_Final $50.;
if pre_auth_id ne  . then Claim_Type_Final='Direct Settlement' ;
else if (PRE_AUTH_ID = . and claim_id=.) and
claim_type in ('Cashless',
'Direct Settlement  Main',
'Direct Settlement Inpatient Main',
'Direct Settlement OPD Main') then Claim_Type_Final='Direct Settlement'; 
else Claim_Type_Final='Reimbursement';

run;
PROC SORT DATA=CLAIMS_TOTAL_F_3;
BY CLAIM_UNIQ_ID;
RUN;

/*3. correcting main claim id in data , populating claim type of main claim and count */
proc sql;
create table a as
select distinct claim_id as Main_Claim_Number, Main_Claim_Number as Main_Claim_Number_corr
from CLAIMS_TOTAL_F_3
having Main_Claim_Number_corr ne .;
quit;
proc sort data=a nodupkey;
by Main_Claim_Number;
proc sort data=CLAIMS_TOTAL_F_3;
by Main_Claim_Number;
data mergechk;
merge CLAIMS_TOTAL_F_3 (in=x) a (in=y);
by Main_Claim_Number;
if x=1 ;run;
data incorrect_main_claim;
set mergechk;
if Main_Claim_Number_corr ne . and Main_Claim_Number_corr ne Main_Claim_Number;
run;
data main_cl_corr;
set mergechk;
final_claim_id_old= final_claim_id;
run;
data main_cl_corr;
set main_cl_corr;
if Main_Claim_Number_corr ne . and Main_Claim_Number_corr ne Main_Claim_Number
then final_claim_id=input(Main_Claim_Number_corr,$20.);
run;
PROC SORT DATA=main_cl_corr;
BY FINAL_CLAIM_ID ;
RUN;
DATA main_cl_corr_1;
SET main_cl_corr;

IF CLAIM_ID = '.' THEN DO; FINAL_CLAIM_ID_1=PRE_AUTH_ID; TAG='P'; END; ELSE FINAL_CLAIM_ID_1=CLAIM_ID; 
IF TAG='P' THEN FINAL_CLAIM_ID_2=CATS(FINAL_CLAIM_ID_1,'P'); ELSE FINAL_CLAIM_ID_2=CATS(FINAL_CLAIM_ID_1);


IF FINAL_CLAIM_ID=FINAL_CLAIM_ID_2 THEN COUNT_CLAIM =1; ELSE COUNT_CLAIM = 0;
RUN;






/*Claim_type main claim*/
proc sql;
create table a as
select distinct final_claim_id 
from main_cl_corr_1 
where claim_type_final=  'Direct Settlement' and final_claim_id ne ''; quit;
data a;
set a;
format claim_type_main_cl $20.;
claim_type_main_cl='Direct Settlement';
run;
proc sort data=a nodupkey;
by final_claim_id;
run;
proc sort data=main_cl_corr_1;
by final_claim_id;
run;
data final;
merge main_cl_corr_1 (in=x) a (in=y);
by final_claim_id;
if x=1;
run;
data final;
set final;
if claim_type_main_cl='' and index(upcase(final_claim_id),'P') gt 0 then claim_type_main_cl='Direct Settlement';
else if claim_type_main_cl='' and index(upcase(final_claim_id),'P') eq 0 then claim_type_main_cl='Reimbursement';
run;

proc sql;
create table a1 as
select distinct file_flag_o, final_status
from final;
quit;
/* 4. Filtering out denied claims */
data output.denied_claims_&vmy;
set final;
where upcase(interim_Status)='DENIED';

format claim_id_1 $30. ;
claim_id_1=compress(claim_id);

if claim_type_main_cl='Direct Settlement' then paymentmode='DS';
else paymentmode='RM';

run;

/* 5. Creating incurredyearmo */
data b;
set output.denied_claims_&vmy;
drop claim_id;
rename claim_id_1 = claim_id;
status='DENIED';
incurredyearmo=cats(year(MIN_of_Date_of_Admission),put(month(MIN_of_Date_of_Admission),z2.));
run;
/*6. Mapping LOB prod*/
data c;
set b;
if substr(policyno,1,1)ne '5' then policyno= substr(policyno,2,15);
run;
data c_1;
set c;
where substr(policyno,1,1)='5';
run;

data c_2;
set c;
where substr(policyno,1,1) ne '5';
run;
proc sql;
create table d as
select  distinct policyno, product 
from inp_pr.premium_file_0323_with_emi
order by policyno;
quit;
proc sort data=c_2;
by policyno;
proc sort data=d nodupkey;
by policyno;
data e;
merge c_2 (in=x) d (in=y);
if x=1;
by policyno;run;
data chk;
merge c_2 (in=x) d (in=y);
if x=1 and y=0;
by policyno;run;
/* data e; */
/* set e; */
/* if product="" and substr(policyno,1,1)='3' */
/* then do; */
/* product='Heartbeat'; */
/* LOB_INT='B2C'; */
/* end; */
/* run; */

data c_1_1;
set c_1;
if (index(BENEFIT,"A") gt 0 or index(BENEFIT,"C") gt 0 or index(BENEFIT,"D")gt 0)then do;
Product = "MHP_Indem";
end;

else if  index(BENEFIT,"S") gt 0 then do;
Product = "MHP_CI";
end;


else if  index(BENEFIT,"B") gt 0 then do;
Product = "MHP_Named";
end;

else if index(BENEFIT,"P") gt 0 then do;
Product = "MHP_PA";
end;

else do;
Product = "MHP_Indem";
end; 
run;

data e_2;
set e c_1_1 ;
run;
proc sql;
create table d as
select  distinct policyno, LOB_INT
from inp_pr.premium_file_0323_with_emi
order by policyno;
quit;
PROC sort data=d nodupkey;
by policyno;
PROC sort data=e_2 ;
by policyno;
data f;
merge e_2 (in=x) d (in=y);
if x=1;
by policyno;
run;
data g;
set f;
if substr(policyno,1,1)='5' and 
lob_int ne 'B2B' and
product='MHP_PA'
THEN LOB_INT='GPA' ;

if substr(policyno,1,1)='5' and 
lob_int ne 'B2B' and
product ne 'MHP_PA'
THEN LOB_INT='B2O' ;
run;

data h;
set g;

format lob $40.;
if LOB_INT='B2C' then LOB='Health Insurance - Individual';
else if LOB_INT='B2B' then LOB='Group Health  - Employer/Employee';
else if LOB_INT='B2O' then LOB='Group  - Other Schemes';
else if LOB_INT='GPA' then LOB='Group Personal Accident';
run;

data output.denied_claim_&vmy;
set h; run;


/*  Denied during the year*/
/*1. Maximus */
data denied_cl_m;
set output.denied_claim_&vmy;
where Date_of_Claim_intimation ge &YRSTDATE ;   /*where claimdecisiondate ge &YRSTDATE ;*/
format reportyearmo $6.;
reportyearmo=CATS(year(reportdate),put(month(reportdate),z2.));
reportyear=year(reportdate);
incurredyear=year(Date_of_Admission);

if paymentmode='Direct Settlement' then paymentmode='DS';
if paymentmode='Reimbursement' then paymentmode='RM';

format LOB $50.;
if LOB_INT='B2C' then LOB='Health Insurance - Individual';
else if LOB_INT='B2B' then LOB='Group Health  - Employer/Employee';
else if LOB_INT='B2O' then LOB='Group  - Other Schemes';
else if LOB_INT='GPA' then LOB='Group Personal Accident';

format system_flag $20.;
system_flag='Maximus';

format uni_mmbr_key $30.;
uni_mmbr_key=cats(policyno,membership_number);

format key $30.;
if pre_auth_id=. then key=claim_id;
else key=cats(pre_auth_id,"P");
run;

/*Reqd cols maximus*/
proc sql;
create table denied_cl_m1 as
select 
key,
claim_id ,
Pre_Auth_ID ,
Date_of_Admission as incurreddate,
reportdate,
Incurredyearmo,
reportyearmo,
status as finalstatus,
LOB,
Paymentmode ,
PRODUCT,
POLICYNO,
MEMBERSHIP_NUMBER,
uni_mmbr_key,
system_flag,
sum(COUNT_CLAIM) as claimcount,
sum(billed_amount) as Billedamount
from denied_cl_m
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;
quit;

/*2. Phoenix*/

/*Denied ITD*/
data denied_claims_p_itd;
set output.denied_claim_&vmy;
where substr(policyno,1,1)='5' ;
format reportyearmo$6.;
reportyearmo=CATS(year(reportdate),put(month(reportdate),z2.));
reportyear=year(reportdate);
incurredyear=year(Date_of_Admission);
if paymentmode='Direct Settlement' then paymentmode='DS';
if paymentmode='Reimbursement' then paymentmode='RM';

format LOB $50.;
if LOB_INT='B2C' then LOB='Health Insurance - Individual';
else if LOB_INT='B2B' then LOB='Group Health  - Employer/Employee';
else if LOB_INT='B2O' then LOB='Group  - Other Schemes';
else if LOB_INT='GPA' then LOB='Group Personal Accident';

FORMAT UNI_MMBR_KEY $30.;
if product = "MHP_Indem" then UNI_MMBR_KEY =CATS(POLICYNO,Membership_number,"_I");
if product = "MHP_CI" then UNI_MMBR_KEY = CATS(POLICYNO,Membership_number,"_C");
if product = "MHP_Named" then UNI_MMBR_KEY = CATS(POLICYNO,Membership_number,"_N");
if product = "MHP_PA" then UNI_MMBR_KEY = CATS(POLICYNO,Membership_number,"_P");

format system_flag $20.;
system_flag='Phoenix';

format key $30.;
if pre_auth_id=. then key=claim_id;
else key=cats(pre_auth_id,"P");
run;



/*Denied till last qtr*/

proc sql;
create table denied_cl_lastyr as
select distinct claim_id as ClaimId,
Pre_Auth_ID as PreAuthID
from output.denied_claim_&pvmy
where substr(policyno,1,1)='5';
quit;
data denied_cl_lastyr;
set denied_cl_lastyr;
format key $30.;
if preauthid=. then key=claimid;
else key=cats(preauthid,"P");
run;
/*change in denied during the year*/
proc sort data=denied_claims_p_itd;
by key;
proc sort data=denied_cl_lastyr nodupkey;
by key;

data inc_denied;
merge denied_claims_p_itd (in=x) denied_cl_lastyr (in=y keep=key);
if x=1 and y=0;
BY KEY;
run;
/*Reqd cols Phoenix*/
proc sql;
create table denied_cl_p1 as
select 
key,
claim_id ,
Pre_Auth_ID ,
Date_of_Admission as incurreddate,
reportdate,
Incurredyearmo,
reportyearmo,
status as finalstatus,
LOB,
Paymentmode ,
PRODUCT,
POLICYNO,
MEMBERSHIP_NUMBER,
uni_mmbr_key,
system_flag,
sum(COUNT_CLAIM) as claimcount,
sum(billed_amount) as Billedamount
from inc_denied
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;
quit;

/* VAS */
/* data vas_denied; */
/* set output.vas_rejected_cl_&vmy; */
/* where datepart(Date_of_Decision) ge &YRSTDATE; */
/* format uni_mmbr_key $30.; */
/* uni_mmbr_key=cats(policyno,membership_number); */
/*  */
/* product='GoActive'; */
/* LOB='Health Insurance - Individual'; */
/*  */
/* if pre_auth_id=. then paymentmode='RM'; else paymentmode='DS'; */
/*  */
/* format key $30.; */
/* key=final_claim_id; */
/* format incurreddate date9.; */
/* incurreddate=MIN_of_Date_of_Admission; */
/* format reportdate date9.; */
/* reportdate=MIN_of_Claim_Reported_Date; */
/* format reportyearmo$6.; */
/* reportyearmo=CATS(year(reportdate),put(month(reportdate),z2.)); */
/* format incurredyearmo$6.; */
/* incurredyearmo=CATS(year(incurreddate),put(month(incurreddate),z2.)); */
/*  */
/* reportyear=year(reportdate); */
/* incurredyear=year(incurreddate); */
/*  */
/* finalstatus='Denied'; */
/* system_flag='VAS'; */
/* count_claim=1; */
/* format claim_ID_N $30.; */
/* claim_ID_N= COMPRESS(claim_id); */
/* drop claim_id; */
/* rename claim_id_n= claim_id; */
/* run; */
/* proc sql; */
/* create table denied_cl_v1 as */
/* select  */
/* key, */
/* claim_id , */
/* Pre_Auth_ID , */
/* incurreddate, */
/* reportdate, */
/* Incurredyearmo, */
/* reportyearmo, */
/* finalstatus, */
/* LOB, */
/* Paymentmode , */
/* PRODUCT, */
/* POLICYNO, */
/* MEMBERSHIP_NUMBER, */
/* uni_mmbr_key, */
/* system_flag, */
/* sum(COUNT_CLAIM) as claimcount, */
/* sum(billed_amount) as Billedamount */
/* from vas_denied */
/* group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15; */
/* quit; */
/*  */

data output.denied_cl_interim_&vmy;
set denied_cl_m1 denied_cl_p1;
run;
/*RCD Mapping to calculate net amount*/
proc sort data=output.denied_cl_interim_&vmy;
by uni_mmbr_key;
data rcd_info;
set inp_pr.premium_file_0323_with_emi (keep=uni_mmbr_key p_st_date rcd);
run;
proc sort data=rcd_info nodupkey;
by uni_mmbr_key;
data output.denied_cl_interim2_&vmy;
merge output.denied_cl_interim_&vmy (in=x) rcd_info(in=y);
by uni_mmbr_key;
if x=1;
run;
proc sql;
create table set1 as
select * from output.denied_cl_interim2_&vmy
where rcd ne .;
quit;
proc sql;
create table set2 as
select * from output.denied_cl_interim2_&vmy
where rcd = .;
quit;
data set2;
set set2;
drop rcd;
run;
proc import datafile="&filepath/RCD_Mapping"
out=output.RCD_Mapping
DBMS=xlsx
replace;
sheet="Sheet1";
;
run;
data output.RCD_Mapping;
set output.RCD_Mapping;
rename FINAL_CLAIM_ID=key;
run;

proc sort data=set2;
by key;
proc sort data=output.RCD_Mapping nodupkey;
by key;
data set3;
merge set2 (in=x) output.RCD_Mapping (in=y);
by key;
if x=1;
run;
data set4;
set set1 set3;
run;



data output.denied_cl_interim2_&vmy;
set set4;


IF MONTH(incurreddate)>3 THEN OCC_YEAR = substr(strip(YEAR(incurreddate)),3,2)||'-'||substr((strip(YEAR(incurreddate)+1)),3,2); 
ELSE OCC_YEAR = substr(strip(YEAR(incurreddate)-1),3,2)||'-'||substr((strip(YEAR(incurreddate))),3,2);

if RCD>='1Apr2010'd and RCD<='31Mar2013'd then Obg_Fac=.1;
else if RCD>='1Apr2013'd and RCD<='31Mar2015'd then Obg_Fac=.05;
else if RCD>='1Apr2015'd and RCD<='31Mar2017'd then Obg_Fac=0.05;
else if RCD>='1Apr2017'd and RCD<='31Mar2022'd then Obg_Fac=.05;
else if RCD>='1Apr2022'd and RCD<='31Mar2025'd then Obg_Fac=.04;
else Obg_Fac=0.04;

if (product = "Health Companion" or product = "GCI" or product = "GHI" or product = "GHS" or product = "MHP_Indem" 
or product = "MHP_Named" or product = "Health Recharge" or product = "ReAssure" or product = "Health Pulse" or product = "Health Premia" or product = "Heartbeat")
and occ_year in ('17-18' , '18-19') then vqs_fac=0.20; 

else if (product = "Health Companion" or product = "GCI" or product = "GHI" or product = "GHS" or product = "MHP_Indem" 
or product = "MHP_Named" or product = "Health Recharge" or product = "ReAssure" or product = "Health Pulse" or product = "Health Premia" or product = "Heartbeat")
and RCD ge mdy(04,01,2019) 
and RCD lt mdy(04,01,2020) then vqs_fac=0.20; 

else if (product = "Health Companion" or product = "GCI" or product = "GHI" or product = "GHS" or product = "MHP_Indem" 
or product = "GoActive" or product= "Health Multiplier"
or product = "MHP_Named" or product = "Health Recharge" or product = "ReAssure" or
 product = "Health Pulse" or product = "Health Premia" or product = "Heartbeat" or product = "Standard Product" 
or product = "GCC" 
or product = "HEALTH MULTIPLIE" 
or product = "MHP_CI"
or product = "RA2.0"
or product = "SENIOR FIRST" 
or product = "SH_INDEM" 
or product = "TRAVEL"
or product = "XH_INDEM"
or product = "ELIXIR")
and RCD ge mdy(04,01,2020) then vqs_fac=0.20;
else vqs_fac=0;

if LOB IN ('Group Health  - Employer/Employee','Group Personal Accident') then vqs_fac=0;

if product = 'GoActive' and p_st_date GE '01Apr2018'd and RCD lt mdy(04,01,2020) 
then net_amount= billedamount*0.65-obg_fac*billedamount;
else net_amount= billedamount-obg_fac*billedamount-vqs_fac*billedamount;

run;
/*Missing RCD*/
data chk;
merge output.denied_cl_interim_&vmy (in=x) rcd_INFO (in=y);
by uni_mmbr_key;
if x=1 and y=0;
run;
proc export data=chk
                           outfile="&filepath/Denied_rcd_chk"
                     dbms=xlsx      replace;
                           sheet="chk";
run;

proc sql;
create table a as
select distinct membershipno, policyno, uni_mmbr_key, rcd,p_st_date
from inp_pr.premium_file_0323_with_emi
where membershipno in ('1440865',
'3391287',
'3448583',
'5599097');quit;

proc export data=a
                           outfile="&filepath/rcd_chk"
                     dbms=xlsx      replace;
                           sheet="chk";
run;

proc sql;
create table a as
select distinct membershipno, policyno, uni_mmbr_key, rcd,p_st_date
from inp_pr.premium_file_0323_with_emi
where POLICYNO IN ('00232600202002',
'00246700201800',
'50073100202000');QUIT;
proc export data=a
                           outfile="&filepath/POL_chk"
                     dbms=xlsx      replace;
                           sheet="chk";
run;



/*  */
/* Data Manual adjustment- count & amt */
/*  */
/* data set1; */
/* set output.denied_cl_interim2_&vmy; */
/* run; */
/* proc sort data=set1 nodupkey; */
/* by claim_id; */
/*  */
/* Importing TPA previous year  */
/* proc import datafile="&filepath/Manual Count Adj Rejected" */
/* out=output.ct_adj_manual_denied */
/* DBMS=xlsx */
/* replace; */
/* sheet="Sheet1"; */
/* ; */
/* run; */
/* proc sort data=output.ct_adj_manual_denied nodupkey; */
/* by claim_id; */
/*  */
/*  */
/* Importing TPA previous year  */
/* proc import datafile="&filepath/Manual Amt Adj Rejected" */
/* out=output.Amt_adj_manual_denied */
/* DBMS=xlsx */
/* replace; */
/* sheet="Sheet1"; */
/* ; */
/* run; */
/* proc sort data=output.Amt_adj_manual_denied nodupkey; */
/* by claim_id; */
/*  */
/*  */
/* PROC SQL; */
/* 	CREATE TABLE WORK.QUERY_FOR_DENIED_CLAIM_0320 AS */
/* 		SELECT */
/* * */
/* 	FROM */
/* 			set2 t1 */
/* 		WHERE */
/* 			UPPER(t1.claim_id) = '212909' */
/* 	; */
/* QUIT; */
/* RUN; */
/* data set2; */
/* merge set1 (in=x) output.ct_adj_manual_denied (in=y) output.Amt_adj_manual_denied(in=z); */
/* by claim_id; */
/* if x=1; */
/* run; */
/* data set3; */
/* set set2; */
/* if  count_manual ne "" then claimcount=count_manual; */
/* if  netamt_manual ne "" then net_amount=netamt_manual; */
/* run; */
/* data set4; */
/* set set3; */
/* if claim_id in ('286571','298286','334550','497486','511169') then delete; */
/* run; */
/* proc export data=set4 */
/*                            outfile="&filepath/Data" */
/*                      dbms=xlsx      replace; */
/*                            sheet="Rejected_current_yr"; */
/* run; */
/*  */
/* data output.den_f; */
/* set set4; */
/* drop Obg_Fac */
/* RCD */
/* count_manual */
/* netamt_manual */
/* p_st_date */
/* system_flag */
/* vqs_fac */
/* ; */
/* run; */
/*  */
/* Denied TPA */
/*  */
/*  Append TPA Paid data during the year */
/*  */
/* Importing TPA previous year  */
/* proc import datafile="&filepath/TPA_Working" */
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
/* proc import datafile="&filepath/TPA_Working" */
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
/* Rejected  during the period */
/* data TPA_Rej; */
/* merge TPA_PrevYr (in=x) TPA_Paid_cy (in=y); */
/* by final_claim_id; */
/* if x=1 and y=0; */
/* run; */
/*  */
/* data TPA_Rej2; */
/* set TPA_Rej; */
/* RENAME KEY=UNI_MMBR_KEY; */
/* drop  MEMBER_NUMBER; */
/* rename Final_claim_count=claimcount; */
/* rename MIN_of_Date_of_Admission= incurreddate; */
/* rename netclaimamt= net_amount; */
/*  */
/* if claim_type='Cashless' then paymentmode='DS'; else paymentmode='RM'; */
/* rename MIN_of_Claim_Reported_Date=reportdate; */
/* finalstatus='DENIED'; */
/*  */
/*  */
/* run; */
/*  */
/*  */
/* Renaming as per paid data */
/* data TPA_Rej_f; */
/* set TPA_Rej2; */
/* key=final_claim_id; */
/* drop claim_type PAIDDATE grossclaimamt */
/* netclaim_wo_vqs */
/* ; */
/*  */
/* IF MONTH(incurreddate)>3 THEN OCC_YEAR = substr(strip(YEAR(incurreddate)),3,2)||'-'||substr((strip(YEAR(incurreddate)+1)),3,2);  */
/* ELSE OCC_YEAR = substr(strip(YEAR(incurreddate)-1),3,2)||'-'||substr((strip(YEAR(incurreddate))),3,2); */
/*  */
/* run; */
/*  */
/* data append_set; */
/* set output.den_f TPA_Rej_f ; */
/* finalstatus='Denied'; */
/* incurredyearmo=cats(year(incurreddate),put(month(incurreddate),z2.)); */
/* reportyearmo=CATS(year(reportdate),put(month(reportdate),z2.)); */
/* reportyear=year(reportdate); */
/* run; */

data prev_yr_cl;
set output.cl_final_&pvmy;
keep key finalstatus;
rename finalstatus=status_last_yr;
run;
proc sort data=prev_yr_cl nodupkey;
by key;

proc sort data=output.denied_cl_interim2_&vmy ;
by key;


data status_last_yr_merge;
merge output.denied_cl_interim2_&vmy (in=x) prev_yr_cl(in=y);
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
'01apr2021'D - '31Mar2022'D   =      '2021_22'
'01apr2022'D - '31Mar2023'D   =      '2022_23';
run;
/*Status to be used*/
data status_claim;
set status_last_yr_merge;
format status_to_be_used $50.;
if status_last_yr="" then status_last_yr="NA";
if status_last_yr="Outstanding" then status_to_be_used="Opening";
else if status_last_yr="NA" and (incurreddate ge &YRSTDATE or reportdate ge &YRSTDATE) then status_to_be_used="New Intimation";
else status_to_be_used="Reopened";
Occurrence_FY = put(incurreddate, financial_year.);
run;

/*Denied claims final*/
data output.denied_cl_&vmy;
set status_claim;
rename net_amount= Netbilledamt_rej;
run;

/*  Manual Adj */
data output.denied_cl_&vmy;
set output.denied_cl_&vmy;
if KEY IN ('186422P'
,'2000001325'
,'2000001531',
'263954P',
'265294P',
'520101',
'520102') then claimcount=1;
run;

data output.denied_cl_&vmy;
set output.denied_cl_&vmy;
if key='526652' and Membership_number=5599097
then do;
PolicyNo='00237700202002';
PRODUCT='GHI';
LOB='Group  - Other Schemes';
uni_mmbr_key='002377002020025599097';
end;
run;

proc export data=output.denied_cl_&vmy
                           outfile="&filepath/Data_rejected"
                     dbms=xlsx      replace;
                           sheet="Rejected_current_yr";
run;
