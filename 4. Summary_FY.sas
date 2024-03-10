
Libname output "/sasdata/Acturial/Reserving/Reporting/Annual Reports/FY 23/Data";
%Let vmy=0323;
/* 4. Previous valuation Month year*/
%Let pvmy= 0322;

options compress=yes;
/* 1. Start date of FY */
%Let yrstdate= mdy(04,01,2022);

/* 5. Output file location*/
%Let filepath=/sasdata/Acturial/Reserving/Reporting/Annual Reports/FY 23/Data;

/*Appending paid/os and rejected data for the quarter*/
data output.data_summary;
set output.paid_os_&vmy output.denied_cl_&vmy;run;

/*Creating required columns*/
data data_summary;
set output.data_summary;

array change _numeric_;
do over change;
if change=. then change=0;
end;

/* Payment_on_claims_finally_settled_during_the_reporting_period_No. */
if UPCASE(finalstatus) ='PAID' and paiddate ge &yrstdate then do;
payment_on_cl_settled_amt=Netclaimamt; 
payment_on_cl_settled_ct=claimcount;
end;
else do;
payment_on_cl_settled_amt=0; 
payment_on_cl_settled_ct=0;
end;

/*Claims_provided_for_the_first_time_during_the_reporting_period_No.*/
if UPCASE(status_to_be_used)='NEW INTIMATION' then do;
New_int_cl_amt=Netclaimamt+Netbilledamt_rej; 
New_int_cl_ct=claimcount;
end;
else do;
New_int_cl_amt=0; 
New_int_cl_ct=0;
end;


/*Claims_reopened_during_the_reporting_period_No.*/
if UPCASE(status_to_be_used)='REOPENED' then do;
Reopened_cl_amt=Netclaimamt+Netbilledamt_rej; 
Reopened_cl_ct=claimcount;
end;
else do;
Reopened_cl_amt=0; 
Reopened_cl_ct=0;
end;

/* Provision_at_the_end_of_the_reporting_period_Outstanding_Claims_No. */
if finalstatus='Outstanding' then do;
Provision_yrend_amt=Netclaimamt; 
Provision_yrend_ct=claimcount;
end;
else do;
Provision_yrend_amt=0; 
Provision_yrend_ct=0;
end;
format file_flag $20.;
file_flag='Current_year';
run;

/*os previous Yearr*/
data opening_prev_yr;
set output.cl_final_&pvmy;

format file_flag $20.;
file_flag='Previous_year';
run;

data data_summary2;
set data_summary  opening_prev_yr;
run;

data OUTPUT.data_summary3;
set data_summary2;

/* Claims_closed_without_payment_during_the_reporting_period_No. */
if upcase(finalstatus)='DENIED' and UPCASE(status_to_be_used) ne 'OPENING' then do;
Rej_cl_amt=Netbilledamt_rej; 
Rej_cl_ct=claimcount;
end;
else do;
Rej_cl_amt=0; 
Rej_cl_ct=0;
end;
if file_flag='Previous_year' and upcase(status_current_yr)="" and key ne "" then do;
Rej_cl_amt=Netclaimamt; 
Rej_cl_ct=claimcount;
end;
/*Provision_at_the_beginning*/
if file_flag='Previous_year' and finalstatus='Outstanding' then do;
Provision_opening_amt=Netclaimamt; 
Provision_opening_ct=claimcount;
end;
else do;
Provision_opening_amt=0; 
Provision_opening_ct=0;
end;

run;
proc import datafile="&FILEPATH/IBNR_0323"
out=output.IBNR_&vmy
DBMS=xlsx
replace;
sheet="Sheet1";
;
run;

/*IBNR Reserves*/
/*Current quarter*/
data ibnr_&VMY;
set output.ibnr_&VMY;
keep occurrence_FY LOB Reserve file_flag;
rename reserve= closing_reserve;
file_flag = 'IBNR_Closing';
run;
/*Previous quarter*/
data ibnr_&PVMY;
set output.IBNR_0322;
keep occurrence_FY LOB Reserve file_flag;
rename reserve= opening_reserve;
file_flag = 'IBNR_Opening';
run;

/*Appending IBNR*/
data output.data_summary4;
set output.data_summary3 ibnr_&vmy ibnr_&pvmy;
if closing_reserve=. then closing_reserve=0;
if opening_reserve=. then opening_reserve=0;
run;


/*LOB wise summary*/

proc sql;
create table Retail as
select Occurrence_FY,
sum(Provision_opening_ct) as Provision_opening_ct,
sum(Provision_opening_amt) as Provision_opening_amt,

sum(opening_reserve) as opening_reserve,
0 as opening_reserve_IBNER,
0 as part_payment_ct,
0 as part_payment_amt,

sum(payment_on_cl_settled_ct) as payment_on_cl_settled_ct,
sum(payment_on_cl_settled_amt) as payment_on_cl_settled_amt,

sum(New_int_cl_ct) as New_int_cl_ct,
sum(New_int_cl_amt) as New_int_cl_amt,

sum(Reopened_cl_ct) as Reopened_cl_ct,
sum(Reopened_cl_amt) as Reopened_cl_amt,

sum(Rej_cl_ct) as Rej_cl_ct,
sum(Rej_cl_amt) as Rej_cl_amt,

sum(Provision_yrend_ct) as Provision_yrend_ct,
sum(Provision_yrend_amt) as Provision_yrend_amt,

sum(closing_reserve) as closing_reserve,
0 as closing_reserve_IBNER

from output.data_summary4
where LOB="Health Insurance - Individual"
group by Occurrence_FY
order by  Occurrence_FY desc  ;
quit;

data retail;
set retail;
array change _numeric_;
do over change;
if change=. then change=0;
end;
recon_ct=sum(payment_on_cl_settled_ct,Rej_cl_ct,Provision_yrend_ct,-1*New_int_cl_ct,-1*Reopened_cl_ct,-1*Provision_opening_ct);
recon_amt=sum(payment_on_cl_settled_amt,Rej_cl_amt,Provision_yrend_amt,-1*New_int_cl_amt,-1*Reopened_cl_amt,-1*Provision_opening_amt) ;
run;

proc export data=Retail
                           outfile="&filepath/Summary_SAS"
                    dbms=xlsx                             REPLACE;    sheet="Health Insurance - Individual";
run;


proc sql;
create table B2B as
select Occurrence_FY,
sum(Provision_opening_ct) as Provision_opening_ct,
sum(Provision_opening_amt) as Provision_opening_amt,

sum(opening_reserve) as opening_reserve,
0 as opening_reserve_IBNER,
0 as part_payment_ct,
0 as part_payment_amt,

sum(payment_on_cl_settled_ct) as payment_on_cl_settled_ct,
sum(payment_on_cl_settled_amt) as payment_on_cl_settled_amt,

sum(New_int_cl_ct) as New_int_cl_ct,
sum(New_int_cl_amt) as New_int_cl_amt,

sum(Reopened_cl_ct) as Reopened_cl_ct,
sum(Reopened_cl_amt) as Reopened_cl_amt,

sum(Rej_cl_ct) as Rej_cl_ct,
sum(Rej_cl_amt) as Rej_cl_amt,

sum(Provision_yrend_ct) as Provision_yrend_ct,
sum(Provision_yrend_amt) as Provision_yrend_amt,

sum(closing_reserve) as closing_reserve,
0 as closing_reserve_IBNER


from output.data_summary4
where LOB="Group Health  - Employer/Employee"
group by Occurrence_FY
order by  Occurrence_FY desc  ;
quit;

data B2B;
set B2B;
array change _numeric_;
do over change;
if change=. then change=0;
end;
recon_ct=sum(payment_on_cl_settled_ct,Rej_cl_ct,Provision_yrend_ct,-1*New_int_cl_ct,-1*Reopened_cl_ct,-1*Provision_opening_ct);
recon_amt=sum(payment_on_cl_settled_amt,Rej_cl_amt,Provision_yrend_amt,-1*New_int_cl_amt,-1*Reopened_cl_amt,-1*Provision_opening_amt) ;
run;

proc export data=B2B
                           outfile="&filepath/Summary_SAS"
                                                 dbms=xlsx REPLACE;    sheet="GroupHealth-Employer/Employee";
run;



proc sql;
create table B2O as
select Occurrence_FY,
sum(Provision_opening_ct) as Provision_opening_ct,
sum(Provision_opening_amt) as Provision_opening_amt,

sum(opening_reserve) as opening_reserve,
0 as opening_reserve_IBNER,
0 as part_payment_ct,
0 as part_payment_amt,

sum(payment_on_cl_settled_ct) as payment_on_cl_settled_ct,
sum(payment_on_cl_settled_amt) as payment_on_cl_settled_amt,

sum(New_int_cl_ct) as New_int_cl_ct,
sum(New_int_cl_amt) as New_int_cl_amt,

sum(Reopened_cl_ct) as Reopened_cl_ct,
sum(Reopened_cl_amt) as Reopened_cl_amt,

sum(Rej_cl_ct) as Rej_cl_ct,
sum(Rej_cl_amt) as Rej_cl_amt,

sum(Provision_yrend_ct) as Provision_yrend_ct,
sum(Provision_yrend_amt) as Provision_yrend_amt,

sum(closing_reserve) as closing_reserve,
0 as closing_reserve_IBNER


from output.data_summary4
where LOB="Group  - Other Schemes"
group by Occurrence_FY
order by  Occurrence_FY desc  ;
quit;
data B2O;
set B2O;
array change _numeric_;
do over change;
if change=. then change=0;
end;
recon_ct=sum(payment_on_cl_settled_ct,Rej_cl_ct,Provision_yrend_ct,-1*New_int_cl_ct,-1*Reopened_cl_ct,-1*Provision_opening_ct);
recon_amt=sum(payment_on_cl_settled_amt,Rej_cl_amt,Provision_yrend_amt,-1*New_int_cl_amt,-1*Reopened_cl_amt,-1*Provision_opening_amt) ;
run;

proc export data=B2O
                           outfile="&filepath/Summary_SAS"
                               dbms=xlsx                  REPLACE;    sheet="Group  - Other Schemes";
run;

proc sql;
create table GIPA as
select Occurrence_FY,
sum(Provision_opening_ct) as Provision_opening_ct,
sum(Provision_opening_amt) as Provision_opening_amt,

sum(opening_reserve) as opening_reserve,
0 as opening_reserve_IBNER,
0 as part_payment_ct,
0 as part_payment_amt,

sum(payment_on_cl_settled_ct) as payment_on_cl_settled_ct,
sum(payment_on_cl_settled_amt) as payment_on_cl_settled_amt,

sum(New_int_cl_ct) as New_int_cl_ct,
sum(New_int_cl_amt) as New_int_cl_amt,

sum(Reopened_cl_ct) as Reopened_cl_ct,
sum(Reopened_cl_amt) as Reopened_cl_amt,

sum(Rej_cl_ct) as Rej_cl_ct,
sum(Rej_cl_amt) as Rej_cl_amt,

sum(Provision_yrend_ct) as Provision_yrend_ct,
sum(Provision_yrend_amt) as Provision_yrend_amt,

sum(closing_reserve) as closing_reserve,
0 as closing_reserve_IBNER

from output.data_summary4
where LOB="Group Personal Accident"
group by Occurrence_FY
order by  Occurrence_FY desc  ;
quit;
data GPA;
set GPA;
array change _numeric_;
do over change;
if change=. then change=0;
end;
recon_ct=sum(payment_on_cl_settled_ct,Rej_cl_ct,Provision_yrend_ct,-1*New_int_cl_ct,-1*Reopened_cl_ct,-1*Provision_opening_ct);
recon_amt=sum(payment_on_cl_settled_amt,Rej_cl_amt,Provision_yrend_amt,-1*New_int_cl_amt,-1*Reopened_cl_amt,-1*Provision_opening_amt) ;
run;


proc export data=GPA
                           outfile="&filepath/Summary_SAS"
                                       dbms=xlsx          REPLACE;    sheet="Group Personal Accident";
run;


