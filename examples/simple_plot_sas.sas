/* 1. Import Excel file */
proc import datafile="sample_data.xlsx"
    out=mydata
    dbms=xlsx
    replace;
    sheet="Sheet1";
    getnames=yes;
run;

/* 2. View imported data */
proc print data=mydata;
run;

/* 3. Create a simple line plot */
proc sgplot data=mydata;
    series x=Month y=Sales / markers;
    xaxis label="Month";
    yaxis label="Sales";
    title "Monthly Sales Plot";
run;