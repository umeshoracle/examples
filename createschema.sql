create or replace  sequence emp_seq start with 10001 ;

create or replace hybrid table dept(  
  deptno     number,  
  dname      varchar2,  
  loc        varchar2,  
  constraint pk_dept primary key (deptno)  
);


create or replace  hybrid table emp(  
  empno    number(12),  
  ename    varchar,  
  job      varchar,  
  mgr      number(12),  
  hiredate date,  
  sal      number ,  
  comm     number default sal*0.25,  
  deptno   number,  
  constraint pk_emp primary key (ename),  
  constraint fk_deptno foreign key (deptno) references dept (deptno)  ,
  index ename_idx (empno)
);

desc table emp;

-- Insert row into DEPT table using named columns.
insert into DEPT (DEPTNO, DNAME, LOC)
values(10, 'ACCOUNTING', 'NEW YORK');

-- Insert a row into DEPT table by column position.
insert into dept  
values(20, 'RESEARCH', 'DALLAS');

insert into dept  
values(30, 'SALES', 'CHICAGO');

insert into dept  
values(40, 'OPERATIONS', 'BOSTON');


select * from dept;

-- Insert EMP row, using TO_DATE function to cast string literal into an oracle DATE format.
insert into emp  
values(  
 7839, 'KING', 'PRESIDENT', null,  
 to_date('17-11-1981','dd-mm-yyyy'),  
 5000, null, 10  
);

insert into emp  
values(  
 7698, 'BLAKE', 'MANAGER', 7839,  
 to_date('1-5-1981','dd-mm-yyyy'),  
 2850, null, 30  
);

insert into emp  
values(  
 7782, 'CLARK', 'MANAGER', 7839,  
 to_date('9-6-1981','dd-mm-yyyy'),  
 2450, null, 10  
);

insert into emp  
values(  
 7566, 'JONES', 'MANAGER', 7839,  
 to_date('2-4-1981','dd-mm-yyyy'),  
 2975, null, 20  
);

insert into emp  
values(  
 7788, 'SCOTT', 'ANALYST', 7566,  
 to_date('13-07-1987','dd-mm-yyyy') ,  
 3000, null, 20  
);

insert into emp  
values(  
 7902, 'FORD', 'ANALYST', 7566,  
 to_date('3-12-1981','dd-mm-yyyy'),  
 3000, null, 20  
);

insert into emp  
values(  
 7369, 'SMITH', 'CLERK', 7902,  
 to_date('17-12-1980','dd-mm-yyyy'),  
 800, null, 20  
);

insert into emp  
values(  
 7499, 'ALLEN', 'SALESMAN', 7698,  
 to_date('20-2-1981','dd-mm-yyyy'),  
 1600, 300, 30  
);

insert into emp  
values(  
 7521, 'WARD', 'SALESMAN', 7698,  
 to_date('22-2-1981','dd-mm-yyyy'),  
 1250, 500, 30  
);

insert into emp  
values(  
 7654, 'MARTIN', 'SALESMAN', 7698,  
 to_date('28-9-1981','dd-mm-yyyy'),  
 1250, 1400, 30  
);

insert into emp  
values(  
 7844, 'TURNER', 'SALESMAN', 7698,  
 to_date('8-9-1981','dd-mm-yyyy'),  
 1500, 0, 30  
);

insert into emp  
values(  
 7876, 'ADAMS', 'CLERK', 7788,  
 to_date('13-07-87', 'dd-mm-yy') - 51,  
 1100, null, 20  
);

insert into emp  
values(  
 7900, 'JAMES', 'CLERK', 7698,  
 to_date('3-12-1981','dd-mm-yyyy'),  
 950, null, 30  
);

insert into emp  
values(  
 7984, 'VIKAS2', 'CLERK', 7782,  
 to_date('23-1-1982','dd-mm-yyyy'),  
 1300, null, 10  
);


update emp set mgr=0 where mgr is null;
update emp set comm=0 where comm is null;

show tables;
----

-- Simple natural join between DEPT and EMP tables based on the primary key of the DEPT table DEPTNO, and the DEPTNO foreign key in the EMP table.
select ename, dname, job, empno, hiredate, loc  
from emp, dept  
where emp.deptno = dept.deptno  
order by ename;

-- The GROUP BY clause in the SQL statement allows aggregate functions of non grouped columns.  The join is an inner join thus departments with no employees are not displayed.
select dname, count(*) count_of_employees
from dept, emp
where dept.deptno = emp.deptno
group by DNAME
order by 2 desc;




update emp 
set sal = sal*1.07
where deptno=20;

select * from emp;

delete from emp where empno=7934;

create or replace  hybrid table emp2(  
  empno    number(4,0),  
  ename    varchar2(10),  
  job      varchar2(9),  
  mgr      number(4,0),  
  id1 number,
  id2 number,
  id3 number,
  id4 number, 
  id5 number,
  hiredate date,  
  sal      number(7,2),  
  comm     number(7,2) default sal*0.25,  
  deptno   number,  
  constraint pk_emp primary key (empno),  
  constraint fk_deptno foreign key (deptno) references dept (deptno)  ,
  index empname_idx (ename ),
  index job_idx (job),
  index mgr_idx (mgr),
    index hd_idx (hiredate),
      index sal_idx (sal),
  index comm_idx (comm),
  index dept_idx(deptno),
  index id1x (id1),
    index id2x (id2),
    index id3x (id3),
    index id4x (id4),
    index id5x (id5)
  
);

show tables;

select ename,empno  from  emp;



update demo.emp set ename='RAJU' where empno=10012;
commit;

insert into emp (empno, ename, job, mgr, hiredate, sal, comm, deptno) values (emp_seq.nextval, 'Umesh', 'Patel' , 7369, to_date('15-April-2022') , 10000, null, 10);

update emp set mgr=0 where mgr is null;
update emp set comm=0 where comm is null;
insert into emp (empno, ename, job, mgr, hiredate, sal, deptno) values (emp_seq.nextval, 'UMESH', 'SE' , 7876, to_date('15-April-2022') , 69500, 10);


update emp set empno=10001 where empno=1;


----

create or replace hybrid TABLE sfo_accounts (
	ID VARCHAR(18),
	PARENTID VARCHAR(18),
	BILLINGCITY VARCHAR(40),
	WEBSITE VARCHAR(255),
	BILLINGLATITUDE NUMBER(18,15),
	NUMBEROFEMPLOYEES NUMBER(8,0),
	ACCOUNTNUMBER VARCHAR(40),
	NAME VARCHAR(255) unique,
	SHIPPINGSTREET VARCHAR(255),
	TYPE VARCHAR(255),
	BILLINGCOUNTRY VARCHAR(80),
	BILLINGSTREET VARCHAR(255),
	BILLINGLONGITUDE NUMBER(18,15),
	PHONE VARCHAR(40),
	SHIPPINGPOSTALCODE VARCHAR(20),
	SHIPPINGCOUNTRY VARCHAR(80),
	BILLINGPOSTALCODE VARCHAR(20),
	SHIPPINGCITY VARCHAR(40),
	SHIPPINGSTATE VARCHAR(80),
	FAX VARCHAR(40),
	SEGMENT__C VARCHAR(25),
	ANNUALREVENUE NUMBER(18,0),
	BILLINGSTATE VARCHAR(80),
	ACTIVE__C VARCHAR(255)
    ,
     constraint pk_actno primary key (id)  
);


create or replace TABLE sfo_opps (
	ID VARCHAR(18),
	LASTMODIFIEDDATE TIMESTAMP_NTZ(9),
	ACCOUNTID VARCHAR(18),
	HASOPPORTUNITYLINEITEM BOOLEAN,
	STAGENAME VARCHAR(255),
	AMOUNT NUMBER(18,2),
	FORECASTCATEGORY VARCHAR(40),
	ISCLOSED BOOLEAN,
	CLOSEDATE DATE,
	NAME VARCHAR(120),
	TYPE VARCHAR(255),
	CREATEDBYID VARCHAR(18),
	LEADSOURCE VARCHAR(255),
	OWNERID VARCHAR(18),
	ISWON BOOLEAN,
	OPPORTUNITY_SOURCE__C VARCHAR(25),
	CREATEDDATE TIMESTAMP_NTZ(9),
	LASTACTIVITYDATE DATE,
	RECORDTYPEID VARCHAR(18),
	FORECASTCATEGORYNAME VARCHAR(255),
    constraint pk_oppid primary key (id),
     constraint fk_acctid foreign key (accountid) references sfo_accounts (id)  
);


create or replace TABLE sfo_cases (
	ID VARCHAR(18),
	ORIGIN VARCHAR(255),
	STATUS VARCHAR(255),
	LASTMODIFIEDDATE TIMESTAMP_NTZ(9),
	ACCOUNTID VARCHAR(18),
	CONTACTID VARCHAR(18),
	ISCLOSED BOOLEAN,
	SUBJECT VARCHAR(255),
	TYPE VARCHAR(255),
	ISESCALATED BOOLEAN,
	OWNERID VARCHAR(18),
	CREATEDDATE TIMESTAMP_NTZ(9),
	CASENUMBER VARCHAR(30),
	CLOSEDDATE TIMESTAMP_NTZ(9),
     constraint fk_acctid_csae foreign key (accountid) references sfo_accounts (id)  
);
