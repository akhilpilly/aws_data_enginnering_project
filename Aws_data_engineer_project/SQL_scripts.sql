
#################################### emp table_ creation query in_ RDS database (source) #######################################################

CREATE TABLE emp (
empid int not null primary key,
empname varchar(50) not null,
emptitle varchar(50) not null,
empsalary int,
empdeptno int not null);

#################################### query for_ inserting data_ into_ created emp table_ in_ RDS database (source) ##############################

insert into emp values (1,'rob smith','developer', 10000, 1);
insert into emp values (2,'john doe','analyst', 7000, 2);
insert into emp values (3,'mark james','manager', 15000, 1);
insert into emp values (4,'clark ford','associate', 9000, 2);
insert into emp values (5,'allen cooper','senior engineer', 12000, 1);
insert into emp values (6,'mike turner','manager', 17000, 2);
insert into emp values (7,'blake scott','associate', 9000, 3);
insert into emp values (8,'james martin','manager', 14000, 3);

#################################### def table_ creation query in_ RDS database (source) ########################################################
create table dept (
deptno int not null primary key,
deptname varchar(50)
);

#################################### query for_ inserting data_ into_ created def table_ in_ RDS database (source) ##############################
insert into dept values (1,'data engineering');
insert into dept values (2,'sales');
insert into dept values (3,'marketing');



#################################### final external_ table_ creation query in_ redshift (destination) ###########################################
create external table schema1.reporting_layer_table(
employee_name varchar,
employee_title varchar,
employee_saalry bigint,
department_name varchar
)
row format delimited
fields terminated by ','
stored as textfile
location 's3://mainbucketemr/reporting_data/emp_transfromed_joined_dept_output/' 
table properties ('skip.header.line.count'='1');

#################################### Displaying the final table_ ###############################################################################

select * from dev.schema1.reporting_layer_table;