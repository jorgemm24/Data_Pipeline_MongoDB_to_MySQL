-- database
--create database afto;

-- Creando la tabla <accounts>
create table accounts
(
id varchar(50),
account_id int,
limite int,
products varchar(100),
n_products smallint,
product_1 varchar(30),
product_2 varchar(30),
product_3 varchar(30),
product_4 varchar(30),
product_5 varchar(30)
);


-- Creando la tabla <customers>
create table customers(
id varchar(50),
username varchar(50),
name nvarchar(50) ,
address nvarchar(100),
birthdate datetime,
email nvarchar(50),
accounts varchar(100),
n_accounts smallint
)


-- Creando la tabla <transactions_details>
create table transactions_details(
account_id int,
date date,
amount int,
transaction_code varchar(20),
symbol varchar(20),
price decimal(10,3),
total decimal(10,3)
);


-- Creando la tabla <transactions>
create table transactions(
id varchar(50),
account_id int,
transaction_count smallint,
bucket_start_date date,
bucket_end_date date
)


/* 


-- querys accounts
select * from accounts limit 50;

select a.*, length(id)  from accounts a where account_id ="627788";

select count(1) from accounts;  # 1746
#truncate table accounts;

select count(distinct account_id) from accounts ; # 1745


select * from (
select  row_number() over(partition by account_id order by  account_id) as  row_account_id, a.*
from accounts a ) table_a
where row_account_id>1

----------------------------------------------------------------------------------------------------
-- querys customers

select * from customers limit 20;
select count(1) from customers;
#truncate table customers;


----------------------------------------------------------------------------------------------------
-- querys transactions
select * from transactions limit 100;
select count(1) from transactions;

#truncate table transactions;

----------------------------------------------------------------------------------------------------
-- querys transactions_details
select * from transactions_details order by account_id limit 100;
select * from transactions_details where account_id = 557378 order by date limit 100;

select count(1) from transactions_details;
select count(1) from transactions_details where account_id = 557378;

#truncate table transactions_details;


-- para el BI
select sum(n_trasanctions)  from (
select account_id, date, transaction_code, symbol, sum(amount) as amount,
		sum(price) as price, sum(total) as total, sum(1) n_trasanctions
from afto.transactions_details
group by account_id, date, transaction_code, symbol
) T

*/


