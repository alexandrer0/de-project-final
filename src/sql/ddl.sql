drop table if exists stv202312114__staging.currencies;
drop table if exists stv202312114__staging.transactions;
drop table if exists stv202312114__dwh.global_metrics;

create table if not exists stv202312114__staging.currencies (
	date_update timestamp(0) not null,
	currency_code int not null,
	currency_code_with int not null,
	currency_with_div numeric(5, 3) not null
)
order by date_update
partition by date_update::date;

create table stv202312114__staging.transactions (
    operation_id varchar(64) not null,
	account_number_from int not null,
	account_number_to int not null,
	currency_code int not null,
	country varchar(32) not null,
	status varchar(32) not null,
	transaction_type varchar(32) not null,
	amount int not null,
	transaction_dt timestamp(0) not null
)
order by transaction_dt
segmented by hash(operation_id,transaction_dt) all nodes
partition by transaction_dt::date;

create table stv202312114__dwh.global_metrics(
    date_update date not null,
    currency_from int not null,
    amount_total  numeric(18,2) not null,
    cnt_transactions int not null,
    avg_transactions_per_account numeric(18,2) not null,
	cnt_accounts_make_transactions int not null
)
order by date_update
partition by date_update::date;