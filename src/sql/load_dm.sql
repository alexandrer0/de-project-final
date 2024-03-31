select drop_partitions('stv202312114__dwh.global_metrics', '{calc_date}'::date-1, '{calc_date}'::date-1);
insert into stv202312114__dwh.global_metrics (
	date_update,
	currency_from,
	amount_total,
	cnt_transactions,
	avg_transactions_per_account,
	cnt_accounts_make_transactions
)
with all_trns as (
	select
		operation_id,
		currency_code,
		account_number_from,
		amount
	from
		stv202312114__staging.transactions
	where
		transaction_dt::date = '{calc_date}'::date-1
		and account_number_from >= 0 and account_number_to >= 0
        and status = 'done'
	),
curr_usd as (
	select
		currency_code_with,
		currency_with_div
	from
		stv202312114__staging.currencies
	where
		date_update = '{calc_date}'::date-1
		and currency_code_with = 420
	)
select
	'{calc_date}'::date-1 as date_update,
	all_trns.currency_code as currency_from,
	sum(all_trns.amount * coalesce(curr_usd.currency_with_div, 1)) as usd_amount,
	sum(all_trns.amount) as cnt_transactions,
	count(distinct account_number_from) as cnt_accounts_make_transactions,
	round(sum(all_trns.amount) / count(distinct account_number_from), 2) avg_transactions_per_account
from all_trns
	left join curr_usd on all_trns.currency_code = curr_usd.currency_code_with
group by 1,2