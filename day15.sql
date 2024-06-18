--ex1--
SELECT extract (year from transaction_date) as yr, product_id, 
spend as curr_year_spend,
lag(spend) over(partition by product_id order by product_id,transaction_date) as prev_year_spend,
round(100*(spend-lag(spend) over(partition by product_id order by product_id,transaction_date))/lag(spend) over(order by product_id,transaction_date),2) as yoy_rate
FROM user_transactions
--ex2--
with cte1 as 
(SELECT card_name, min((make_date(issue_year,issue_month,1))) as launch_date
FROM monthly_cards_issued
group by card_name)
select a.card_name, a.issued_amount
from monthly_cards_issued as a   
join cte1 as b   
on a.card_name=b.card_name and
make_date(a.issue_year,a.issue_month,1)=b.launch_date
order by a.issued_amount desc
--ex3--
with cte as (SELECT user_id, spend, transaction_date, row_number() over(partition by user_id order by transaction_date) 
FROM transactions)
select user_id, spend, transaction_date
from cte
where row_number=3
--ex4--
with cte as (SELECT user_id,product_id,
rank() over(partition by user_id order by transaction_date desc) as stt,
transaction_date
FROM user_transactions)
select transaction_date,user_id, count(product_id) as purchase_count
from cte 
where stt=1
group by user_id, transaction_date
order by transaction_date
