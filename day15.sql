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
--ex5--
SELECT user_id, tweet_date,
round(avg(tweet_count) over (partition by user_id	order by tweet_date rows BETWEEN 2 preceding and current row),2) as rolling_avg_3d
FROM tweets
--ex6--
with cte as(SELECT merchant_id, credit_card_id, amount,
transaction_timestamp,
(extract(epoch from transaction_timestamp- lag(transaction_timestamp) over(partition by merchant_id,credit_card_id, amount)))/60 as min_dif
FROM transactions)
select count(*) from cte as payment_count
where min_dif<=10
--ex7--
with cte as (select category, product, sum(spend) as total_spend,
rank() over (partition by category order by sum(spend) desc) as rank
from product_spend
where extract (year from transaction_date)=2022
group by category, product)
select category, product, total_spend
from cte  
where rank in (1,2)
--ex8--
with cte as(SELECT  a.artist_name, count(*),
dense_rank() over(order by (count(*)) desc)
FROM artists as a
left join songs as b
on a.artist_id=b.artist_id
left join global_song_rank as c  
on b.song_id=c.song_id
where rank<=10
group by a.artist_name)
select cte.artist_name, cte.dense_rank as artist_rank
from cte
where dense_rank<=5
