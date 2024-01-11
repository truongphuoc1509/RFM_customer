
-- join 2 bảng customer_registered và customer_transaction
with data as (
    select t.CustomerID as id,
           r.created_date,
           r.stopdate,
           count(distinct t.Transaction_ID) as number_order,
           sum(t.GMV) as total_GMV,
           max(t.Purchase_Date) as latest_time

    from customer_registered r
    right join customer_transaction t
    on r.id = t.CustomerID
    where r.created_date is not null
    and r.created_date < date('2022-09-01')
    and r.stopdate < date('2022-09-01')
    group by 1,2
    order by 2 desc
            )
 -- tính tuổi hợp đồng của khách nếu họ đã ngừng sử dụng, hoặc còn sử dụng
,trans as (
select id,
        datediff(date(latest_time),date('2022-09-01') ) as Recency,
        CASE when stopdate is null then datediff(date('2022-09-01'), date(created_date))
             else datediff(date(stopdate), date(created_date))
        end as age,
        number_order,
        total_GMV as Monetary
from data
         )

, RFM as (
    select id,
           Recency,
           number_order / age as Frequency,
           Monetary/ age as Monetary
    from trans
           )


-- chia thành 5 phần cho từng yếu tố
, Ranks as (
SELECT
    id,
    NTILE(5) OVER (ORDER BY Recency) AS Recency_Group,
    NTILE(5) OVER (ORDER BY Frequency) AS Frequency_Group,
    NTILE(5) OVER (ORDER BY Monetary) AS Monetary_Group
FROM
    RFM)


, RFM_scores as (
SELECT
    *,
    CONCAT(Recency_Group, Frequency_Group, Monetary_Group) AS RFM_scores
FROM
    Ranks)


-- mapping label segment
, result as (select id,
       RFM_scores,
       CASE
           WHEN RFM_scores between 111 and 155 THEN 'Risky'
           WHEN RFM_scores between 211 and 255 THEN 'Hold and improve'
           WHEN RFM_scores between 311 and 353 THEN 'Potential loyal'
           WHEN RFM_scores between 354 and 454 or RFM_scores between 511 and 535 or RFM_scores = 541
               THEN 'Loyal'
           WHEN RFM_scores =  455 or RFM_scores between 542 and 555 THEN 'Star'
           ELSE 'dont know'
       END as Segment

from RFM_scores)

-- count id cho từng segment
# SELECT Segment, count(*) as number_customers
# FROM result
# group by  Segment

-- đếm id theo từng location, branch và segment
, b as  (select r.id,
       RFM_scores,
       Segment,
       cr.LocationID,
       cr.BranchCode
from result r
left join customer_registered cr
on  r.id = cr.ID)

, c as (select b.id, RFM_scores,Segment, l.LocationNameVN, l.BranchFullName
       from b
left join location l
on  b.LocationID = l.LocationID
and b.BranchCode= l.BranchCode)


-- count user theo segment và location
# SELECT LocationNameVN,
#        SUM(CASE WHEN Segment = 'Risky' THEN number_id ELSE 0 END) AS Risky,
#        SUM(CASE WHEN Segment = 'Hold and improve' THEN number_id ELSE 0 END) AS HoldAndImprove,
#        SUM(CASE WHEN Segment = 'Potential loyal' THEN number_id ELSE 0 END) AS PotentialLoyal,
#        SUM(CASE WHEN Segment = 'Loyal' THEN number_id ELSE 0 END) AS Loyal,
#        SUM(CASE WHEN Segment = 'Star' THEN number_id ELSE 0 END) AS Star
# FROM (
#     SELECT LocationNameVN, Segment, COUNT(id) AS number_id
#     FROM c
#     GROUP BY LocationNameVN, Segment
# ) AS SourceTable
# GROUP BY LocationNameVN;


-- đếm user theo segment và branch
    SELECT BranchFullName,
       SUM(CASE WHEN Segment = 'Risky' THEN number_id ELSE 0 END) AS Risky,
       SUM(CASE WHEN Segment = 'Hold and improve' THEN number_id ELSE 0 END) AS HoldAndImprove,
       SUM(CASE WHEN Segment = 'Potential loyal' THEN number_id ELSE 0 END) AS PotentialLoyal,
       SUM(CASE WHEN Segment = 'Loyal' THEN number_id ELSE 0 END) AS Loyal,
       SUM(CASE WHEN Segment = 'Star' THEN number_id ELSE 0 END) AS Star
FROM (
    SELECT BranchFullName, Segment, COUNT(id) AS number_id
    FROM c
    GROUP BY BranchFullName, Segment
) AS SourceTable
GROUP BY BranchFullName;



# Tính Retention

with retention as (
    select CustomerID as id,
           DATE_FORMAT(Purchase_Date, '%Y-%m-01') as mon,
           min(DATE_FORMAT(Purchase_Date, '%Y-%m-01')) over (partition by CustomerID) as first_order,
           sum(GMV) as GMV
    from customer_transaction
    group by 1,2
    order by mon)

, rate as (select mon,
                  id,
                  first_order,
                  TIMESTAMPDIFF(MONTH, mon, LEAD(mon) OVER (PARTITION BY id ORDER BY mon)) AS duration,
                  TIMESTAMPDIFF(MONTH, LAG(mon) OVER (PARTITION BY id ORDER BY mon), mon) AS dura_lag,
                  GMV
           from retention)

, metrics as (
    select
        mon,
           -- Tôổng Số khách hàng của mỗi tháng
        count(distinct id) as active_user,
            -- Số khách hàng được giữ lại từ tháng trước
        count(distinct case when duration = 1 then id end) as retained_user,
            -- Số khách hàng phục hồi
        count(case when dura_lag > 1  then id end) as recover_user,
            -- GMV tháng trước của những khách hàng được giữ lại
        sum(case when duration = 1 then GMV end) as retained_GMV,
            -- GMV tháng này từ những khách hàng được giữ chân từ tháng trước
        sum(case when dura_lag = 1 then GMV end) as GMV_from_retain_user,
            -- GMV tháng này từ những khách hàng phục hồi
        sum(case when dura_lag > 1 then GMV end) as GMV_from_recover_user,
            -- số khách hàng bị mất của tháng truớc
        count(distinct case when duration > 1 or duration is null then id end) as lost_user,
            -- số GMV bị mất từ tháng trước của khach lost
        sum(case when duration > 1 or duration is null then GMV end) as lost_GMV,
            -- Số khách hàng mới của tháng này
        count(distinct case when first_order = mon then id end) as new_user,
            -- GMV cua tháng này từ khách hàng mới
        sum(case when first_order = mon then GMV end) as new_GMV,
            -- Tổng gmv  của tháng này
        sum(GMV) as GMV
    from rate
    group by 1
    )

, structe as (
    select
        date_format(mon,'%m-%y') as month,
        active_user,
        new_user,
        recover_user,
        GMV,
        new_GMV,
        GMV_from_retain_user,
        GMV_from_recover_user

    from metrics
)

, a as (select date_format(date_add(mon, interval 1 month), '%m-%y') as month,
       retained_user,
       retained_user/active_user as retention_user_rate,
       lost_user/active_user as lost_user_rate,
       retained_GMV/GMV as retention_GMS_rate,
       lost_GMV/GMV as lost_GMS_rate

from  metrics)

select s.month, s.active_user,  s.new_user, s.recover_user, a.retained_user, a.retention_user_rate, a.lost_user_rate,
       s.GMV, s.GMV_from_retain_user,s.new_GMV, s.GMV_from_recover_user, a.retention_GMS_rate, a.lost_GMS_rate

from a
right join structe s
on a.month = s.month



# tỉ lệ đăng kí/hủy

# with register as (select DATE_FORMAT(created_date, '%Y-%m') as month,
#        count(distinct ID) as number_register
# from customer_registered
# where created_date < date('2022-09-01')
# group by  1)
#
# , cancel as ( select DATE_FORMAT(stopdate, '%Y-%m') as month,
#        count(distinct ID) as number_cancel
# from customer_registered
# group by  1)
#
# select r.month,
#        number_cancel/number_register as stable_rate
#     from register r
#     inner join  cancel c
#     on r.month = c.month
# order by  month
#
