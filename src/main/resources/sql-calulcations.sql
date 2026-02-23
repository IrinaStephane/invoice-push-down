-- Q1
select invoice_id, customer_name, sum(quantity * unit_price) as total_amount, status
from invoice_line
         join invoice i on i.id = invoice_line.invoice_id
group by invoice_id, customer_name, status;

-- Q2
select invoice_id, customer_name, sum(quantity * unit_price) as total_amount, status
from invoice_line
         join invoice i on i.id = invoice_line.invoice_id
where status = 'CONFIRMED'
   or status = 'PAID'
group by invoice_id, customer_name, status;

-- Q3
SELECT SUM(CASE WHEN i.status = 'PAID' THEN invoice_line.quantity * invoice_line.unit_price ELSE 0 END) AS total_paid,
       SUM(CASE
               WHEN i.status = 'CONFIRMED' THEN invoice_line.quantity * invoice_line.unit_price
               ELSE 0 END)                                                                              AS total_confirmed,
       SUM(CASE
               WHEN i.status = 'DRAFT' THEN invoice_line.quantity * invoice_line.unit_price
               ELSE 0 END)                                                                              AS total_draft
FROM invoice_line
         JOIN invoice i ON invoice_line.invoice_id = i.id;
-- Q4
select (percent_paid + percent_confirmed + percent_draft) as revenue_percent
from (select sum(case when i.status = 'PAID' THEN il.quantity * il.unit_price * 1.0 else 0 end) as percent_paid,
             sum(case
                     when i.status = 'CONFIRMED' THEN il.quantity * il.unit_price * 0.5
                     else 0 end)                                                                as percent_confirmed,
             sum(case when i.status = 'DRAFT' THEN il.quantity * il.unit_price * 0 else 0 end)  as percent_draft
      from invoice i
               join invoice_line il on i.id = il.invoice_id) revenue_percentages;
-- Q5-A
select invoice_id,
       (ht + ht * tax_config.rate / 100) as ttc
from (select invoice_id, sum(quantity * unit_price) as ht
      from invoice_line
      group by invoice_id) invoice_ht,
     tax_config;

select (percent_paid + percent_confirmed + percent_draft) as revenue_percent_ttc
from (select sum(case when i.status = 'PAID' THEN ttc * 1.0 else 0 end) as percent_paid,
             sum(case
                     when i.status = 'CONFIRMED' THEN ttc * 0.5
                     else 0 end)                                        as percent_confirmed,
             sum(case when i.status = 'DRAFT' THEN ttc * 0 else 0 end)  as percent_draft
      from (select invoice_id,
                   (ht + ht * tax_config.rate / 100) as ttc
            from (select invoice_id, sum(quantity * unit_price) as ht
                  from invoice_line
                  group by invoice_id) invoice_ht,
                 tax_config) invoice_ttc
               join invoice i on invoice_ttc.invoice_id = i.id
               join invoice_line il on i.id = il.invoice_id) revenue_percentages;