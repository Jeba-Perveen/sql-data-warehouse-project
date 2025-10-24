/*
=========================================================================================================
Quality checks
=========================================================================================================
Script purpose:
    This Script performs quality checks to validate the integrity,consistency , and accuracy of the gold
    layer. These checks ensure :-
       - uniqueness of surrogate keys in dimension tables.
       - Referential integrity between fact and dimension tables.
       - Validation of relationship in the data model for analytics purposes.

usage notes : 
    - run these checks after data loading silver layer.
    - Investigate and resolve any discrepancies found during the checks.
===========================================================================================================
*/

-- ===========================================================================
-- Checking 'gold.dim_customer'
-- ===========================================================================
-- check for uniqueness of customer key in gold.dim_customer
select 
    customer_key,
    count(*) as  duplicate_cnt
from gold.dim_customer
group by customer_key
having count(*)>1;

-- ===========================================================================
-- Checking 'gold.dim_products_key'
-- ===========================================================================
-- check for uniqueness of product key in gold.dim_products
select 
    product_key,
    count(*) as  duplicate_cnt
from gold.dim_products
group by product_key
having count(*)>1;

-- ===========================================================================
-- Checking 'gold.fact_sales'
-- ===========================================================================
-- check data model connectivity between fact and dimension
select *
from gold.fact_sales f
left join gold.dim_customer c
on c.customer_key = f.customer_key
left  join gold.dim_products p
on p.product_key = f.product_key
where p.product_key is null or c.customer_key is null;
