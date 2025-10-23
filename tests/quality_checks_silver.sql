/*
===========================================================================================================
Quality checks
============================================================================================================
script purpose:
    This script performs various quality checks for data consistency,accuracy and standardizatin across
    the silver schemas.
=============================================================================================================
*/

-- ===============================================================
-- checking 'silver.crm_cust_info'
-- ===============================================================
-- check for nulls or duplicate in primary key
-- expectation : no result
select cst_id,
	   count(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null

select * from(
	select *,
		   ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as rnk
	from silver.crm_cust_info
	)t where rnk = 1

select cst_key
from silver.crm_cust_info
where cst_key is null or cst_key = ' '


-- checking for unwanted spaces.
select cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname)

select cst_lastname
from silver.crm_cust_info
where cst_lastname != trim(cst_lastname)

select cst_gndr
from silver.crm_cust_info
where cst_gndr != trim(cst_gndr)

-- Data standardization & consistency
select distinct cst_gndr
from silver.crm_cust_info

select distinct cst_marital_status
from silver.crm_cust_info
-- ********************************************************************

  
-- ===============================================================
-- checking 'silver.crm_prd_info'
-- ===============================================================
select prd_id,
	   prd_key,
	   prd_nm,
	   prd_cost,
	   prd_line,
	   prd_start_dt,
	   prd_end_dt
from silver.crm_prd_info

-- check for nulls or duplicate in primary key
-- expectation : no result
select prd_id,
	   count(*)
from silver.crm_prd_info
group by prd_id
having count(*) >1 or prd_id is null

-- check for unwanted spaces
select prd_nm
from silver.crm_prd_info
where prd_nm != trim(prd_nm)

--check for nulls or negative numbers
--expectation : No result
select prd_cost
from silver.crm_prd_info
where prd_cost <0 or prd_cost is null

-- Data standardization and consistency
select 
	distinct prd_line
from silver.crm_prd_info

-- check for invalid date orders
select * from
silver.crm_prd_info
where prd_end_dt < prd_start_dt;
-- *****************************************************************************************


-- ===============================================================
-- checking 'silver.crm_sales_details
-- ===============================================================
-- check for invalid dates
select 
	 nullif( sls_order_dt ,0)	  
from silver.crm_sales_details
where sls_order_dt<=0 

-- check for outliers
select 
	 nullif( sls_order_dt ,0)	  
from silver.crm_sales_details
where sls_order_dt<=0 or
len(sls_order_dt) !=8 or
sls_order_dt >20500101 

-- check data consistency : between sales,quantity and price
-- sales = quantity * price
-- values must not be null or zero or negative
select sls_sales,
		sls_quantity,
		sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null
or sls_quantity is null
or sls_price is null
or sls_sales <=0
or sls_quantity <=0
or sls_price<=0
-- ***************************************************************************************************

  
-- ===============================================================
-- checking 'silver.erp_cust_az12'
-- ===============================================================
-- identify out of range date
select distinct bdate
from silver.erp_cust_az12
where bdate<'1924-01-01' or bdate > getdate()

-- Data standardization & consistency
select distinct gen from silver.erp_cust_az12
-- ***************************************************************************************************

  
-- ===============================================================
-- checking 'silver.erp_px_cat_g1v2'
-- =============================================================== 
-- check for unwanted spaces
select * from silver.erp_px_cat_g1v2
where cat !=trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance)

-- Data standardization & consistency
select distinct cat from silver.erp_px_cat_g1v2

select distinct subcat from silver.erp_px_cat_g1v2

select distinct maintenance from silver.erp_px_cat_g1v2
