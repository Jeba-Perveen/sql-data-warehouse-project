/*
======================================================================================================
DDl Script : Create gold views
======================================================================================================
Script purpose :
    This script creates views for the gold layer in the data warehouse.
    The gold layer represents the final dimension and fact tables (star schema)

    Each view performs transformations and combines data from silver layer to produce clean,enriched
    and business ready dataset.

usage:
 - These views can be queried directly for analystics and reporting.
=======================================================================================================
*/

-- ====================================================================================================
-- create dimension : gold.dim_customer
-- ====================================================================================================
If object_id('gold.dim_customer','V') is not null
  Drop view gold.dim_customer
  
GO
  
create view gold.dim_customer as
	select ROW_NUMBER() over(order by ci.cst_id)    as customer_key,
		   ci.cst_id                                  as customer_id,
		   ci.cst_key                                 as customer_number,
		   ci.cst_firstname                           as first_name,
		   ci.cst_lastname                            as last_name,
		   la.cntry                                   as country,
		   ci.cst_marital_status                      as marital_status,
		   case when ci.cst_gndr != 'n/a' then ci.cst_gndr
			  else coalesce(ca.gen,'n/a') end           as gender,
		   ci.cst_create_date                         as create_date,
		   ca.bdate                                   as birthdate
	from silver.crm_cust_info ci
	left join silver.erp_cust_az12 ca
	on ci.cst_key = ca.cid
	left join silver.erp_loc_a101 la
	on la.cid = ci.cst_key
;
GO
  
--**************************************************************************************

-- ====================================================================================================
-- create dimension : gold.dim_products
-- ====================================================================================================
If object_id('gold.dim_products','V') is not null
  Drop view gold.dim_products

GO
  
  create view gold.dim_products as 
	select ROW_NUMBER() over(order by pr.prd_start_dt,pr.prd_key)    as product_key,
		   pr.prd_id                                                   as product_id,
		   pr.prd_key                                                  as product_number,	   
		   pr.prd_nm                                                   as product_name,	   	   
		   pr.cat_id                                                   as category_id,
		   pc.cat                                                      as category,
		   pc.subcat                                                   as subcategory,
		   pc.maintenance                                              as maintenance,
		   pr.prd_cost                                                 as cost,
		   pr.prd_line                                                 as product_line,
		   pr.prd_start_dt                                             as start_date
	from silver.crm_prd_info  pr
	left join silver.erp_px_cat_g1v2 pc
	on pr.cat_id = pc.id
	where pr.prd_end_dt is null;

GO
  
-- ********************************************************************************************

-- ====================================================================================================
-- create dimension : gold.fact_sales
-- ====================================================================================================
If object_id('gold.fact_sales','V') is not null
  Drop view gold.fact_sales

GO
  
create view gold.fact_sales as
	select cd.sls_ord_num         as order_number,
		   pr.product_key,
		   cu.customer_key,
		   cd.sls_order_dt           as order_date,
		   cd.sls_ship_dt            as shipping_date,
		   cd.sls_due_dt             as due_date,
		   cd.sls_sales              as sales_amount,
		   cd.sls_quantity           as quantity,
		   cd.sls_price              as price
	from silver.crm_sales_details cd
	left join gold.dim_products pr
	on pr.product_number = cd.sls_prd_key
	left join gold.dim_customer cu
	on cu.customer_id = cd.sls_cust_id;

GO
