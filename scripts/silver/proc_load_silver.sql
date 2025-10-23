/*
==============================================================================================================
Store Procedure : Load silver layer (source -> silver)
==============================================================================================================
Script purpose:
        This stored procedured performs the ETL process to populate the 'silver' schema tables from the 'bronze' schema.
        it performs the following action:
          - Truncates thr silver tables .
          - insert transformed and cleansed data from bronze tables into silver tables.

Parameters:
    none.
    this stored procedured does not accept any parameter or return any values.

usage example:
    exec silver.load_silver;
==============================================================================================================
*/


create or alter procedure silver.load_silver as
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime,@batch_end_time datetime;
	begin try
		set @batch_start_time = getdate();
		print '==========================================================';
		print 'Loading silver layer';
		print '==========================================================';
	
		print '----------------------------------------------------------';
		print 'Loading CRM tables';
		print '----------------------------------------------------------';

		set @start_time = GETDATE();
		print 'Truncating table: silver.crm_cust_info';
		truncate table silver.crm_cust_info;
		print '>> inserting data into : silver.crm_cust_info'
		insert into silver.crm_cust_info(
						cst_id,
						cst_key,
						cst_firstname,
						cst_lastname,
						cst_marital_status,
						cst_gndr,
						cst_create_date)	   
		select cst_id,
			   cst_key,
			   trim(cst_firstname) as cst_firstname,
			   trim(cst_lastname) as cst_lastname,
			   case when upper(trim(cst_marital_status)) = 'S' then 'single'
					 when upper(trim(cst_marital_status)) = 'M' then 'married'
				else 'n/a' end as cst_marital_status,
			   case when upper(trim(cst_gndr)) = 'F' then 'female'
					when upper(trim(cst_gndr)) = 'M' then 'male'
			   else 'n/a' end as cst_gndr,
			   try_convert(date ,cst_create_date ) as cst_create_date

		from(	
			select *,
				   ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as rnk
			from bronze.crm_cust_info
			)t where rnk = 1

		set @end_time = GETDATE();
		print '>> load duration :' + cast(datediff(second , @start_time , @end_time)as nvarchar) + ' second';
		print '>>--------------------------';

		--****************************************************************************************************
		set @start_time = GETDATE();
		print 'Truncating table: silver.crm_prd_info';
		truncate table silver.crm_prd_info;
		print '>> inserting data into : silver.crm_prd_info'
		insert into silver.crm_prd_info(
				prd_id,
				cat_id,
				prd_key,		
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt)

		select prd_id,
			   replace(substring(prd_key,1,5),'-','_') as cat_id,
			   substring(prd_key,7,len(prd_key)) as prd_key,
			   prd_nm,
			   isnull(prd_cost,0) as prd_cost,
			   case upper(trim(prd_line))
					 when 'M' then 'mountain'
					 when 'R' then 'road'
					 when 'S' then 'other sales'
					 when 'T' then 'touring'
					 else 'n/a'
				 end as prd_line,
			   cast(prd_start_dt as date) as prd_start_dt,
			   cast(dateadd(day , -1,
					lead(prd_start_dt)
					over(partition by prd_key order by prd_start_dt))  as date) as prd_end_dt	   
		from bronze.crm_prd_info

		set @end_time = GETDATE();
		print '>> load duration :' + cast(datediff(second , @start_time , @end_time)as nvarchar) + ' second';
		print '>>--------------------------';

		--********************************************************************************************
		set @start_time = GETDATE();
		print 'Truncating table: silver.crm_sales_details';
		truncate table silver.crm_sales_details;
		print '>> inserting data into : silver.crm_sales_details'
		insert into silver.crm_sales_details(
					sls_ord_num,
					sls_prd_key,
					sls_cust_id,
					sls_order_dt,
					sls_ship_dt,
					sls_due_dt,
					sls_sales,
					sls_quantity,
					sls_price)
		select  sls_ord_num,sls_prd_key,sls_cust_id,
				case 
					when sls_order_dt =0 or len(sls_order_dt) != 8 then null
					else convert(date,convert(nvarchar(8),sls_order_dt,112))
					end as sls_order_dt,

				case when sls_ship_dt =0 or len(sls_ship_dt) != 8 then null
					else convert(date,convert(nvarchar(8),sls_ship_dt,112))
					end as sls_ship_dt,

				case when sls_due_dt =0 or len(sls_due_dt) != 8 then null
					else convert(date,convert(nvarchar(8),sls_due_dt,112))
					end as sls_due_dt,

				case when sls_sales is null or sls_sales<=0 or  sls_sales != sls_quantity * abs(sls_price)
					 then sls_quantity * abs(sls_price)
					 else sls_sales end as sls_sales,

				sls_quantity,

				case when sls_price is null or sls_price<=0
						 then sls_sales/nullif(sls_quantity ,0)
					else sls_price end as sls_price
		from bronze.crm_sales_details

		set @end_time = GETDATE();
		print '>> load duration :' + cast(datediff(second , @start_time , @end_time)as nvarchar) + ' second';
		print '>>--------------------------';

		--*********************************************************************************************
		print '----------------------------------------------------------';
		print 'Loading ERP tables';
		print '----------------------------------------------------------';

		set @start_time = GETDATE();
		print 'Truncating table: silver.erp_cust_az12';
		truncate table silver.erp_cust_az12;
		print '>> inserting data into : silver.erp_cust_az12'
		insert into silver.erp_cust_az12(
				 cid,
				 bdate,
				 gen)

		select case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
					else cid end as cid,
				case when bdate>getdate() then null
					else bdate end as bdate,
			   case when upper(trim(gen)) in('Female','F') then 'female'
					when upper(trim(gen)) in ('Male','M') then 'male'
					else 'n/a' end as gen
		from bronze.erp_cust_az12

		set @end_time = GETDATE();
		print '>> load duration :' + cast(datediff(second , @start_time , @end_time)as nvarchar) + ' second';
		print '>>--------------------------';

		--*********************************************************************************************
		set @start_time = GETDATE();
		print 'Truncating table: silver.erp_loc_a101';
		truncate table silver.erp_loc_a101;
		print '>> inserting data into : silver.erp_loc_a101'
		insert into silver.erp_loc_a101(
				cid,
				cntry)

		select replace(cid,'-','') as cid,
			case when trim(cntry) = 'DE' then 'Germany'
					when trim(cntry) in ('US','USA') then 'United State'
					when trim(cntry) = ''or cntry is null then 'n/a'
					else trim(cntry)
					end as cntry
		from bronze.erp_loc_a101

		set @end_time = GETDATE();
		print '>> load duration :' + cast(datediff(second , @start_time , @end_time)as nvarchar) + ' second';
		print '>>--------------------------';

		--***********************************************************************************************
		set @start_time = GETDATE();
		print 'Truncating table: silver.erp_px_cat_g1v2';
		truncate table silver.erp_px_cat_g1v2;
		print '>> inserting data into : silver.erp_px_cat_g1v2'
		insert into silver.erp_px_cat_g1v2(
				id,
				cat,
				subcat,
				maintenance)
		select id,
			   cat,
			   subcat,
			   maintenance
		from bronze.erp_px_cat_g1v2

		--**********************************************************************************************
		set @end_time = GETDATE();
		print '>> load duration :' + cast(datediff(second , @start_time , @end_time)as nvarchar) + ' second';
		print '>>--------------------------';

		set @batch_end_time = GETDATE();
		print '===============================================';
		print 'Loading silver layer is completed';
		print 'Total load duration:' + cast(datediff(second , @batch_start_time,@batch_end_time)as nvarchar) + ' seconds';
		print '===============================================';

	end try
	begin catch
		print '======================================================';
		print 'ERROR OCCURED DURING LOADING silver LAYER';
		print 'Error message' + Error_message();
		print 'Error message' + cast(error_number() as nvarchar);
		print 'Error message' + cast(error_state() as nvarchar);
		print '======================================================';
	end catch 

end

