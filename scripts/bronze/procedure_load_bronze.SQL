/*
==============================================================================================================
Store Procedure : Load bronze layer (source -> Bronze)
==============================================================================================================
Script purpose:
        This stored procedured loads data into the 'bronze' schema from external CSV files.
        it performs the following action:
          - Truncates thr bronze tables before loading data.
          - Uses the 'BULKINSERT' command to load data from CSV files to bronze tables

Parameters:
    none.
    this stored procedured does not accept any parameter or return any values.

usage example:
    EXEC bronze.load_bronze;
==============================================================================================================
*/


create or alter procedure bronze.load_bronze as
begin 
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime,@batch_end_time datetime;
	begin try
		set @batch_start_time = getdate();
		print '==========================================================';
		print 'Loading Bronze layer';
		print '==========================================================';
	
		print '----------------------------------------------------------';
		print 'Loading CRM tables';
		print '----------------------------------------------------------';

		set @start_time = GETDATE();
		print '>> Truncating table : bronze.crm_cust_info';
		Truncate table bronze.crm_cust_info;

		print '>> Inserting date into table : bronze.crm_cust_info';
		Bulk insert bronze.crm_cust_info
		from 'C:\Users\jebap\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			  firstrow = 2,
			  fieldterminator = ',',
			  tablock,
			  keepnulls);
		set @end_time = GETDATE();
		print '>> load duration :' + cast(datediff(second , @start_time , @end_time)as nvarchar) + ' second';
		print '>>--------------------------';


		set @start_time = GETDATE();
		print '>> Truncating table  : bronze.crm_prd_info';
		Truncate table bronze.crm_prd_info;

		print '>> Inserting data into table : bronze.crm_prd_info';
		Bulk insert bronze.crm_prd_info
		from 'C:\Users\jebap\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			  firstrow = 2,
			  fieldterminator = ',',
			  tablock,
			  keepnulls)
		set @end_time = GETDATE();
		print '>> load duration :' + cast(datediff(second , @start_time , @end_time)as nvarchar) + ' second';
		print '>>--------------------------';


		set @start_time = GETDATE();
		print '>> Truncating table : bronze.crm_sales_details';
		Truncate table bronze.crm_sales_details;

		print '>> inserting Date into table : bronze.crm_sales_details';
		Bulk insert bronze.crm_sales_details
		from 'C:\Users\jebap\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			  firstrow = 2,
			  fieldterminator = ',',
			  tablock,
			  keepnulls)
		set @end_time = GETDATE();
		print '>> load duration :' + cast(datediff(second , @start_time , @end_time)as nvarchar) + ' second';
		print '>>--------------------------';


		print '----------------------------------------------------------';
		print 'Loading ERP tables';
		print '----------------------------------------------------------';

		set @start_time = GETDATE();
		print '>> Truncating table : bronze.erp_cust_az12';
		Truncate table bronze.erp_cust_az12;

		print '>> inserting data into table  : bronze.erp_cust_az12';
		Bulk insert bronze.erp_cust_az12
		from 'C:\Users\jebap\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			  firstrow = 2,
			  fieldterminator = ',',
			  tablock,
			  keepnulls)
		set @end_time = GETDATE();
		print '>> load duration :' + cast(datediff(second , @start_time , @end_time)as nvarchar) + ' second';
		print '>>--------------------------';


		set @start_time = GETDATE();
		print '>> Truncating table : bronze.erp_loc_a101';
		Truncate table bronze.erp_loc_a101;

		print '>> Truncating data into table : bronze.erp_loc_a101';
		Bulk insert bronze.erp_loc_a101
		from 'C:\Users\jebap\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			  firstrow = 2,
			  fieldterminator = ',',
			  tablock,
			  keepnulls);
		set @end_time = GETDATE();
		print '>> load duration :' + cast(datediff(second , @start_time , @end_time)as nvarchar) + ' second';
		print '>>--------------------------';


		set @start_time = GETDATE();
		print '>> Truncating table : bronze.erp_px_cat_g1v2';
		Truncate table bronze.erp_px_cat_g1v2;

		print '>> Truncating table : bronze.erp_px_cat_g1v2';
		Bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\jebap\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			  firstrow = 2,
			  fieldterminator = ',',
			  tablock,
			  keepnulls);
		set @end_time = GETDATE();
		print '>> load duration :' + cast(datediff(second , @start_time , @end_time)as nvarchar) + ' second';
		print '>>--------------------------';

		set @batch_end_time = GETDATE();
		print '===============================================';
		print 'Loading bronze layer is completed';
		print 'Total load duration:' + cast(datediff(second , @batch_start_time,@batch_end_time)as nvarchar) + ' seconds';
		print '===============================================';

	end try
	begin catch
		print '======================================================';
		print 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		print 'Error message' + Error_message();
		print 'Error message' + cast(error_number() as nvarchar);
		print 'Error message' + cast(error_state() as nvarchar);
		print '======================================================';
	end catch 
end







