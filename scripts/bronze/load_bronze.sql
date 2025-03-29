exec bronze.load_bronze
create or alter procedure bronze.load_bronze as 
begin
	begin try
		declare @start_time datetime, @end_time datetime;
		print '=============================================';
		print 'Loading Bronze Layer';
		print '=============================================';

		print '---------------------------------------------';
		print 'Loading CRM Tables';
		print '---------------------------------------------';

		set @start_time = getdate();
		print '>> Truncating Table: bronze.crm_cust_info'
		truncate table bronze.crm_cust_info
		print '>> Inserting Data Into : bronze.crm_cust_info'
		bulk insert bronze.crm_cust_info
		from 'D:\Download\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' second';
		print '--------------------------------------------'

		set @start_time = getdate();
		print '>> Truncating Table: bronze.crm_prd_info'
		truncate table bronze.crm_prd_info
		print '>> Inserting Data Into : bronze.crm_prd_info'
		bulk insert bronze.crm_prd_info
		from 'D:\Download\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' second';
		print '--------------------------------------------'

		set @start_time = getdate();
		print '>> Truncating Table: bronze.crm_sales_details'
		truncate table bronze.crm_sales_details
		print '>> Inserting Data Into : bronze.crm_sales_details'
		bulk insert bronze.crm_sales_details
		from 'D:\Download\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' second';
		print '--------------------------------------------'
		
		set @start_time = getdate();
		print '---------------------------------------------';
		print 'Loading CRM Tables';
		print '---------------------------------------------';

		print '>> Truncating Table: bronze.erp_cust_az12'
		truncate table bronze.erp_cust_az12
		print '>> Inserting Data Into : bronze.erp_cust_az12'

		bulk insert bronze.erp_cust_az12
		from 'D:\Download\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' second';
		print '--------------------------------------------'
		
		set @start_time = getdate();
		print '>> Truncating Table: bronze.erp_loc_a101'
		truncate table bronze.erp_loc_a101
		print '>> Inserting Data Into : bronze.erp_loc_a101'
		bulk insert bronze.erp_loc_a101
		from 'D:\Download\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' second';
		print '--------------------------------------------'
		
		set @start_time = getdate()

		print '>> Truncating Table: bronze.erp_px_cat_g1v2'
		truncate table bronze.erp_px_cat_g1v2
		print '>> Inserting Data Into : bronze.erp_px_cat_g1v2'
		bulk insert bronze.erp_px_cat_g1v2
		from 'D:\Download\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' second';
		print '--------------------------------------------'
	end try
	begin catch
		print '-------------------------------------------'
		print 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		print 'Eror message' + ERROR_MESSAGE();
		print 'Eror message' + cast (ERROR_NUMBER() as nvarchar);
		print 'Eror message' + cast (ERROR_STATE() as nvarchar);
		print '-------------------------------------------'
	end catch
end

