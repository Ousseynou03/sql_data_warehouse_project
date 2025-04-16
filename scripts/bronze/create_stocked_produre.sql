
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY
		PRINT '=============================================='
		PRINT 'Loading the bronze layer'
		PRINT '=============================================='

		PRINT 'Loading the CRM Tables'

		set @start_time = GETDATE();
		PRINT @start_time;
		PRINT '>> Trancating table : bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\weuzi\OneDrive\Documents\Data Engineering\sql_data_warehouse_project\dataset\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		SELECT COUNT(*) FROM bronze.crm_cust_info;

		-- prd_info
		TRUNCATE TABLE bronze.crm_prd_info
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\weuzi\OneDrive\Documents\Data Engineering\sql_data_warehouse_project\dataset\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SELECT COUNT(*) FROM bronze.crm_prd_info;

		-- crm_sales_details
		TRUNCATE TABLE bronze.crm_sales_details
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\weuzi\OneDrive\Documents\Data Engineering\sql_data_warehouse_project\dataset\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SELECT COUNT(*) FROM bronze.crm_sales_details;


		-- erp_cust_az12
		TRUNCATE TABLE bronze.erp_cust_az12
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\weuzi\OneDrive\Documents\Data Engineering\sql_data_warehouse_project\dataset\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SELECT COUNT(*) FROM bronze.erp_cust_az12;

		-- erp_loc_a101
		TRUNCATE TABLE bronze.erp_loc_a101
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\weuzi\OneDrive\Documents\Data Engineering\sql_data_warehouse_project\dataset\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SELECT COUNT(*) FROM bronze.erp_loc_a101;


		-- erp_px_cat_g1v2
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\weuzi\OneDrive\Documents\Data Engineering\sql_data_warehouse_project\dataset\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;

	END TRY
	BEGIN CATCH
		PRINT '================================================'
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT '================================================'
	END CATCH

END
