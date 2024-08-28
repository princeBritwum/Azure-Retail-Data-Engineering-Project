# Azure Retail Data Engineering Project

## Project Overview

This project demonstrates a retail data pipeline built on Azure. The pipeline ingests raw sales data from an on-premise server, processes it through ETL steps, and transforms it for analytics and reporting using Azure services such as Data Factory, Azure Synapse Analytics, Databricks, and SQL Data Warehouse. 

## Architecture

![docs/architecture diagram](https://github.com/princeBritwum/Azure-Retail-Data-Engineering-Project/blob/main/docs/architecture%20diagram.png)

The architecture includes:
- **Self-Hosted Integration runtimes** for loading data from on-prem datasource
- **Azure Data Factory / Azure Synapse Analytics** for orchestrating data movement.
- **Azure Databricks** for data transformation and processing.
- **Azure Data Lake** for scalable data storage.
- **Azure SQL Data Warehouse** for data warehousing and reporting.

## Features
- **Data Ingestion:** Automated pipelines for ingesting raw sales data.
- **ETL Processing:** Transformation of raw data into clean, analytics-ready datasets.
- **Data Analytics:** Insights and reporting using Power BI.

## Getting Started

### Prerequisites

- Python 3.8+
- Azure Subscription
  1. Synapse Workspace
  2. Dedicated SQL Pool
  3. Data Lake Gen 2
  4. Databricks
- [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli)
- Jupyter Notebook
- On-Premise Server with MSSQL Db

### Setup

1. Clone this repository:
   ```bash
   git clone https://github.com/your-username/azure-retail-data-engineering.git
   cd azure-retail-data-engineering
2. Prepare Your DataWarehouse Sever on Azure Synapse Analytics using the Dedicated Sql pool. For the purposes of this demo, I used 1 fact table and two Dimension Table.
   - In creating the Fact Table, I used a Hash Distribution , this is generally recommened against round-robbin for heavy Fact Table where data is often loaded.
   - I created a partition on my Fact Table to group Data using the date key. 
   ```sql
   SET ANSI_NULLS ON
   GO
   SET QUOTED_IDENTIFIER ON
   GO
   CREATE TABLE [dbo].[FactRetail]
   (
   	[IncrementalKey] [int] NOT NULL,
   	[Timestamp] [datetime2](7) NOT NULL,
   	[ProductKey] [int] NOT NULL,
   	[OrderDateKey] [int] NOT NULL,
   	[DueDateKey] [int] NOT NULL,
   	[ShipDateKey] [int] NULL,
   	[CustomerKey] [int] NOT NULL,
   	[PromotionKey] [int] NOT NULL,
   	[CurrencyKey] [int] NOT NULL,
   	[SalesOrderNumber] [nvarchar](20) NOT NULL,
   	[SalesOrderLineNumber] [tinyint] NOT NULL,
   	[OrderQuantity] [smallint] NOT NULL,
   	[UnitPrice] [money] NOT NULL,
   	[ProductStandardCost] [money] NOT NULL,
   	[TotalProductCost] [money] NOT NULL,
   	[SalesAmount] [money] NOT NULL,
   	[TaxAmount] [money] NOT NULL,
   	[FreightAmount] [money] NOT NULL,
   	[OrderDate] [datetime] NULL,
   	[DueDate] [datetime] NULL,
   	[ShipDate] [datetime] NULL
   )
   WITH
   (
   	DISTRIBUTION = HASH ( [ProductKey] ),
   	CLUSTERED COLUMNSTORE INDEX,
   	PARTITION
   	(
   		[OrderDateKey] RANGE RIGHT FOR VALUES (20101229, 20110101, 20120101, 20130101, 20140101, 20150101, 20160101, 20170101, 20180101, 20190101, 20200101, 20210101, 20220101)
   	)
   )
   GO
3. To Begin the ETL process, I created a data pipeline in Azure Synapse Analytics that Moves Data from On-Premise MSSQL Server to the Synapse Dedicated SQL Pool DW Tables. Steps to achieve this are detailed below;
   
  - Source: Connect Source Data from On-Prem MSSQL Server to Azure Dataset
     - I created a Self-Hosted Integration Runtime to create to Connect Data from On-Premise MSQl Server
     - I proceeded to use my Integration Runtime to create a Linked Service to the Source Dataset where On-Prem Data will reside
     - I use the below Query to load data the Fact Table in the On-Premise MSSQL into the Source Dataset considering the partition defined
       ```sql
       SELECT
       ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS IncrementalID,
       GETDATE() AS [Timestamp],
       [ProductKey],
       [OrderDateKey],
       [DueDateKey],
       [ShipDateKey],
       [CustomerKey],
       [PromotionKey],
       [CurrencyKey],
       [SalesOrderNumber],
       [SalesOrderLineNumber],
       [OrderQuantity],
       [UnitPrice],
       [ProductStandardCost],
       [TotalProductCost],
       [SalesAmount],
       [TaxAmt],
       [Freight],
       [OrderDate],
       [DueDate],
       [ShipDate]
       FROM FactInternetSales
       WHERE [OrderDateKey] >= 20101229 and [OrderDateKey] < 20240101
     The [IncrementalID] was included to generate unique values using the ROW_NUMBER() OVER () function.
    - Once initial data is loaded, subsequent data load is done using the query below;
      ```sql
      SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS IncrementalID,
       GETDATE() AS [Timestamp],
       [ProductKey],
       [OrderDateKey],
       [DueDateKey],
       [ShipDateKey],
       [CustomerKey],
       [PromotionKey],
       [CurrencyKey],
       [SalesOrderNumber],
       [SalesOrderLineNumber],
       [OrderQuantity],
       [UnitPrice],
       [ProductStandardCost],
       [TotalProductCost],
       [SalesAmount],
       [TaxAmt],
       [Freight],
       [OrderDate],
       [DueDate],
       [ShipDate]
      FROM FactInternetSales
      WHERE orderdate >= DATEADD(DAY, -3, CAST(GETDATE() AS DATE)) 
 - Sink : The Sink Dataset is the destination where data loaded from the Source dataset resides. To create the sink dataset, I use the below step;
     - I created a linked service to the Dedicated Sql Pool Table where source data would be loaded
     - In the Copy method, I used UPSERT, this is because I want to insert new data as it comes in and update existing data when need be.
     - The key columns I used in the copy method are [SalesOrderNumber] and [Timestamp] since they determine the transactions that comes in the destination and when they came in
     - I also mapped the columns in the source dataset to the destination dataset to ensure there are no column mismatch when data load is in progress
 - Trigger : I created a trigger DataLoad to initiate the pipeline every two days at 4:00 am since this is when new data comes into the Source(on-Premise SQL database.


**Pipeline Images**
a. Source
![docs/PipeLine 1a.png](https://github.com/princeBritwum/Azure-Retail-Data-Engineering-Project/blob/main/docs/PipeLine%201a.png)

b. Sink
![docs/PipeLine1b.png](https://github.com/princeBritwum/Azure-Retail-Data-Engineering-Project/blob/main/docs/PipeLine1b.png)

c.Trigger
![docs/DataLoad Trigger.png](https://github.com/princeBritwum/Azure-Retail-Data-Engineering-Project/blob/main/docs/DataLoad%20Trigger.png)

- To see the loaded data in Fact Table from the On-Premise, run the below query;
  ```sql
          SELECT TOP (100) [IncrementalKey]
          ,[Timestamp]
          ,[ProductKey]
          ,[OrderDateKey]
          ,[DueDateKey]
          ,[ShipDateKey]
          ,[CustomerKey]
          ,[PromotionKey]
          ,[CurrencyKey]
          ,[SalesOrderNumber]
          ,[SalesOrderLineNumber]
          ,[OrderQuantity]
          ,[UnitPrice]
          ,[ProductStandardCost]
          ,[TotalProductCost]
          ,[SalesAmount]
          ,[TaxAmount]
          ,[FreightAmount]
          ,[OrderDate]
          ,[DueDate]
          ,[ShipDate]
           FROM [dbo].[FactRetail]

Thats not all, we also have a Customer Cards data is stored in the  Azure Datalake Storage Gen2 directory, This data is been dropped into by external data directly into the storage account, since we need this data as part of our analysis on our customers, we need to bring this data into our Datawarehouse(Dedicated SQL pool).
The data is for Customer Cards records and contains sensitive data like Credit cards numbers we dont have everybody to have access to. 
To achieve this goal, we will need to Transform the data as we load it from the Storage account and encrypt the credit cards numbers to provide security for our customers.

Lets get into this;
1. First We will need to create the Customer Cards Table in our DW. We will achieve this with the code below, remember we use the Round_robin distribution here;
   ```sql
   CREATE TABLE [dbo].CustomerCardData
    (
        [CustomerID] VARCHAR(20) NOT NULL,
        [CardType] CHAR(50) NULL,
        [CardHash] NVARCHAR(100) NULL,
        [IssuingCountry] CHAR(50)  NULL,
        [ExpiryDate] DATE NULL,
        [CVV2] CHAR(50)  NULL
    
    )
    WITH
    (
        DISTRIBUTION = ROUND_ROBIN,
        CLUSTERED COLUMNSTORE INDEX
    )
    GO
2. Next we use the  Notebook in [src/CustomerCardData.ipynb] to;
   - Load the data from the Storage Account into a Schema [CustomerCardSchema]
   

    ```ipynb
    %%pyspark
    from pyspark.sql.types import *
    
    
    CustomerCardSchema = StructType(
                        [ StructField("CustomerId", IntegerType(), True),
                          StructField("CardNumber", StringType(), True),
                          StructField("CardType", StringType(), True),
                          StructField("IssuingCountry", StringType(), True),
                          StructField("ExpiryDate", DateType(), True),
                          StructField("CVV2", StringType(), True)
                        ])
    df = spark.read.load('abfss://files@datalakecpvl0xw.dfs.core.windows.net/sales_data/Customer Credit Card.csv', format='csv'
    ## If header exists uncomment line below
    , header=True, schema=CustomerCardSchema
    )
    df.write.mode("overwrite").saveAsTable("CustomerCardDetails")
    display(df)
- We then imprt the sha1 function "from pyspark.sql.functions import sha1" to encrypt the CardNumber column from the data and save the transformed data back to the data lake. The Transformed data can be found in [data/Transformed/]
  
  ```ipynb
      from pyspark.sql.functions import sha1
      from pyspark.sql.functions import *
      
      # compute the hash value of the name column
      hashed_df = df.withColumn("CardHash", sha1("CardNumber"))
      
      hashed_df = hashed_df.select(trim('CustomerId').alias('CustomerId'), 'CardType','CardHash','IssuingCountry','ExpiryDate','CVV2')
      display(hashed_df)
- We then save the transformed data back to the data lake. The Transformed data can be found in [data/Transformed/]
  ```ipynb
      from pyspark.sql.functions import *
      hashed_df.write.mode('overwrite').csv('/retaildata/destination/TransformedRetailData/CustomerCardData.csv')
      print ("Transformed data saved!")

- Now to load the transformed data into our DW, we make use of the 'COPY INTO' function
  ```sql
      COPY INTO dbo.CustomerCardData
      (CustomerID 1, CardType 2, CardHash 3, IssuingCountry 4, ExpiryDate 5, CVV2 6)
      FROM 'https://datalake*******.dfs.core.windows.net/files/retaildata/destination/TransformedRetailData/CustomerCardData.csv'
      WITH
      (
      	FILE_TYPE = 'CSV'
      	,MAXERRORS = 0
      	,FIRSTROW = 2
      	,ERRORFILE = 'https://datalake*******.dfs.core.windows.net/files/'
      )

-For continues integration , I added the [src/CustomerCardData.ipynb] to the Retail pipeline and added a Copy Into activity to load the transformed Customer Cards data from the Storage Location (Dalake Gen 2) into the CustomerCards table in the DW. The new pipeline is below;
![docs/Retail Pipeline v1.png](https://github.com/princeBritwum/Azure-Retail-Data-Engineering-Project/blob/main/docs/Retail%20Pipeline%20v1.png)

3. At this point, we are pretty in a good shape to start visualizing data from the Data warehouse;

We would use SQL to write an aggregated query for business Insights, we will then use Power BI to visualize the data. Lets dig in;

- We will Create a View that would make it easy for Data Analyst and Business User to consume direct insight without worrying about SQL Queries, since we are using Partitions in our DW Fact Table we will select specific partition to create the view to improve the response rate of queries.
  ```sql
      CREATE VIEW CustomerTransactions2013
      AS
      SELECT P.EnglishProductName AS ProductName,
             A.[CustomerKey],
             M.EnglishPromotionName AS PromotionName,
             Y.CurrencyName,
             A.[SalesOrderNumber],
             B.FirstName + ' ' + B.LastName As FullName,
             CASE
                 WHEN C.[CardType] IS NULL THEN
                     'Cash'
                 ELSE
                     C.[CardType]
             END [CardType],
             CASE
                 WHEN C.IssuingCountry IS NULL THEN
                     'US'
                 ELSE
                     C.[IssuingCountry]
             END [IssuingCountry],
             SUM(A.[OrderQuantity]) AS [OrderQuantity],
             AVG(A.[UnitPrice]) AS [UnitPrice],
             SUM(A.[ProductStandardCost]) AS [ProductStandardCost],
             SUM(A.[TotalProductCost]) AS [TotalProductCost],
             SUM(A.[SalesAmount]) AS [SalesAmount],
             SUM(A.[TaxAmount]) AS [TaxAmount],
             SUM(A.[FreightAmount]) AS [FreightAmount],
             CAST(A.[OrderDate] AS DATE) AS [OrderDate]
      FROM [FactRetail] A
          JOIN [DimCustomer] B
              ON A.[CustomerKey] = B.CustomerKey
          LEFT JOIN [CustomerCardData] C
              ON A.[CustomerKey] = C.[CustomerID]
          JOIN [dbo].[DimProduct] P
              ON A.[ProductKey] = P.ProductKey
          JOIN [dbo].[DimCurrency] Y
              ON A.[CurrencyKey] = Y.[CurrencyKey]
          JOIN DimPromotion M
              ON A.PromotionKey = M.PromotionKey
      WHERE OrderDateKey >= 20130101 and OrderDateKey < 20140101
      GROUP BY P.EnglishDescription,
               A.[CustomerKey],
               A.[SalesOrderNumber],
               CAST(A.[OrderDate] AS DATE),
               C.[CardType],
               C.[IssuingCountry],
               B.FirstName,
               B.LastName,
               M.EnglishPromotionName,
               P.EnglishProductName,
               Y.CurrencyName


4. We would now use this view as our data source in Power BI to Create insightful visuals for Business Users.
 - To create Visuals in Power BI, we need to First Establish connection with our DW(Sypase Dedicated SQL Pool) in Azure.
   
![docs/Selecting View from DW.png](https://github.com/princeBritwum/Azure-Retail-Data-Engineering-Project/blob/main/docs/Selecting%20View%20from%20DW.png)

 - We created this Report afterwards;
   
![docs/Report Visual 1.png](https://github.com/princeBritwum/Azure-Retail-Data-Engineering-Project/blob/main/docs/Report%20Visual%201.png)

### Conclusion



### Challenges and Lessons Learnt
