# Azure Retail Data Engineering Project

![Azure Logo](https://example.com/logo.png)

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
