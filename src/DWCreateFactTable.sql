SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[FactRetail]
( 
	[IncrementalKey] [int]  NOT NULL,
	[Timestamp] [datetime2](7)  NOT NULL,
	[ProductKey] [int]  NOT NULL,
	[OrderDateKey] [int]  NOT NULL,
	[DueDateKey] [int]  NOT NULL,
	[ShipDateKey] [int]  NULL,
	[CustomerKey] [int]  NOT NULL,
	[PromotionKey] [int]  NOT NULL,
	[CurrencyKey] [int]  NOT NULL,
	[SalesOrderNumber] [nvarchar](20)  NOT NULL,
	[SalesOrderLineNumber] [tinyint]  NOT NULL,
	[OrderQuantity] [smallint]  NOT NULL,
	[UnitPrice] [money]  NOT NULL,
	[ProductStandardCost] [money]  NOT NULL,
	[TotalProductCost] [money]  NOT NULL,
	[SalesAmount] [money]  NOT NULL,
	[TaxAmount] [money]  NOT NULL,
	[FreightAmount] [money]  NOT NULL,
	[OrderDate] [datetime]  NULL,
	[DueDate] [datetime]  NULL,
	[ShipDate] [datetime]  NULL
)
WITH
(
	DISTRIBUTION = HASH ( [ProductKey] ),
	CLUSTERED COLUMNSTORE INDEX,
	PARTITION
	(
		[OrderDateKey] RANGE RIGHT FOR VALUES (20101229, 20110101, 20120101, 20130101, 20140101, 20150101, 20160101, 20170101, 20180101, 20190101, 20200101, 20210101)
	)
)
GO