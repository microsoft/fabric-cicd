CREATE TABLE [dbo].[Time] (
    [TimeID]                INT           NOT NULL,
    [TimeBKey]              VARCHAR (8)   NOT NULL,
    [HourNumber]            INT           NOT NULL,
    [MinuteNumber]          INT           NOT NULL,
    [SecondNumber]          INT           NOT NULL,
    [TimeInSecond]          INT           NOT NULL,
    [HourlyBucket]          VARCHAR (15)  NOT NULL,
    [DayTimeBucketGroupKey] INT           NOT NULL,
    [DayTimeBucket]         VARCHAR (100) NOT NULL
);


GO