CREATE TABLE [dbo].[Geography] (
    [GeographyID] INT          NOT NULL,
    [ZipCodeBKey] VARCHAR (10) NOT NULL,
    [County]      VARCHAR (50) NULL,
    [City]        VARCHAR (50) NULL,
    [State]       VARCHAR (50) NULL,
    [Country]     VARCHAR (50) NULL,
    [ZipCode]     VARCHAR (50) NULL
);


GO