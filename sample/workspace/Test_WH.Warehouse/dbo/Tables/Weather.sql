CREATE TABLE [dbo].[Weather] (
    [DateID]                   INT        NOT NULL,
    [GeographyID]              INT        NOT NULL,
    [PrecipitationInches]      FLOAT (53) NOT NULL,
    [AvgTemperatureFahrenheit] FLOAT (53) NOT NULL
);


GO