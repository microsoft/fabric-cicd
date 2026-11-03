/**************************************************************************************
Description:    This script creates NYTaxi views and stored procedures (2024 data version)
**************************************************************************************/

/***************************************************************
Create a view to find the preferred method of payment and associated amount processed in 2024 year
*****************************************************************/

CREATE VIEW [dbo].[vw_PaymentAnalysis] AS
SELECT
    PaymentType
    ,COUNT(T.PaymentType) AS PaymentsCount
    ,SUM(TotalAmount) AS TotalAmountProcessed
FROM dbo.Trip AS T
JOIN dbo.[Date] AS D
    ON T.[DateID]=D.[DateID]
WHERE YEAR(D.[Date])=2024
GROUP BY    
    PaymentType

GO