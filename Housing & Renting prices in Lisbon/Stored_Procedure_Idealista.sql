USE [HPL]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[IDEALISTA_CLEANING] AS
BEGIN

WITH CTE0 AS (
	SELECT
		[HEADER]
		,[PRICE]
		,[ROOMS]
		,[AREA]
		,[FLOOR]
		,[LINK]
		,[DATE_SCRAPING]
		,ROW_NUMBER() OVER (PARTITION BY [LINK] ORDER BY [DATE_SCRAPING] DESC) AS ROW_N
		--ROW_NUMER allows the same add to stay updated in our database without duplicates
	FROM [HPL].[dbo].[Idealista_PY]
)
--- I am creating a common table expressions because I want to eleminate duplicates, for that I am using a window function that canot be used in the WHERE clause. 
	SELECT
		[HEADER]
		,trim(lower(replace(right([Header], CHARINDEX(',', REVERSE([Header]))), ',', ''))) as Map
		,CAST(REPLACE(left([PRICE], CHARINDEX('€', [PRICE])-1), '.', '') AS INT) as PRICE
		,cast(SUBSTRING([ROOMS], 2, 3) as int) as ROOMS
		,cast(left([AREA], CHARINDEX(' ', [AREA])) as int) as AREA
		,CASE WHEN [FLOOR] LIKE '%º%' then CAST(left([FLOOR], CHARINDEX('º', [FLOOR])-1) AS INT)
			WHEN [FLOOR] LIKE '%Rés do chão%' then 0
			WHEN [FLOOR] LIKE '%Cave%' then -1
			ELSE NULL END AS [FLOOR_NR]
		,CASE WHEN [FLOOR] LIKE '%sem elevador%' then 'sem elevador'
			WHEN [FLOOR] LIKE '%com elevador%' then 'com elevador'
			END AS [ELEVATOR]
		,[DATE_SCRAPING]
	FROM CTE0
	WHERE [ROW_N] = 1
	ORDER BY [HEADER];
END;
GO