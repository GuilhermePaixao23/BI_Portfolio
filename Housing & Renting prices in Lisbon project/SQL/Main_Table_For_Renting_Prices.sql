DROP TABLE IF EXISTS #TMP_IDEALISTA;
	SELECT
		[HEADER]
		,[PRICE]
		,[ROOMS]
		,[AREA]
		,[FLOOR]
		,[LINK]
		,[DATE_SCRAPING]
		,ROW_NUMBER() OVER (PARTITION BY [LINK] ORDER BY [DATE_SCRAPING] DESC) AS ROW_N
		--ROW_NUMER allows the same add to stay updated in our database qwithout duplicates
	into #TMP_IDEALISTA
	FROM [HPL].[dbo].[Idealista_PY];
DROP TABLE IF EXISTS #TMP_IDEALISTA2;
	SELECT
		[HEADER]
		,trim(lower(replace(right([Header], CHARINDEX(',', REVERSE([Header]))), ',', ''))) as Map
		,CAST(REPLACE(left([PRICE], CHARINDEX('€', [PRICE])-1), '.', '') AS INT) as PRICE
		,cast(SUBSTRING([ROOMS], 2, 3) as int) as ROOMS
		,cast(left([AREA], CHARINDEX(' ', [AREA])) AS INT) as AREA
		,CASE WHEN [FLOOR] LIKE '%º%' then CAST(left([FLOOR], CHARINDEX('º', [FLOOR])-1) AS INT)
			WHEN [FLOOR] LIKE '%Rés do chão%' then 0
			WHEN [FLOOR] LIKE '%Cave%' then -1
			ELSE NULL END AS [FLOOR_NR]
		,CASE WHEN [FLOOR] LIKE '%sem elevador%' then 'sem elevador'
			WHEN [FLOOR] LIKE '%com elevador%' then 'com elevador'
			END AS [ELEVATOR]
		,[DATE_SCRAPING]
	INTO #TMP_IDEALISTA2
	FROM #TMP_IDEALISTA
	WHERE [ROW_N] = 1
	ORDER BY [HEADER];
DROP TABLE IF EXISTS #TMP_CENTURY;
	SELECT
		[HEADER]
		,[PRICE]
		,case when LOWER([PRICE]) like '%preço%' THEN 99 ELSE 0 END AS NUMERIC_PRICE
		,[ROOMS]
		,[AREA]
		,[BEDROOMS]
		,[MAP]
		,[LINK]
		,[DATE_SCRAPING]
		,ROW_NUMBER() OVER (PARTITION BY [LINK] ORDER BY [DATE_SCRAPING] DESC) AS ROW_N
		--ROW_NUMER allows the same add to stay updated in our database qwithout duplicates
	INTO #TMP_CENTURY
	FROM [HPL].[dbo].[CENTURY_PY];
DROP TABLE IF EXISTS #TMP_CENTURY2;
	SELECT
		[HEADER]
		,CAST(REPLACE(REPLACE(REPLACE(left([PRICE], CHARINDEX('€', [PRICE])-1), '.', ''), ' ', ''), CHAR(160), '') AS INT) as PRICE
		,[ROOMS]
		,CAST(left([AREA], CHARINDEX('m2', [AREA])-1) AS INT) as AREA
		,case when [BEDROOMS] = 'nan' then 0 else
			CAST(REPLACE([BEDROOMS], '.0', '') AS INT)
			END AS BEDROOMS
		,[MAP]
		,CASE 
			-- If there are two or more commas, extract the middle part
			WHEN CHARINDEX(',', [MAP]) > 0 AND CHARINDEX(',', [MAP], CHARINDEX(',', [MAP]) + 1) > 0
				THEN LOWER(LTRIM(SUBSTRING([MAP], 
							  CHARINDEX(',', [MAP]) + 1, 
							  CHARINDEX(',', [MAP], CHARINDEX(',', [MAP]) + 1) - CHARINDEX(',', [MAP]) - 1)))
			-- If there's only one comma, return the last part
			WHEN CHARINDEX(',', [MAP]) > 0
				THEN LOWER(LTRIM(SUBSTRING([MAP], CHARINDEX(',', [MAP]) + 1, LEN([MAP]))))
			-- If no comma is present, return the entire map
			ELSE lower(LTRIM(Map)) END AS MiddleValue
		,[DATE_SCRAPING]
	INTO #TMP_CENTURY2
	FROM #TMP_CENTURY
	WHERE [ROW_N] = 1
		and [NUMERIC_PRICE] = 0
		and CASE WHEN CHARINDEX(',', [MAP]) <= 0 THEN NULL
			ELSE TRIM(LEFT([MAP], CHARINDEX(',', Map)-1)) END like lower('%lisboa%');
DROP TABLE IF EXISTS #TMP_REMAX;
	SELECT
		[TYPE]
		,[PRICE]
		,[ROOMS]
		,[AREA]
		,[MAP]
		,[BEDROOMS]
		,[REALTOR]
		,[DATE_SCRAPING]
		,ROW_NUMBER() OVER (PARTITION BY [TYPE], [ROOMS], [AREA], [BEDROOMS], [REALTOR] ORDER BY [DATE_SCRAPING] DESC) AS ROW_N
		--ROW_NUMER allows the same add to stay updated in our database qwithout duplicates
	INTO #TMP_REMAX
	FROM [HPL].[dbo].[REMAX_PY];
DROP TABLE IF EXISTS #TMP_REMAX2;
	SELECT
		[TYPE]
		,CAST(REPLACE(LEFT([PRICE], CHARINDEX('€', [PRICE])-1), ' ', '') AS INT) as PRICE
		,CAST(IIF(ISNUMERIC([ROOMS]) = 1, [ROOMS], NULL) AS INT) AS ROOMS
		,CAST(LEFT([AREA], CHARINDEX('m2', [AREA])-1) AS INT) AS AREA
		,[MAP]
		,RIGHT([MAP], CHARINDEX(',', REVERSE([MAP]))-1) AS MAP2
		,[BEDROOMS]
		,[DATE_SCRAPING]
		,case when CHARINDEX('-', [MAP]) <> 0 then TRIM(LOWER(RIGHT([MAP], CHARINDEX(',', REVERSE([MAP]))-1)))
			else TRIM(LOWER(LEFT([MAP], CHARINDEX(',', [MAP])-1)))
			end as MiddleValue
	INTO #TMP_REMAX2
	FROM #TMP_REMAX
	WHERE [ROW_N] = 1
	ORDER BY [TYPE];
	
SELECT
	[HEADER]
	,CASE WHEN [Map] = 'alcantara' THEN 'alcântara' ELSE [Map] END AS Map
	,[PRICE]
	,[ROOMS]
	,[AREA]
	,[DATE_SCRAPING]
	, 'IDEALISTA' AS SOURCE
FROM #TMP_IDEALISTA2

UNION ALL

SELECT
	[HEADER]
	,CASE WHEN [MiddleValue] = 'alcantara' THEN 'alcântara' ELSE [MiddleValue] END AS MiddleValue
	,[PRICE]
	,[ROOMS]
	,[AREA]
	,[DATE_SCRAPING]
	, 'CENTURY' AS SOURCE
FROM #TMP_CENTURY2

UNION ALL

SELECT
	[TYPE] AS HEADER
	,CASE WHEN [MiddleValue] = 'alcantara' THEN 'alcântara' ELSE [MiddleValue] END AS MiddleValue
	,[PRICE]
	,[ROOMS]
	,[AREA]
	,[DATE_SCRAPING]
	, 'REMAX' AS SOURCE
FROM #TMP_REMAX2
-- In this solution I am using an agregation of three tables into one, using UNION ALL.
--I am also using temporary tables (#table), this solution saves RAM from the machine and uses more CPU