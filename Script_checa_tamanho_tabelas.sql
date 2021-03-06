CREATE PROC #T (
	@acao INT ,
	@nomeBanco VARCHAR(30) = NULL
)
AS
BEGIN
	DROP TABLE IF EXISTS #SpaceUsed
	CREATE TABLE #SpaceUsed (
		Name VARCHAR(100),
		Rows BIGINT,
		Space_reserved VARCHAR(50),
		Data VARCHAR(50),
		Index_size VARCHAR(50),
		Space_unused VARCHAR(50)
	)

	DECLARE @var VARCHAR(100)
	DECLARE percorre CURSOR FOR 
	
		SELECT name FROM sys.tables
			WHERE name NOT IN ('ANS_SIB_Beneficiario_TempAdesoesSetembro')
	OPEN   percorre
	FETCH NEXT FROM percorre INTO @var 
	WHILE @@FETCH_STATUS = 0
		BEGIN
		
			INSERT INTO #SpaceUsed
			EXEC SP_SPACEUSED  @var
		
			FETCH NEXT FROM percorre INTO @var 
		END
	CLOSE percorre
	DEALLOCATE percorre 



	IF @acao = 1
	BEGIN
		SELECT name,
				rows,
				CASE WHEN 
						LEN(LTRIM(RTRIM(REPLACE(REPLACE(SPACE_reserved, 'KB', ''), 'GB', '')))) > 6
						THEN CONVERT(VARCHAR(MAX), (CONVERT(BIGINT, LTRIM(RTRIM(REPLACE(REPLACE(SPACE_reserved, 'KB', ''), 'GB', ''))) /1024/1024))) + ' GB'
						ELSE CONVERT(VARCHAR(MAX), (CONVERT(BIGINT, LTRIM(RTRIM(REPLACE(REPLACE(SPACE_reserved, 'KB', ''), 'GB', '')))) /1024 )) + ' MB' END Space_reserved,
				CASE WHEN 
						LEN(LTRIM(RTRIM(REPLACE(REPLACE(Data, 'KB', ''), 'GB', '')))) > 6
						THEN CONVERT(VARCHAR(MAX), (CONVERT(BIGINT, LTRIM(RTRIM(REPLACE(REPLACE(Data, 'KB', ''), 'GB', ''))) /1024/1024))) + ' GB'
						ELSE CONVERT(VARCHAR(MAX), (CONVERT(BIGINT, LTRIM(RTRIM(REPLACE(REPLACE(Data, 'KB', ''), 'GB', '')))) /1024)) + ' MB' END Data,
				CASE WHEN 
						LEN(LTRIM(RTRIM(REPLACE(REPLACE(Index_size, 'KB', ''), 'GB', '')))) > 6
						THEN CONVERT(VARCHAR(MAX), (CONVERT(BIGINT, LTRIM(RTRIM(REPLACE(REPLACE(Index_size, 'KB', ''), 'GB', ''))) /1024/1024))) + ' GB'
						ELSE CONVERT(VARCHAR(MAX), (CONVERT(BIGINT, LTRIM(RTRIM(REPLACE(REPLACE(Index_size, 'KB', ''), 'GB', '')))) /1024)) + ' MB' END Index_size,
				CASE WHEN 
						LEN(LTRIM(RTRIM(REPLACE(REPLACE(Space_unused, 'KB', ''), 'GB', '')))) > 6
						THEN CONVERT(VARCHAR(MAX), (CONVERT(BIGINT, LTRIM(RTRIM(REPLACE(REPLACE(Space_unused, 'KB', ''), 'GB', ''))) /1024/1024))) + ' GB'
						ELSE CONVERT(VARCHAR(MAX), (CONVERT(BIGINT, LTRIM(RTRIM(REPLACE(REPLACE(Space_unused, 'KB', ''), 'GB', '')))) /1024)) + ' MB' END Space_unused
		

		FROM (
			SELECT name, 
								rows, 
								Space_reserved, 
								CONVERT(BIGINT, REPLACE(REPLACE(data, 'KB', ''), 'GB', '')) Data,
								index_size,
								space_unused 
					FROM #SpaceUsed 
				) TB

		WHERE Tb.Name NOT LIKE '%LOG%'
		ORDER BY Tb.Data DESC 
	END
	ELSE IF @acao = 2	
	BEGIN	
		SELECT DB_NAME(database_id) AS DatabaseName,
			   SUM((size * 8) / 1024) SizeMB
		FROM sys.master_files
		WHERE DB_NAME(database_id) = @nomeBanco COLLATE Latin1_General_CI_AS  
		GROUP BY DB_NAME(database_id)
	END

END




EXEC #T 2 ,'ceps'