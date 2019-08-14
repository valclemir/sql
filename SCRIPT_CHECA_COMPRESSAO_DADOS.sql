


DROP TABLE IF EXISTS #SpaceUsed
GO
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



GO





DROP TABLE IF EXISTS #dados_comprimidos
GO
CREATE TABLE #dados_comprimidos (
	nomeObjeto VARCHAR(100),
	NomeSchema VARCHAR(10),
	index_id INT,
	partition_number INT,
	Tamanho_atual_tabela VARCHAR(100),
	Tamanho_apos_compressao VARCHAR(100),
	amostragem_tamanho_atual_tabela VARCHAR(100),
	amostragem_apos_compressao VARCHAR(100)
)
DECLARE @Name VARCHAR(1000)
DECLARE percorre CURSOR FOR  

SELECT name  /*SUM(CONVERT(INT, RTRIM(REPLACE(data,  'GB',  ''))))  'Tamanho tabelas logs(GB)'*/
 FROM (
SELECT name,
		rows,
		CASE WHEN 
				LEN(LTRIM(RTRIM(REPLACE(REPLACE(SPACE_reserved, 'KB', ''), 'GB', '')))) > 6
				THEN CONVERT(VARCHAR(MAX), (CONVERT(BIGINT, LTRIM(RTRIM(REPLACE(REPLACE(SPACE_reserved, 'KB', ''), 'GB', ''))) /1024/1024))) + ' GB'
				ELSE CONVERT(VARCHAR(MAX), (CONVERT(BIGINT, LTRIM(RTRIM(REPLACE(REPLACE(SPACE_reserved, 'KB', ''), 'GB', '')))) /1024 )) + ' MB' END Space_reserved,
		CASE WHEN 
				LEN(LTRIM(RTRIM(REPLACE(REPLACE(Data, 'KB', ''), 'GB', '')))) > 6
				THEN CONVERT(VARCHAR(MAX), (CONVERT(BIGINT, LTRIM(RTRIM(REPLACE(REPLACE(Data, 'KB', ''), 'GB', ''))) /1024/1024)))  + ' GB'
				ELSE CONVERT(VARCHAR(MAX), (CONVERT(BIGINT, LTRIM(RTRIM(REPLACE(REPLACE(Data, 'KB', ''), 'GB', '')))) /1024)) + ' MB'  END Data,
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

WHERE Tb.Name  LIKE '%LOG%'
) AS TB
WHERE tb.data NOT LIKE '%mb%' AND tb.name NOT LIKE '%Comissionados_Adesao_log%'
AND tb.name NOT IN (SELECT nomeObjeto FROM #dados_comprimidos)
-- ORDER BY tb.data DESC 
OPEN  percorre
FETCH NEXT FROM percorre INTO @Name
WHILE @@FETCH_STATUS = 0
	BEGIN 
		
		INSERT INTO #dados_comprimidos
		EXEC sp_estimate_data_compression_savings 'dbo', @name, NULL, NULL, 'PAGE' ;  

		FETCH NEXT FROM percorre INTO @Name
	END
CLOSE percorre
DEALLOCATE percorre 


SELECT * FROM #dados_comprimidos
SELECT (SUM(CONVERT(BIGINT, Tamanho_atual_tabela)) / 1024) / 1024  FROM #dados_comprimidos
SELECT (SUM(CONVERT(BIGINT, Tamanho_apos_compressao)) / 1024) / 1024  FROM #dados_comprimidos

