USE MASTER 
GO
DROP PROCEDURE IF EXISTS SP_IMPORT_EXCEL_CSV
GO
CREATE PROCEDURE SP_IMPORT_EXCEL_CSV (
	@tipoFile VARCHAR(30),
	@NomeBanco VARCHAR(30),
	@NomeTabela VARCHAR(70),
	@fieldTerminator CHAR(1) = NULL,
	@NomePaginaPlanilhas VARCHAR(60) = NULL,
	@Caminho VARCHAR(200),
	@NameFileCsv VARCHAR(200) = NULL 
)
-- EXEMPLO 
/*Importando XLSX
EXEC SP_IMPORT_EXCEL_CSV    @tipoFile = 'xlsx',
							@NomeBanco = 'basicus_teste',
							@NomeTabela = 'tmp',
							@fieldTerminator = NULL,
							@NomePaginaPlanilhas = 'criticas_07', --name sheets
							@caminho = '\\10.85.2.97\Repositorio\Relatório_Críticas_Estado_SSA_Funprev_TCM_Julho.xlsx',
							@NameFileCsv = NULL 

IMPORTANTO CSV
EXEC SP_IMPORT_EXCEL_CSV @tipofile = 'CSV',
							@NomeBanco = 'basicus_teste',
							@NomeTabela = 'tmp',
							@fieldTerminator = NULL,
							@NomePaginaPlanilhas = NULL, 
							@caminho = '\\10.85.2.97\repositorio', --Sem o nome do arquivo
							@NameFileCsv = 'Relatório_Críticas_Estado_SSA_Funprev_TCM_Julho.csv'
*/
AS 
	BEGIN  
		IF @tipoFile LIKE '%XLS%' AND DATALENGTH(@NomeTabela) <= 60
			BEGIN 
				BEGIN TRY 
					BEGIN TRAN 
						DECLARE @CommandDrop NVARCHAR(1000), @CommandXls NVARCHAR(1000)
						SET @CommandDrop = 'DROP TABLE IF EXISTS '+ @NomeTabela
						EXEC SP_EXECUTESQL @CommandDrop
						SET @CommandXls = N'SELECT * INTO '+@Nomebanco+'.dbo.'+@NomeTabela+' FROM OPENROWSET (''Microsoft.ACE.OLEDB.12.0''
						,''Excel 12.0;Database='+@Caminho+''', ['+@NomePaginaPlanilhas+'$])'
						IF EXISTS (SELECT * FROM sys.databases WHERE name = @NomeBanco)
							EXEC SP_EXECUTESQL @CommandXls
					COMMIT TRAN 
				END TRY
				BEGIN CATCH 
					IF @@TRANCOUNT <> 0 
						RAISERROR ('Verifique se o caminho e o nome do banco de dados está correto e tente novamente!', 15, 1)
						ROLLBACK 
				END CATCH 
			END
		ELSE  IF @tipoFile LIKE '%CSV%' AND DATALENGTH(@NomeTabela) <= 60
			BEGIN 
				BEGIN TRY 
					BEGIN TRAN 
						DECLARE @CommandCsv NVARCHAR(1000)
						/*SET @CommandCsv = 'BULK INSERT '+@NomeBanco+'.dbo.'+@Nometabela+'
							FROM '''+@Caminho+'''
								WITH
								(
										FIELDTERMINATOR = '''+@fieldTerminator+''',
										ROWTERMINATOR = ''\n''
								)'
						EXEC SP_EXECUTESQL @CommandCsv*/
						SET @CommandCsv = N'SELECT * INTO '+@NomeBanco+'.dbo.'+@NomeTabela+' FROM OPENROWSET(''Microsoft.ACE.OLEDB.12.0'',
										''text;Database='+@Caminho+''',''SELECT * FROM '+@NameFileCsv+''')'
						IF EXISTS (SELECT * FROM sys.databases WHERE name = @NomeBanco)
							EXEC SP_EXECUTESQL @CommandCsv 
						ELSE 
							RAISERROR('ERROR', 15, 1)
					COMMIT TRAN 
				END TRY 
				BEGIN CATCH 
					IF @@TRANCOUNT <> 0 
						RAISERROR ('Verifique se o caminho e o nome do banco de dados está correto e tente novamente!', 15, 1)
						ROLLBACK
				END CATCH 
			
			END	
	END

GO





