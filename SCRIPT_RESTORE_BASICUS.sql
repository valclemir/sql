--Restore_Basicus_teste
--Script alterado no dia: 05/04/2018
USE MASTER 
ALTER DATABASE [Basicus_teste]
SET SINGLE_USER 
WITH ROLLBACK IMMEDIATE;
GO

--DROP DATABASE Basicus_teste
GO



RESTORE DATABASE [Basicus_teste] FROM
DISK = N'\\10.85.0.6\sqlbkp\Basicus_backup_2019-03-29 18-55-34.bak' WITH  FILE = 1, replace,

MOVE N'DbCurator_Data' TO N'H:\Data\Basicus_Teste.mdf',
MOVE N'Basicus_Data01' TO N'H:\Data\Basicus_Teste_1.ndf',
MOVE N'Basicus_Data02' TO N'H:\Data\Basicus_Teste_2.ndf',
MOVE N'Basicus_Data03' TO N'H:\Data\Basicus_Teste_3.ndf',
MOVE N'Basicus_Data04' TO N'H:\Data\Basicus_Teste_4.ndf',
MOVE N'Basicus_Data05' TO N'H:\Data\Basicus_Teste_5.ndf',
MOVE N'Basicus_Data06' TO N'H:\Data\Basicus_Teste_6.ndf',
MOVE N'Basicus_Data07' TO N'H:\Data\Basicus_Teste_7.ndf',
MOVE N'Basicus_Data08' TO N'H:\Data\Basicus_Teste_8.ndf',
MOVE N'Basicus_Data09' TO N'H:\Data\Basicus_Teste_9.ndf',
MOVE N'Basicus_Data10' TO N'H:\Data\Basicus_Teste_10.ndf',
MOVE N'Basicus_Data11' TO N'H:\Data\Basicus_Teste_11.ndf',
MOVE N'Basicus_Data12' TO N'G:\Data\Basicus_Teste_12.ndf',
--Move para a partição G
MOVE N'Basicus_Data13' TO N'G:\Data\Basicus_Teste_13.ndf',
MOVE N'Basicus_Data14' TO N'G:\DATA\Basicus_Teste_14.ndf',
MOVE N'Basicus_Data15' TO N'E:\DATA\Basicus_Teste_15.ndf',
MOVE N'Basicus_Indices01' TO N'E:\DATA\Basicus_Teste_16.ndf',
MOVE N'Basicus_Indices02' TO N'E:\DATA\Basicus_Teste_17.ndf',
MOVE N'Basicus_Indices03' TO N'E:\DATA\Basicus_Teste_18.ndf',
--Move para partição E
MOVE N'Basicus_Indices04' TO N'E:\DATA\Basicus_Teste_19.ndf',
MOVE N'Basicus_Indices05' TO N'E:\DATA\Basicus_Teste_20.mdf',
MOVE N'Basicus_Indices06' TO N'E:\DATA\Basicus_Teste_21.mdf',
MOVE N'Basicus_Indices07' TO N'E:\DATA\Basicus_Teste_22.mdf',
MOVE N'Basicus_Indices08' TO N'E:\DATA\Basicus_Teste_23.mdf',
MOVE N'Basicus_Indices09' TO N'E:\DATA\Basicus_Teste_24.mdf',
MOVE N'Basicus_Indices10' TO N'E:\DATA\Basicus_Teste_25.mdf',
MOVE N'Basicus_Indices11' TO N'E:\DATA\Basicus_Teste_26.mdf',
MOVE N'Basicus_Indices12' TO N'E:\DATA\Basicus_Teste_27.mdf',
MOVE N'Basicus_Indices13' TO N'E:\DATA\Basicus_Teste_28.mdf',
MOVE N'Basicus_Indices14' TO N'E:\DATA\Basicus_Teste_29.mdf',
MOVE N'Basicus_Indices15' TO N'E:\DATA\Basicus_Teste_30.mdf',
MOVE N'Basicus_Indices16' TO N'E:\DATA\Basicus_Teste_31.mdf',
MOVE N'DbCurator_Log' TO N'E:\DATA\Basicus_Teste_32.ldf',
MOVE N'DbCurator_Log01' TO N'E:\DATA\Basicus_Teste_33.ldf',

STATS = 1

ALTER DATABASE [Basicus_teste]
SET MULTI_USER 
WITH ROLLBACK IMMEDIATE;

-- ADD USUARIOS ORFÃOS
GO
USE Basicus_teste
GO
--CRIA USUÁRIOS 
IF NOT EXISTS (SELECT name FROM sys.sysusers WHERE name = 'devTeste')
	BEGIN 
		CREATE USER DevTeste FROM LOGIN [devTeste]
	END 
GO
ALTER ROLE DB_DATAREADER ADD MEMBER devTeste
GO
ALTER ROLE DB_DATAWRITER ADD MEMBER devTeste
GO
ALTER ROLE DB_DDLADMIN ADD MEMBER devTeste
GO

-- USER RAMIRO
IF NOT EXISTS (SELECT name FROM sys.sysusers WHERE name = 'ODONTOSYSTEM\ramirofrancisco')
	BEGIN 
		CREATE USER [ODONTOSYSTEM\ramirofrancisco] FROM LOGIN [ODONTOSYSTEM\ramirofrancisco]
	END 

ALTER ROLE DB_DATAREADER ADD MEMBER [ODONTOSYSTEM\ramirofrancisco]
GO
ALTER ROLE DB_DATAWRITER ADD MEMBER [ODONTOSYSTEM\ramirofrancisco]
GO
ALTER ROLE db_ddladmin  ADD MEMBER [ODONTOSYSTEM\ramirofrancisco]
GO
-- USER KATIANE
IF NOT EXISTS (SELECT name FROM sys.sysusers WHERE name = 'ODONTOSYSTEM\katianechagas')
	BEGIN 
		CREATE USER [ODONTOSYSTEM\katianechagas] FROM LOGIN [ODONTOSYSTEM\katianechagas]
	END 
GO
ALTER ROLE DB_DATAREADER ADD MEMBER [ODONTOSYSTEM\katianechagas]
GO
ALTER ROLE DB_DATAWRITER ADD MEMBER [ODONTOSYSTEM\katianechagas]
GO
ALTER ROLE db_ddladmin  ADD MEMBER [ODONTOSYSTEM\katianechagas]
GO

-- USER CINARA
IF NOT EXISTS (SELECT name FROM sys.sysusers WHERE name = 'ODONTOSYSTEM\cicera.lima')
	BEGIN 
		CREATE USER [ODONTOSYSTEM\cicera.lima] FROM LOGIN [ODONTOSYSTEM\cicera.lima]
	END 
GO
ALTER ROLE DB_DATAREADER ADD MEMBER [ODONTOSYSTEM\cicera.lima]
GO
ALTER ROLE DB_DATAWRITER ADD MEMBER [ODONTOSYSTEM\cicera.lima]
GO
ALTER ROLE db_ddladmin  ADD MEMBER [ODONTOSYSTEM\cicera.lima]
GO

--USER DavidSousa
IF NOT EXISTS (SELECT name FROM sys.sysusers WHERE name = 'ODONTOSYSTEM\davidsousa')
	BEGIN 
		CREATE USER [ODONTOSYSTEM\davidsousa] FROM LOGIN [ODONTOSYSTEM\davidsousa]
	END 
GO
ALTER ROLE DB_DATAREADER ADD MEMBER [ODONTOSYSTEM\davidsousa]
GO
ALTER ROLE DB_DATAWRITER ADD MEMBER [ODONTOSYSTEM\davidsousa]
GO
ALTER ROLE db_ddladmin  ADD MEMBER [ODONTOSYSTEM\davidsousa]

GO

-- DROPA E RECRIA SYNONYM

DROP TABLE IF EXISTS #Sinonimos
SELECT name,
	   REPLACE( 
	   REPLACE(
	   REPLACE(
	   REPLACE(
	   REPLACE(
	   REPLACE(
				base_object_name, 'SIAPWEB', 'SIAPWEB_TESTE')
				, 'ANSDB', 'ANSDB_TESTE')
				, 'ODONTOBASE', 'ODONTOBASETESTE')
				, 'GerenciadorDocumentosDB', 'GerenciadorDocumentosDB_Teste')
				, 'CorporeBD1', 'CorporeBD1_Teste')
				, 'Gerencial', 'Gerencial_Teste') 'EsquemaSinonimos'
	  ,base_object_name
INTO #Sinonimos
FROM sys.synonyms

DECLARE @Name VARCHAR(50),
		@EsquemaSinonimos VARCHAR(100),
		@Base_object_name VARCHAR(100),
		@COMANDO VARCHAR(1000) 

DECLARE Percorre CURSOR FOR 
		SELECT  name,
			   EsquemaSinonimos,
			   base_object_name
		FROM #Sinonimos
OPEN Percorre 
FETCH NEXT FROM Percorre INTO @name, @EsquemaSinonimos, @Base_object_name

WHILE @@FETCH_STATUS = 0 
	BEGIN 
		SET @COMANDO = ' DROP SYNONYM IF EXISTS ' + @name
		EXEC (@COMANDO) 
		SET @COMANDO = ' CREATE SYNONYM '+@name + ' FOR '+@EsquemaSinonimos 
		EXEC (@COMANDO) 
	FETCH NEXT FROM Percorre INTO @name, @EsquemaSinonimos, @Base_object_name
	END 
CLOSE Percorre 
DEALLOCATE Percorre 

