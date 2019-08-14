-- PARTE 1 Trigger DDL

DROP TRIGGER IF EXISTS TG_AuditoriaDLL ON ALL SERVER 
GO
DROP TABLE IF EXISTS auditoria_teste.dbo.auditoriaDDL
GO
CREATE TABLE auditoria_teste.dbo.AuditoriaDDL (
	Nome_login VARCHAR(50),
	host_name VARCHAR(50),
	ipCliente VARCHAR(20),
	Nome_database VARCHAR(50),
	NomeApp VARCHAR(100),
	NomeSchema VARCHAR(20),
	NomeObjetoCriado VARCHAR(50),
	TipoComando VARCHAR(50),
	ComandoSQL VARCHAR(8000),
	DataComando DATETIME2(3)
)
GO

CREATE TRIGGER TG_AuditoriaDLL
ON ALL SERVER 
FOR CREATE_DATABASE, ALTER_DATABASE, DROP_DATABASE
	,CREATE_PROCEDURE, ALTER_PROCEDURE, DROP_PROCEDURE
	,CREATE_TABLE, ALTER_TABLE, DROP_TABLE
	,CREATE_FUNCTION, ALTER_FUNCTION, DROP_FUNCTION
	,CREATE_SCHEMA, ALTER_SCHEMA, DROP_SCHEMA
	,CREATE_USER, DROP_USER
	,CREATE_LOGIN, DROP_LOGIN
AS
    BEGIN
		SET NOCOUNT ON
	 
		DECLARE @EventData XML = EventData()
		INSERT INTO auditoria_teste.dbo.AuditoriaDDL
		SELECT 
				--@EventData.value('data(/EVENT_INSTANCE/SPID)[1]','VARCHAR(100)') session_id,
				@EventData.value('data(/EVENT_INSTANCE/LoginName)[1]','VARCHAR(100)') NomeLogin,
				HOST_NAME() HostName,
				(SELECT client_net_address FROM sys.dm_exec_connections WHERE session_id = @@SPID) IpCliente,
				@EventData.value('data(/EVENT_INSTANCE/DatabaseName)[1]','VARCHAR(100)') NomeDatabase,
				APP_NAME() NomeAPP,
				@EventData.value('data(/EVENT_INSTANCE/SchemaName)[1]','VARCHAR(100)') NomeSchema,
				@EventData.value('data(/EVENT_INSTANCE/ObjectName)[1]','VARCHAR(100)') NomeObjetoCriado,
				@EventData.value('data(/EVENT_INSTANCE/EventType)[1]','VARCHAR(100)') TipoComando,
				@EventData.value('data(/EVENT_INSTANCE/TSQLCommand/CommandText)[1]','VARCHAR(8000)') ComandoSQL,
				CONVERT(DATETIME2(3), @EventData.value('data(/EVENT_INSTANCE/PostTime)[1]','VARCHAR(100)')) DataComando
	END 

GO
-- PART 2 TG_AuditoriaLogon

DROP TRIGGER IF EXISTS TG_AuditoriaLogon ON ALL SERVER
GO
DROP TABLE IF EXISTS auditoria_teste.dbo.AuditoriaLogon 
GO
CREATE TABLE auditoria_teste.dbo.AuditoriaLogon (
	Ds_Login VARCHAR(50),
	Ds_Host VARCHAR(50),
	Ds_IpCliente VARCHAR(20),
	Ds_NomeApp VARCHAR(100),
	DataLogin DATETIME2(0)
)
GO

CREATE TRIGGER TG_AuditoriaLogon 
ON ALL SERVER 
FOR LOGON 
AS 
	BEGIN 
		SET NOCOUNT ON 
			DECLARE 
					@spid INT,
					@Ds_login VARCHAR(50),
					@Ds_Host VARCHAR(50),
					@Ds_IpCliente VARCHAR(20),
					@Ds_NomeApp VARCHAR(100), 
					@DataLogin DATETIME2(0)
			IF APP_NAME() LIKE 'Microsoft SQL Server Management Studio%' OR APP_NAME() LIKE 'HeidiSQL%' OR APP_NAME() LIKE 'Azure Data Studio%'
				BEGIN
					SET @Ds_login = EVENTDATA().value('data(/EVENT_INSTANCE/LoginName)[1]','VARCHAR(100)')
					SET @Ds_Host = HOST_NAME()
					SET @Ds_IpCliente = EVENTDATA().value('(/EVENT_INSTANCE/ClientHost)[1]', 'varchar(50)')
					SET @Ds_NomeApp = APP_NAME()
					SET @DataLogin = CONVERT(DATETIME2(2), EVENTDATA().value('data(/EVENT_INSTANCE/PostTime)[1]','VARCHAR(100)'))
									
					DECLARE @TableVariable TABLE (
						
						ds_login VARCHAR(50),
						Ds_host VARCHAR(50),
						ds_ipCliente VARCHAR(20),
						ds_nomeApp VARCHAR(50),
						DataLogin DATETIME2(0)
					)	
				
					INSERT INTO @TableVariable 
					SELECT  @Ds_login, @Ds_host, @Ds_IpCliente, @Ds_NomeApp, @DataLogin
				
				
				
					SELECT DISTINCT * INTO #Temp FROM @TableVariable
					INSERT INTO auditoria_teste.dbo.AuditoriaLogon (Ds_login, Ds_Host, Ds_IpCliente, Ds_NomeApp, DataLogin)
					SELECT DISTINCT  * FROM #Temp
				END
	END 



SELECT DISTINCT    * FROM auditoria_teste.dbo.AuditoriaLogon
ORDER BY DataLogin DESC


SELECT T1.name, T2.name FROM sys.stats T1
INNER JOIN sys.Tables T2 
	ON (T1.object_id = T2.object_id)


UPDATE STATISTICS SOS3_Paciente WITH FULLSCAN





SELECT * FROM sys.syslogins
WHERE name LIKE '%roni%'


GRANT EXECUTE ON siControleHoras TO [BUILTIN\Administrators]

SELECT * FROM sys.database_permissions
SP_HELPTEXT siControleHoras




DROP TABLE IF EXISTS Log_spWhoisactive
GO
CREATE TABLE Log_spWhoisactive (
	[Data_Log] DATETIME2(3),
	session_id INT,
	Comando XML,
	login_name VARCHAR(50),
	host_name VARCHAR(50),
	Database_name VARCHAR(50),
	program_name VARCHAR(100),
) 
GO

DROP TABLE IF EXISTS #Temp
GO
CREATE TABLE #temp  (
      Dt_Log DATETIME ,
      [dd hh:mm:ss.mss] VARCHAR(8000) NULL ,
      [database_name] VARCHAR(128) NULL ,
      [session_id] SMALLINT NOT NULL ,
      blocking_session_id SMALLINT NULL ,
      [sql_text] XML NULL ,
      [login_name] VARCHAR(128) NOT NULL ,
      [wait_info] VARCHAR(4000) NULL ,
      [status] VARCHAR(30) NOT NULL ,
      [percent_complete] VARCHAR(30) NULL ,
      [host_name] VARCHAR(128) NULL ,
      [sql_command] XML NULL ,
      [CPU] VARCHAR(100) ,
      [reads] VARCHAR(100) ,
      [writes] VARCHAR(100),
	  [Program_Name] VARCHAR(100)
    );      
GO
EXEC sp_WhoIsActive @get_outer_command = 1,
            @output_column_list = '[collection_time][d%][session_id][blocking_session_id][sql_text][login_name][wait_info][status][percent_complete]
      [host_name][database_name][sql_command][CPU][reads][writes][program_name]',
    @destination_table = '#temp'

GO

INSERT INTO Log_spWhoisactive
SELECT 
	   Dt_log, 
	   session_id, 
	   sql_text, 
	   login_name, 
	   host_name, 
	   database_name, 
	   program_name
FROM #temp
WHERE CAST(sql_text AS VARCHAR(8000)) NOT IN (
			SELECT CAST(comando AS VARCHAR(8000)) FROM Log_spWhoisactive)
AND session_id
