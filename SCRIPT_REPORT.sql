/*********************************************************************************************
(C) 2015, Fabricio Lima Solu��es em Banco de Dados

Site: www.fabriciolima.net

Feedback: fabricioflima@gmail.com
*********************************************************************************************/

--Coloque o nome da base onde voc� pretende guardar esse Log. Por favor, n�o guarde na master ou outra base de sistema. Obrigado.
/*USE Basicus 
GO

if OBJECT_ID('Resultado_WhoisActive') is not null
	drop table Resultado_WhoisActive

CREATE TABLE Resultado_WhoisActive  (
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

  USE [msdb]
GO

/****** Object:  Job [DBA - Carga Whoisactive]    Script Date: 04/23/2014 19:59:41 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Database Maintenance]    Script Date: 04/23/2014 19:59:41 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DBA - Carga Whoisactive', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'DBA - WhoisActive', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC sp_WhoIsActive @get_outer_command = 1,
            @output_column_list = ''[collection_time][d%][session_id][blocking_session_id][sql_text][login_name][wait_info][status][percent_complete]
      [host_name][database_name][sql_command][CPU][reads][writes][program_name]'',
    @destination_table = ''Resultado_WhoisActive''
', 
		@database_name=N'Basicus', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'DBA - WhoisActive', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=1, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20180917, 
		@active_end_date=99991231, 
		@active_start_time=70000, 
		@active_end_time=230000, 
		@schedule_uid=N'c8a3eb26-b2ed-456d-8c4d-ae7c95e88163'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

*/

SP_WHOISACTIVE 


SP_SPACEUSED [Resultado_WhoisActive]

USE Basicus 
SELECT * FROM [Resultado_WhoisActive]

-- Traz todas as rotinas executadas hoje
SELECT  [Dt_Log]
      ,[dd hh:mm:ss.mss]
      ,[database_name]
      ,[session_id]
      ,[blocking_session_id]
      ,[sql_text]
      ,[login_name]
      ,[wait_info]
      ,[status]
      ,[percent_complete]
      ,[host_name]
      ,[sql_command]
      ,[CPU]
      ,[reads]
      ,[writes]
      ,[Program_Name]
FROM [Basicus].[dbo].[Resultado_WhoisActive]
WHERE DATEPART(dd, DT_LOG) = DATEPART(dd, GETDATE())
ORDER BY DT_log ASC


USE Auditoria


SELECT * FROM sys.procedures 
WHERE name LIKE '%portfolioclientes_odontologia_acompanhamento_ANS_enviar_email%'

-- Traz todas as rotinas executadas na ultima hora de hoje
SELECT  [Dt_Log]
      ,[dd hh:mm:ss.mss]
      ,[database_name]
      ,[session_id]
      ,[blocking_session_id]
      ,[sql_text]
      ,[login_name]
      ,[wait_info]
      ,[status]
      ,[percent_complete]
      ,[host_name]
      ,[sql_command]
      ,[CPU]
      ,[reads]
      ,[writes]
      ,[Program_Name]
FROM [Basicus].[dbo].[Resultado_WhoisActive]
WHERE DATEPART(dd, DT_LOG) = DATEPART(dd, GETDATE()) 
	  AND DATEPART(hh, DT_LOG) = DATEPART(hh, GETDATE()) AND blocking_session_id IS NOT NULL 
	  --AND DATEPART(MINUTE, DT_LOG) = DATEPART(MINUTE, GETDATE())
	  --AND session_id = 195
ORDER BY blocking_session_id DESC


-- Traz todas as rotinas executadas no ultimo de minute 
SELECT  [Dt_Log]
      ,[dd hh:mm:ss.mss]
      ,[database_name]
      ,[session_id]
      ,[blocking_session_id]
      ,[sql_text]
      ,[login_name]
      ,[wait_info]
      ,[status]
      ,[percent_complete]
      ,[host_name]
      ,[sql_command]
      ,[CPU]
      ,[reads]
      ,[writes]
      ,[Program_Name]
FROM [Basicus].[dbo].[Resultado_WhoisActive]
WHERE DATEPART(dd, DT_LOG) = DATEPART(dd, GETDATE()) 
	  AND DATEPART(hh, DT_LOG) = DATEPART(hh, GETDATE())
	  AND DATEPART(MINUTE, DT_LOG) = DATEPART(MINUTE, GETDATE())
ORDER BY blocking_session_id DESC

 



-- Traz todas as rotinas executadas nos ultimos 10 minutos 
SELECT  [Dt_Log]
      ,[dd hh:mm:ss.mss]
      ,[database_name]
      ,[session_id]
      ,[blocking_session_id]
      ,[sql_text]
      ,[login_name]
      ,[wait_info]
      ,[status]
      ,[percent_complete]
      ,[host_name]
      ,[sql_command]
      ,[CPU]
      ,[reads]
      ,[writes]
      ,[Program_Name]
FROM [Basicus].[dbo].[Resultado_WhoisActive]
WHERE DATEPART(dd, DT_LOG) = DATEPART(dd, GETDATE()) 
	  AND DATEPART(hh, DT_LOG) = DATEPART(hh, GETDATE())
	  AND DATEPART(MINUTE, DT_LOG) = DATEPART(MINUTE, DATEADD(MINUTE, -10, GETDATE()))
ORDER BY blocking_session_id DESC





WITH CTE
AS (
SELECT DISTINCT [Hora do blocking]
	  ,CASE WHEN (blocking_session_id IS NULL) THEN 0
	  WHEN blocking_session_id IS NOT NULL THEN 1 ELSE blocking_session_id END blocking_session_id
	  
FROM (
SELECT CONVERT(TIME(3), Dt_log) [Hora do blocking]
      ,SUBSTRING([dd hh:mm:ss.mss], 3, 100)  [hh:mm:ss.mss]
      ,[database_name]
      ,[session_id]
      ,[blocking_session_id]
      ,CAST([sql_text] AS VARCHAR(MAX)) AS [sql_text]
      ,[login_name]
      ,[wait_info]
      ,[status]
      ,[percent_complete]
      ,[host_name]
      ,[sql_command]
      ,[CPU]
      ,[reads]
      ,[writes]
      ,[Program_Name]
FROM [Basicus].[dbo].[Resultado_WhoisActive]
/*WHERE DATEPART(dd, DT_LOG) = DATEPART(dd, GETDATE()) 
	  AND DATEPART(hh, DT_LOG) = DATEPART(hh, GETDATE())
	  --AND DATEPART(MINUTE, DT_LOG) = DATEPART(MINUTE, GETDATE())*/
) as t         
--WHERE blocking_session_id = 0
--GROUP BY [Hora do blocking], blocking_session_id, session_id
)

SELECT [Hora do blocking],blocking_session_id FROM CTE 
--WHERE blocking_session_id = 1
--GROUP BY [Hora do blocking], blocking_session_id
ORDER BY [Hora do blocking] ASC 





--Quantidade total de locks por minuto
WITH CTE
AS (
SELECT  SUBSTRING(CONVERT(VARCHAR(MAX), [Hora do blocking]), 1, 16)  [Hora do blocking]
	   --MAX([hh:mm:ss.mss]) [hh:mm:ss.mss]
	  ,CASE WHEN (blocking_session_id IS NULL) THEN 0
	  --WHEN blocking_session_id IS NOT NULL THEN 1 
	  WHEN (blocking_session_id) IS NOT NULL THEN 1
	  ELSE blocking_session_id END blocking_session_id 
	  ,[sql_text]  
	  ,session_id
	  ,[database_name]
	  
FROM (
SELECT CONVERT(DATETIME2(0), DT_LOG)  AS [Hora do blocking]
      ,SUBSTRING([dd hh:mm:ss.mss], 3, 100)  [hh:mm:ss.mss]
      ,[database_name]
      ,[session_id]
      ,[blocking_session_id]
      ,CAST([sql_text] AS VARCHAR(MAX)) AS [sql_text]
      ,[login_name]
      ,[wait_info]
      ,[status]
      ,[percent_complete]
      ,[host_name]
      ,[sql_command]
      ,[CPU]
      ,[reads]
      ,[writes]
      ,[Program_Name]
FROM basicus.[dbo].[Resultado_WhoisActive]
WHERE DATEPART(dd, DT_LOG) = DATEPART(dd, GETDATE()) 
/*	  AND DATEPART(hh, DT_LOG) = DATEPART(hh, GETDATE())
	  --AND DATEPART(MINUTE, DT_LOG) = DATEPART(MINUTE, GETDATE())*/
) as t         
--WHERE blocking_session_id IS NOT NULL  AND sql_text IS NOT NULL 
--GROUP BY blocking_session_id, sql_text, [database_name]
)

SELECT  [Hora do blocking]
		,COUNT(blocking_session_id) qtd_blocking_session_id
		
FROM CTE 
WHERE blocking_session_id = 1 
GROUP BY [Hora do blocking]
ORDER BY [Hora do blocking]




-- Quantidade total de locks por hora
WITH CTE
AS (
SELECT  SUBSTRING(CONVERT(VARCHAR(MAX), [Hora do blocking]), 1, 13)  [Hora do blocking]
	   --MAX([hh:mm:ss.mss]) [hh:mm:ss.mss]
	  ,CASE WHEN (blocking_session_id IS NULL) THEN 0
	  --WHEN blocking_session_id IS NOT NULL THEN 1 
	  WHEN (blocking_session_id) IS NOT NULL THEN 1
	  ELSE blocking_session_id END blocking_session_id 
	  ,[sql_text]  
	  ,session_id
	  ,[database_name]
	  
FROM (
SELECT CONVERT(DATETIME2(0), DT_LOG)  AS [Hora do blocking]
      ,SUBSTRING([dd hh:mm:ss.mss], 3, 100)  [hh:mm:ss.mss]
      ,[database_name]
      ,[session_id]
      ,[blocking_session_id]
      ,CAST([sql_text] AS VARCHAR(MAX)) AS [sql_text]
      ,[login_name]
      ,[wait_info]
      ,[status]
      ,[percent_complete]
      ,[host_name]
      ,[sql_command]
      ,[CPU]
      ,[reads]
      ,[writes]
      ,[Program_Name]
FROM basicus.[dbo].[Resultado_WhoisActive]
--WHERE DATEPART(dd, DT_LOG) = DATEADD(dd, -1, DATEPART(dd, GETDATE())) 
	  --AND DATEPART(hh, DT_LOG) = DATEPART(hh, GETDATE())
	  --AND DATEPART(MINUTE, DT_LOG) = DATEPART(MINUTE, GETDATE())*/
) as t         
--WHERE blocking_session_id IS NOT NULL  AND sql_text IS NOT NULL 
--GROUP BY blocking_session_id, sql_text, [database_name]
)

SELECT  [Hora do blocking]
		,COUNT(blocking_session_id) qtd_blocking_session_id
		
FROM CTE 
WHERE blocking_session_id = 1   
GROUP BY [Hora do blocking]
ORDER BY [Hora do blocking]















-- Quantidade total de locks por hora
WITH CTE
AS (
SELECT  SUBSTRING(CONVERT(VARCHAR(MAX), [Hora do blocking]), 1, 13)  [Hora do blocking]
	   --MAX([hh:mm:ss.mss]) [hh:mm:ss.mss]
	  ,CASE WHEN (blocking_session_id IS NULL) THEN 0
	  --WHEN blocking_session_id IS NOT NULL THEN 1 
	  WHEN (blocking_session_id) IS NOT NULL THEN 1
	  ELSE blocking_session_id END blocking_session_id 
	  ,[sql_text]  
	  ,session_id
	  ,[database_name]
	  
FROM (
SELECT CONVERT(DATETIME2(0), DT_LOG)  AS [Hora do blocking]
      ,SUBSTRING([dd hh:mm:ss.mss], 3, 100)  [hh:mm:ss.mss]
      ,[database_name]
      ,[session_id]
      ,[blocking_session_id]
      ,CAST([sql_text] AS VARCHAR(MAX)) AS [sql_text]
      ,[login_name]
      ,[wait_info]
      ,[status]
      ,[percent_complete]
      ,[host_name]
      ,[sql_command]
      ,[CPU]
      ,[reads]
      ,[writes]
      ,[Program_Name]
FROM basicus.[dbo].[Resultado_WhoisActive]
WHERE DATEPART(dd, DT_LOG) = DATEADD(dd, -1, DATEPART(dd, GETDATE()))
	  --AND DATEPART(hh, DT_LOG) = DATEPART(hh, GETDATE())
	  --AND DATEPART(MINUTE, DT_LOG) = DATEPART(MINUTE, GETDATE())*/
) as t         
--WHERE blocking_session_id IS NOT NULL  AND sql_text IS NOT NULL 
--GROUP BY blocking_session_id, sql_text, [database_name]
)

SELECT  [Hora do blocking]
		,COUNT(blocking_session_id) qtd_blocking_session_id
		
FROM CTE 
WHERE blocking_session_id = 1   --AND [Hora do blocking] = GETDATE()
GROUP BY [Hora do blocking]
ORDER BY [Hora do blocking]



--table variable
DECLARE @table TABLE(
	Name VARCHAR(MAX),
	Rows VARCHAR(MAX),
	Reserved VARCHAR(MAX),
	Data VARCHAR(MAX),
	Index_size VARCHAR(MAX),
	Unused VARCHAR(MAX)
)

--variables 
DECLARE @name VARCHAR(MAX),
		@NameTable VARCHAR(MAX)

DECLARE  TamanhoTotal CURSOR FOR 
	SELECT 
		name 
	FROM sys.tables 
	WHERE /*name IN ('contratos'
					,'localidades_filiais'
					,'log_localidade'
					,'matriz_filiais'
					,'siagenda'
					,'sianamnese'
					,'siautorizacoes'
					,'siclinicas'
					,'siclinicas_tipos'
					,'sicoberturas_planos_procedimentos'
					, 'sicoberturas_planos_procedimentos_faces'
					,'sidentistas'
					,'sidentistas_contratos'
					,'sidentistas_contratos_credenciamento'
					,'sidentistas_contratos_tipos'
					,'sidentistas_formas_pagto'
					,'sidentistas_status_contratos'
					, 'sidentistas_tipos_remuneracao'
					, 'siespecialidades'
					, 'siexameinicial'
					,  'sifaces'
					,'siplanodetratamento'
					,'siplanodetratamento_itens'
					,'siplanodetratamento_modelo'
					,'siprocedimentos')*/
				/*NAME IN ('AMA_00_Historico_Dos_Clientes_Inferencia'
					,'Graph_Fecho_Transitivo_Producao'
					,'Graph_Vetor_Probabilidades'
					,'Clientes_Categorias'
					,'AMA_00_Clinicas_de_Radiologia'
					,'Graph_Inferencia_Vetor_Probabilidades'
					,'AGENTE_AUTORIZADOR_Extracao_De_Dados'
					,'AMA_00_DENTISTAS_COM_CONSULTAS_REMUNERADAS'
					,'AGENTE_AUTORIZADOR_Fatos_Da_Producao_Extraida_V2'
					,'AGENTE_AUTORIZADOR_Localidades_Autorizadas_Resina'
					,'AGENTE_AUTORIZADOR_Localidades_Nao_se_aplica_Dentistas_Ferem_Modelo'
					,'AMA_00_Cadeia_Procedimentos_Protese_Improprios_mesmo_dia'
					,'AMA_00_CadeiaNaoPermitidaNoMesmoDia'
					,'AMA_00_Contratos_Extracao'
					,'AMA_00_Dentes_Vizinhos_Com_Faces_Vizinhas'
					,'AMA_00_Dentistas_Credenciados_Para_Prevencao_E_Clinico'
					,'AMA_00_DENTISTAS_EXCECOES_MODELO'
					,'AMA_00_Dentistas_Liberacao_Fatores_Moderadores_RCI'
					,'AMA_00_DENTISTAS_LIMITES_PROCEDIMENTOS'
					,'AMA_00_Historico_Dos_Clientes_Extracao'
					,'AMA_00_IDADE_PERMITIDA_PROCEDIMENTOS'
					,'AMA_00_Indices_Composicao_Das_Variaveis_Da_Producao'
					,'AMA_00_Longevidade_Dos_Procedimentos'
					,'AMA_00_PROCEDIMENTOS_GLOBAIS_COM_PARCIAIS_IMPROPRIOS'
					,'AMA_00_ProcedimentosIsentosDeAutorizacao'
					,'AMA_00_Relacao_De_Clientes_Extracao'
					,'AMA_00_RELACIONAMENTO_PROCEDIMENTO_GRUPO'
					,'AMA_00_TABELA_AJUSTE_PROCEDIMENTOS'
					,'AMA_00_TIPOS_DE_GRUPOS_PROCEDIMENTOS'
					,'AMA_00_TuplasProcedimentoDenteFaceRegras'
					,'AUDITORIA_DENTISTA_PADRAO_WS_Arquivo_Historico_Classificacao_Mensal'
					,'ENTERPRISE_KNOWLEDGE_Log_Fatos_Procedimentos_PT'
					,'ENTERPRISE_KNOWLEDGE_Procedimentos_Abragencia_Atuacao'
					,'Graph_ArqProducao'
					,'Graph_Base_Conhecimento_Bayesiano'
					,'Graph_Classe_Probabilidades'
					,'Graph_Inferencia_Variaveis_Estados'
					,'Graph_Total_Observacoes'
					,'Graph_Variaveis_Explicativas'
					,'Graph_Variaveis_Probabilidades')*/
					NAME IN ('siItem_Remuneracao', 'siRemuneracao')
			
OPEN TamanhoTotal 
FETCH NEXT FROM TamanhoTotal
INTO @NameTable

WHILE @@FETCH_STATUS = 0
BEGIN 
	
	INSERT INTO @table(Name, rows, Reserved, data, Index_size, Unused)
	EXEC SP_SPACEUSED @NameTable

	FETCH NEXT FROM TamanhoTotal
	INTO @NameTable
END
CLOSE TamanhoTotal
DEALLOCATE TamanhoTotal

SELECT SUM(CAST(REPLACE(REPLACE(Reserved, 'k', ''), 'b', '') AS INT)) AS Data FROM (
SELECT Name, 
		rows, 
		SUBSTRING(Data, 0, CHARINDEX('KB', Reserved)) 'Reserved', 
		SUBSTRING(Data, 0, CHARINDEX('KB', Data)) 'Data', 
		index_size, 
		Unused 
FROM @table  
) AS T 








USE CorporeBD1_Teste

select definition AS [Codigo]
from sys.sql_modules
where object_name(object_id) LIKE '%FN_folha%'



SP_HELPTEXT FN_FOLHA_DE_PONTO_CHAPA_CHEFE



SELECT * --INTO RETORNO_ODONTO_SYSTEM_ENRIQ_PF
    FROM OPENROWSET('Microsoft.ACE.OLEDB.12.0',
      'Excel 12.0; Database=C:\Users\jose.valclemir\Documents\Atualizacao_18102018_1539794015_RETORNO_ODONTO.xls', [RETORNO_ODONTO_SYSTEM_ENRIQ_PF$]);



