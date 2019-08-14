ALTER DATABASE basicus_teste SET QUERY_STORE = ON
	(
		 -- READ_ONLY -> Esse modo indica que novas estatísticas de tempo de execução de consulta ou planos executados não serão rastreados (coletados)
		 -- READ_WRITE -> Permite a captura de planos executados por consulta e estatísticas de tempo de execução de consulta
		 OPERATION_MODE = READ_WRITE 
		,CLEANUP_POLICY = (STALE_QUERY_THRESHOLD_DAYS = 15) --Especifica o número de dias que os dados devem ser mantidos no repositório de consultas,
		,DATA_FLUSH_INTERVAL_SECONDS = 900 -- Determina a frequência na qual os dados gravados no repositório de consultas é persistida no disco.
		,MAX_STORAGE_SIZE_MB = 100 --Configura o tamanho máximo do repositório de consultas.
		,INTERVAL_LENGTH_MINUTES = 1 --Determina o intervalo de tempo de captura das estatisticas das querys
		,SIZE_BASED_CLEANUP_MODE = AUTO -- Controla se o processo de limpeza será ativado automaticamente quando o volume total dos dados se aproximar do tamanho máximo.
		,QUERY_CAPTURE_MODE = AUTO -- Indica se o repositório de consultas captura todas as consultas ou consultas relevantes com base no consumo de recursos e na contagem de execuções. ALL -> Captura todas as consultas, AUTO -> Ignora consultas com duração de compilação e execução insignificante.
		,MAX_PLANS_PER_QUERY = 200 -- Um número inteiro que representa a quantidade máxima de planos de manutenção para cada consulta. O valor padrão é 200
		--Controla se o Repositório de Consultas captura informações de estatísticas de espera. Pode ser OFF ou ON(Padrão)
	)


ALTER DATABASE basicus_teste SET QUERY_STORE = ON 
	(
		OPERATION_MODE = READ_WRITE,
		CLEANUP_POLICY = (STALE_THRESHOLD_DAYS = 15),
		DATA_FLUSH_INTERVAL_SECONDS = 900,
		MAX_STORAGE_SIZE_MB = 100,
		INTERVAL_LENGTH_MINUTES = 1,
		SIZE_BASED_CLEANUP_MODE = AUTO,
		MAX_PLANS_PER_QUERY = 200
	) 

SELECT * 
FROM sys.dm_tran_active_transactions

SELECT * FROM sys.dm_exec_requests


--Retorna informações sobre a fila de espera de tarefas que estão esperando algum recurso.
SELECT * FROM sys.dm_os_waiting_tasks
WHERE session_id = 1

/*Retorna informações sobre todas as esperas encontradas por threads executados. 
É possível usar essa exibição agregada para diagnosticar problemas de desempenho 
com o SQL Server e também com consultas e lotes específicos. 
DM exec_session_wait_stats (Transact-SQL) fornece informações semelhantes por sessão.*/
SELECT * FROM sys.dm_os_wait_stats;


DBCC TRACEON (1204,-1)
GO
DBCC TRACEON (1222,-1)
GO

/* Second Option Enabling Trace Flags 1204 and 1222 using DBCC TRACEON Statement at global level */

DBCC TRACEON (1204, 1222, -1)
GO

DBCC TRACESTATUS(-1)




BEGIN TRAN 
UPDATE Clientes 
	SET NomeCompleto = 'Valclemir' 
WHERE idCliente = 1

UPDATE contratos 
	SET DtContrato = GETDATE() 
WHERE idContrato = 1 

COMMIT











SELECT so.name
		, (avg_total_user_cost * avg_user_impact) * (User_seeks + user_scans) AS Impact 
		, mid.equality_columns
		, mid.inequality_columns
		, mid.included_columns
FROM sys.dm_db_missing_index_group_stats AS stats 
INNER JOIN sys.dm_db_missing_index_groups AS mig 
	ON (Stats.group_handle = mig.index_group_handle)
INNER JOIN sys.dm_db_missing_index_details AS mid
	ON (mid.index_handle = mig.index_handle)
INNER JOIN sys.objects so WITH(NOLOCK) 
	ON (mid.object_id = so.object_id)
WHERE stats.group_handle IN (
								SELECT TOP 5 group_handle 
								FROM sys.dm_db_missing_index_group_stats H
								ORDER BY (avg_total_user_cost * avg_user_impact) 
										* (user_seeks + user_scans))



SELECT 
	highest_cpu_querys.plan_handle,
	highest_cpu_querys.total_worker_time,
	q.dbid,
	q.objectid,
	q.number,
	q.encrypted,
	q.text,
	qp.query_plan
FROM (
		SELECT TOP 50 o.plan_handle,
					 o.total_worker_time 
		FROM sys.dm_exec_query_stats o
		ORDER BY o.total_worker_time DESC) AS Highest_cpu_querys
CROSS APPLY sys.dm_exec_sql_text(highest_cpu_querys.plan_handle) AS q
CROSS APPLY sys.dm_exec_query_plan (highest_cpu_querys.plan_handle) AS qp
ORDER BY Highest_cpu_querys.total_worker_time DESC 
	 






DECLARE @Numero INT = 1
WHILE @Numero < 1000000
	BEGIN 
		SET @Numero = @Numero + 1
		IF @Numero = 999999
			GOTO FIMPROGRAMA 
		ELSE 
			CONTINUE
	END

FIMPROGRAMA:
	PRINT 'FIMPROGRAMA'
GOTO BEGINPROGRAM
BEGINPROGRAM:
	PRINT 'BEGIN'

SELECT s.name as schemaname, object_name (t.object_id) as table_name, t.lock_escalation_desc
FROM sys.tables t, sys.schemas s
--WHERE object_name(t.object_id) = 'Products' 
WHERE  s.name = 'dbo' 
and s.schema_id = t.schema_id 

SELECT * FROM sys.objects WHERE type = 'U'


SELECT * FROM sys.dm_os_memory_objects 

SELECT sp.rows,
	   st.name
FROM sys.partitions sp 
INNER JOIN sys.tables st 
	ON(sp.object_id = st.object_id)



SELECT * FROM sys.dm_exec_session_wait_stats


SELECT wait_type
	  ,SUM(waiting_tasks_count) waiting_tasks_count
	  ,SUM(wait_time_ms) wait_time_ms
	  ,SUM(max_wait_time_ms) max_wait_time_ms
FROM (
				SELECT  
					   wait_type
					  , waiting_tasks_count
					  ,MAX(wait_time_ms) wait_time_ms
					  , MAX(max_wait_time_ms) max_wait_time_ms
					  --,signal_wait_time_ms
				FROM sys.dm_exec_session_wait_stats 
				WHERE session_id = 149
				GROUP BY 
						 wait_type
						 ,waiting_tasks_count
						 --max_wait_time_ms,
						 --signal_wait_time_ms
) AS T 
GROUP BY wait_type
ORDER BY waiting_tasks_count DESC 


SELECT * FROM basicus_teste.dbo.clientes 
WHERE idCliente IN (1, 2, 4, 5, 1000, 10, 8)


SELECT * FROM clientes 
SELECT * FROM (
	SELECT MAX(wait_duration_ms) wait_duration_ms, wait_type FROM sys.dm_os_waiting_tasks
	WHERE session_id = 5
	GROUP BY wait_type
) AS T 
ORDER BY wait_duration_ms DESC 


SELECT * 
FROM sys.dm_exec_session_wait_stats 
	WHERE session_id = 149

SELECT * FROM sys.dm_exec_query_stats
SELECT * FROM sys.dm_os_waiting_tasks


SELECT session_id,
		login_time,
		host_name,
		program_name,
		client_interface_name,
		login_name,
		nt_domain,
		nt_user_name,
		status,
		cpu_time,
		reads,
		writes,
		logical_reads,
		original_login_name
FROM sys.dm_exec_sessions


SELECT * FROM sys.dm_db_partition_stats

SELECT  --qs.sql_handle,
		q.dbid,
		qs.last_execution_time,
	  qs.last_elapsed_time,
	  qs.execution_count,
	  q.text
	  --der.database_id
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text (qs.sql_handle) q
WHERE execution_count > 1000

SELECT * FROM sys.dm_exec_query_stats qs

SELECT * FROM sys.dm_exec_session_wait_stats
SELECT * FROM sys.dm_exec_requests

SELECT * FROM sys.dm_os_waiting_tasks
SELECT * FROM sys.dm_os_wait_stats

SELECT t1.database_id, qs.dbid FROM sys.dm_exec_requests t1
CROSS APPLY sys.dm_exec_sql_text(0x02000000E3389528072F9CCE2E1746B5A8019858B978F9DA0000000000000000000000000000000000000000) qs
WHERE t1.database_id = qs.dbid

SELECT * FROM sys.dm_exec_query_stats


SELECT T2.* FROM (
SELECT T2.*, T1.creation_time, T1.last_execution_time FROM sys.dm_exec_query_stats T1 
CROSS APPLY sys.dm_exec_sql_text (T1.sql_handle) T2
) AS T2 
INNER JOIN sys.objects T3 
	ON (T2.objectid = T3.object_id)
WHERE T3.type IN ('U', 'P', 'FN')





SELECT T2.*, T1.creation_time, T1.Last_execution_time FROM sys.dm_exec_query_stats T1
CROSS APPLY sys.dm_exec_sql_text(T1.sql_handle)   T2
ORDER BY T1.Last_execution_time DESC


SELECT * FROM sys.dm_exec_requests 









SELECT sp.row_count,
		st.name
FROM sys.dm_db_partition_stats sp
INNER JOIN sys.tables st 
	ON(sp.object_id = st.object_id)


SELECT * FROM sys.dm_os_waiting_tasks
WHERE session_id = 138

SELECT * FROM sys.dm_os_wait_stats

SELECT * FROM sys.dm_exec_session_wait_stats
WHERE session_id = 93






SELECT SUM (pages_in_bytes) as 'Bytes Used', type
FROM sys.dm_os_memory_objects
GROUP BY type
ORDER BY 'Bytes Used' DESC;



SELECT SUM (pages_in_bytes) as 'Bytes Used'
FROM sys.dm_os_memory_objects



SELECT SUM(Pages_in_bytes)  FROM sys.dm_os_memory_objects





ALTER DATABASE Auditoria_teste SET QUERY_STORE = ON 

ALTER DATABASE Auditoria_teste SET QUERY_STORE 
	(
		SIZE_BASED_CLEANUP_MODE = AUTO,
		CLEANUP_POLICY = (INTERVAL_LENGTH_MINUTES = 15),
		STALE_QUERY_THRESHOLD_DAYS = 15,
		QUERY_CAPTURE_MODE = AUTO,
		MAX_STORAGE_SIZE_MB = 1024
	)





SELECT TOP 1 * FROM sys.dm_os_wait_stats /* Retorna informações sobre todas as esperas encontradas 
											por threads excutados, essa instrunção foi trocada pela sys.dm_exec_session_wait_stats */

SELECT TOP 1 * FROM sys.dm_exec_session_wait_stats -- Retorna todas as sessões e informações de watings das mesmas.

SELECT TOP 1 * FROM sys.dm_os_waiting_tasks -- Retorna informações sobre a fila de espera de tarefas que estão esperando algum recurso.

SELECT TOP 1 * FROM sys.dm_os_memory_objects /* Retorna objetos da memoria que estão alocados no momento, pelo sql server. Serve também 
												para identificar possíveis vazamentos ou pressão de memoria.*/				

SELECT TOP 1 * FROM sys.dm_exec_query_stats qs /* Retorna estatisticas de todas as querys, muito usado para se obter o plano e codigo da consulta
												em junção com a sys.dm_exec_sql_text, como no exemplo abaixo: 
													SELECT TOP 10 st.text FROM sys.dm_exec_query_stats qs 
													CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
												*/


SELECT 
   OBJECT_NAME(f.parent_object_id) AS 'NameTable',
   COL_NAME(fc.parent_object_id,fc.parent_column_id) AS 'Field name',
   delete_referential_action_desc AS 'On Delete'
FROM sys.foreign_keys AS f,
     sys.foreign_key_columns AS fc,
     sys.tables t 
WHERE f.OBJECT_ID = fc.constraint_object_id
AND t.OBJECT_ID = fc.referenced_object_id
AND delete_referential_action_desc = 'CASCADE'
ORDER BY 1




--ON DELETE CASCADE ON UPDATE CASCADE
/* 
ON DELETE CASCADE --> Com está opcao ativa, podemos deletar dados na tabela pai e automaticamente deleta os dados referenciados 
na tabela filho.

ON UPDATE CASCADE --> Com está opcao ativa, podemos atualizar dados na tabela pai e automaticamente irá atualizar na tabela filho.

NO ACTION --> Quando nenhuma das opcoes são setada, automaticamente ela é setada, usado com o ON DELETE NO ACTION, quando tentar deletar um dado da tabela pai
automaticamente irá lançar um erro de violação de chave estrangeira, que é referência na tabela filho.

SET NULL --> Se você especificar ON DELETE SET NULL ao criar a chave estrangeira e se você excluir a coluna de 
chave primária da tabela pai, a coluna de chave estrangeira relacionada da tabela filha será automaticamente 
definida como valor NULL. EX (ON DELETE SET NULL ON UPDATE SET NULL)

SET DEFAULT --> Para utilizar a opção SET DEFAULT para Excluir e Atualizar, você precisa criar uma restrição 
padrão na chave estrangeira, caso contrário, esta opção não será executada. 
Em nosso caso, não criamos a restrição Padrão na chave estrangeira DepartmentId, portanto, precisamos criá-la primeiro.

NOCHECK --> Desabilita a chave estrangeira, CHECK habilita, ex: ALTER TABLE Name_table NOCHECK/CHECK CONSTRAINT NAME_Constraint
*/
DROP TABLE IF EXISTS T2 
GO
DROP TABLE IF EXISTS T1 
GO
CREATE TABLE T1 (
	id INT PRIMARY KEY,
	Nome VARCHAR(10)
)
GO
CREATE TABLE T2 (
	Id INT,
	idT1 INT,
	CONSTRAINT FK_idT1 FOREIGN KEY (idT1) REFERENCES T1(id) ON DELETE NO ACTION ON UPDATE NO ACTION  
)
GO
INSERT INTO T1(id, Nome) 
	VALUES (1, 'V'),
			(2, 'A'),
			(3, 'F')

GO
INSERT INTO T2(id, idT1)
	VALUES (1, 1),
			(2, 2),
			(3, 3)
GO
SELECT * FROM T1
SELECT * FROM T2 
GO

DELETE FROM T1 WHERE id = 2


USE Basicus_teste
GO
SELECT qt.query_sql_text,
		q.last_execution_time,
		CAST(sp.query_plan AS XML) 
FROM sys.query_store_query_text as qt
INNER JOIN sys.query_store_query as q 
	ON (qt.query_text_id = q.query_text_id)
INNER JOIN sys.query_store_plan sp
	ON (q.query_id = sp.query_id)


SELECT (sp.query_plan),
		q.last_execution_time
FROM sys.query_store_plan sp
INNER JOIN sys.query_store_query q
	ON (q.query_id = sp.query_id)

SELECT sp.query_plan
	   --q.last_execution_time
FROM sys.query_store_plan sp
INNER JOIN sys.query_store_query q
	ON (sp.query_id = q.query_id)

SELECT TOP 1 T1.last_execution_time, CAST(T1.query_plan AS NVARCHAR(MAX)), T2.* FROM sys.query_store_plan T1
INNER JOIN sys.query_store_query T2
	ON (T1.query_id = T2.Query_id)

SELECT TOP 1 * FROM sys.query_store_plan



SELECT event_data.value('(event[@name="xml_deadlock_report"]/@timestamp)[1]','datetime') FROM (
SELECT  CAST(event_data AS XML) event_data
FROM sys.fn_xe_file_target_read_file(N'C:\log_events_sql_deadLocks_0_131897892491060000.xel', NULL, NULL, NULL)
) AS T 

SELECT event_data.query('event/data/value/deadlock[1]') FROM (
	SELECT  CAST(event_data AS XML) event_data
	FROM sys.fn_xe_file_target_read_file(N'C:\log_events_sql_deadLocks_0_131897892491060000.xel', NULL, NULL, NULL)
) AS T 

SELECT event_data.value('(event/data/value/deadlock/victim-list/victimProcess/@id)[1]', 'VARCHAR(MAX)') FROM (
	SELECT  CAST(event_data AS XML) event_data
	FROM sys.fn_xe_file_target_read_file(N'C:\log_events_sql_deadLocks_0_131897892491060000.xel', NULL, NULL, NULL)
) AS T 


SELECT event_data.value('(event/data/value/deadlock/process-list/process/@isolationlevel)[1]', 'VARCHAR(MAX)') FROM (
	SELECT  CAST(event_data AS XML) event_data
	FROM sys.fn_xe_file_target_read_file(N'C:\log_events_sql_deadLocks_0_131897892491060000.xel', NULL, NULL, NULL)
) AS T 


SET STATISTICS IO ON
SELECT * FROM (
				SELECT  
						st.text,
						t2.waittime,
						t2.transactionname,
						t2.LockMode,
						t2.Status,
						t2.clientapp,
						t2.hostname,
						t2.loginname,
						t2.isolation_level,
						CAST(t2.lastbatchcompleted AS datetime2(3)) AS LastbatchCompleted
				FROM (
					SELECT CONVERT(VARBINARY(100), UPPER(event_data.value('(event/data/value/deadlock/process-list/process/executionStack/frame/@sqlhandle)[1]', 'VARCHAR(MAX)')), 1) Sql_handle,
						   UPPER(event_data.value('(event/data/value/deadlock/process-list/process/@waittime)[1]', 'VARCHAR(MAX)')) waittime,
						   UPPER(event_data.value('(event/data/value/deadlock/process-list/process/@transactionname)[1]', 'VARCHAR(MAX)')) transactionname,
						   UPPER(event_data.value('(event/data/value/deadlock/process-list/process/@lockMode)[1]', 'VARCHAR(MAX)')) LockMode,
						   UPPER(event_data.value('(event/data/value/deadlock/process-list/process/@status)[1]', 'VARCHAR(MAX)')) Status,
						   UPPER(event_data.value('(event/data/value/deadlock/process-list/process/@clientapp)[1]', 'VARCHAR(MAX)')) clientapp,
						   UPPER(event_data.value('(event/data/value/deadlock/process-list/process/@hostname)[1]', 'VARCHAR(MAX)')) hostname,
						   UPPER(event_data.value('(event/data/value/deadlock/process-list/process/@loginname)[1]', 'VARCHAR(MAX)')) loginname,
						   UPPER(event_data.value('(event/data/value/deadlock/process-list/process/@isolationlevel)[1]', 'VARCHAR(MAX)')) isolation_level,
						   UPPER(event_data.value('(event/data/value/deadlock/process-list/process/@lastbatchcompleted)[1]', 'VARCHAR(MAX)')) lastbatchcompleted
					FROM (
							SELECT  CAST(event_data AS XML) event_data
							FROM sys.fn_xe_file_target_read_file(N'C:\log_events_sql_deadLocks_0_131897892491060000.xel', NULL, NULL, NULL)
					) AS T1 
				) AS T2
				CROSS APPLY sys.dm_exec_sql_text(sql_handle) AS st
) AS T3
ORDER BY LastbatchCompleted DESC 





SELECT  
		st.text 'QUERY DEADLOCK',
		t2.OwnerLock,
		T2.RequestTypeLock
FROM (
	SELECT  CONVERT(VARBINARY(100), UPPER(event_data.value('(event/data/value/deadlock/process-list/process/executionStack/frame/@sqlhandle)[1]', 'VARCHAR(MAX)')), 1) Sql_handle,
			UPPER(event_data.value('(event/data/value/deadlock/resource-list/keylock/owner-list/owner/@mode)[1]', 'VARCHAR(MAX)'))	OwnerLock,
			UPPER(event_data.value('(event/data/value/deadlock/resource-list/keylock/waiter-list/waiter/@mode)[1]', 'VARCHAR(MAX)')) RequestTypeLock
	FROM (
			SELECT  CAST(event_data AS XML) event_data
			FROM sys.fn_xe_file_target_read_file(N'C:\log_events_sql_deadLocks_0_131897892491060000.xel', NULL, NULL, NULL)
	) AS T1 
) AS T2
CROSS APPLY sys.dm_exec_sql_text(sql_handle) AS st





BEGIN TRAN
UPDATE Clientes 
	SET NomeCompleto = 'Valclemir' 
WHERE idCliente = 1


UPDATE Clientes 
	SET NomeCompleto = 'Valclemir' 
WHERE idCliente = 1

UPDATE contratos 
	SET dtContrato = GETDATE()
WHERE idCOntrato = 1 


COMMIT








KILL 59





SELECT TOP 1 * FROM sys.dm_exec_session_wait_stats
SELECT TOP 1 * FROM sys.dm_os_wait_stats
SELECT TOP 1 * FROM sys.dm_os_waiting_tasks



SELECT TOP 1 st.text FROM sys.dm_exec_query_stats
CROSS APPLY sys.dm_exec_sql_text(sql_handle) st



SELECT 
		iss.user_seeks,
		iss.user_scans,
		i.name,
		i.type_desc,
		OBJECT_NAME(iss.object_id)
FROM sys.dm_db_index_usage_stats iss
INNER JOIN sys.indexes i 
	ON(iss.object_id = i.object_id)
WHERE (iss.user_scans <> 0 AND iss.user_scans > 3) 
AND (iss.user_seeks <> 0 AND iss.user_seeks > 2)


SELECT * FROM sys.dm_exec_query_stats
SELECT * FROM sys.dm_os_wait_stats
SELECT * FROM sys.dm_exec_session_wait_stats
ORDER BY waiting_tasks_count DESC



SELECT * FROM sys.dm_db_missing_index_details
SELECT * FROM sys.dm_db_missing_index_group_stats



SELECT T.*, T3.* FROM sys.dm_db_missing_index_details T3
CROSS APPLY sys.dm_db_missing_index_columns (index_handle) AS T


SELECT type, SUM(pages_in_bytes) as 'Bytes_used' FROM sys.dm_os_memory_objects 
GROUP BY type 
ORDER BY 2 DESC


SELECT * FROM msdb.dbo.sysjobs
WHERE name LIKE '%job%'


SELECT * FROM sys.dm_os_memory_clerks


SELECT   DISTINCT CONVERT(DECIMAL(18, 2) , user_seeks * avg_total_user_cost * ( avg_user_impact * 0.01 )) AS [index_advantage] ,
         migs.last_user_seek ,
         mid.[statement] AS [Database.Schema.Table] ,
         mid.equality_columns ,
         mid.inequality_columns ,
         mid.included_columns ,
         migs.unique_compiles ,
         migs.user_seeks ,
         migs.avg_total_user_cost ,
         migs.avg_user_impact ,
         OBJECT_NAME(mid.[object_id]) AS [Table Name] ,
         p.rows AS [Table Rows]
FROM     sys.dm_db_missing_index_group_stats AS migs WITH ( NOLOCK )
         INNER JOIN sys.dm_db_missing_index_groups AS mig WITH ( NOLOCK ) ON migs.group_handle = mig.index_group_handle
         INNER JOIN sys.dm_db_missing_index_details AS mid WITH ( NOLOCK ) ON mig.index_handle = mid.index_handle
         INNER JOIN sys.partitions AS p WITH ( NOLOCK ) ON p.[object_id] = mid.[object_id]
WHERE    mid.database_id = DB_ID()
ORDER BY index_advantage DESC
OPTION ( RECOMPILE );




SELECT group_handle,
		user_seeks,
		last_system_seek,
		user_scans,
		last_user_scan,
		avg_total_user_cost,
		avg_total_system_cost,
		avg_user_impact
FROM sys.dm_db_missing_index_group_stats

SELECT T1.index_group_handle, 
		T2.group_handle,
		CONVERT(NUMERIC(10, 2), T2.avg_total_user_cost) avg_total_user_cost,
		T2.avg_user_impact,
		T2.avg_system_impact,
		T2.avg_total_system_cost,
		T3.equality_columns,
		T3.included_columns,
		T3.statement,
		T4.column_name
FROM sys.dm_db_missing_index_groups T1
INNER JOIN sys.dm_db_missing_index_group_stats T2
	ON (T1.index_group_handle = T2.group_handle)
INNER JOIN sys.dm_db_missing_index_details T3
	ON (T1.index_handle = T3.index_handle)
CROSS APPLY sys.dm_db_missing_index_columns (T1.index_handle) T4


SELECT  *  FROM sys.dm_db_missing_index_group_stats

SELECT T1.object_id,
		T1.statement,
		T2.name 
FROM sys.dm_db_missing_index_details T1
INNER JOIN sys.indexes T2 
	ON (T1.object_id = T2.object_id)





SELECT migs.group_handle, mid.*  
FROM sys.dm_db_missing_index_group_stats AS migs  
INNER JOIN sys.dm_db_missing_index_groups AS mig  
	ON (migs.group_handle = mig.index_group_handle)  
INNER JOIN sys.dm_db_missing_index_details AS mid  
	ON (mig.index_handle = mid.index_handle)  
WHERE migs.group_handle = 24;  






SELECT TOP 10 * FROM dbo.clientes 

SELECT NomeCompleto, NomeAbreviado,
		sexo, DataNascimento
FROM clientes
WHERE nomecompleto = 'VALCLEMIR'

CREATE NONCLUSTERED INDEX IDX_NomeCompleto_clientes ON Clientes (NomeCOmpleto) 
INCLUDE (NomeAbreviado, Sexo, DataNascimento)


CREATE NONCLUSTERED INDEX IDX_NomeCompleto_clientes ON Clientes (NomeCOmpleto) 
INCLUDE (Sexo, DataNascimento, NomeAbreviado)
WITH (DROP_EXISTING = ON)


DROP INDEX IDX_NomeCompleto_clientes ON clientes 














SELECT  T3.name,
		T1.last_user_seek, T1.last_user_scan, T1.last_user_lookup,
		T1.user_seeks, T1.user_scans, T1.user_lookups,
		T2.name, T2.type_desc FROM sys.dm_db_index_usage_stats T1
INNER JOIN sys.indexes  T2 
	ON (T1.object_id = T2.object_id)
	AND (T1.index_id = T2.index_id)
INNER JOIN sys.databases T3
	ON (T1.database_id = T3.database_id)
WHERE YEAR(last_user_seek) > 2017
	 AND T1.user_seeks <> 0 


SET STATISTICS IO ON
DROP TABLE IF EXISTS #Temp 
GO
CREATE TABLE #Temp 
	(	
		object_id BIGINT,
		Avg_fragmentation_in_percent NUMERIC(10, 2),
		NameTable VARCHAR(100),
		Type_desc VARCHAR(100),
		NameIndex VARCHAR(100)
	)
GO
DECLARE @Contador INT = 1 
WHILE @Contador < 100
	
	INSERT INTO #Temp 
	SELECT  
			T1.object_id,
			CONVERT(NUMERIC(10,2), T1.avg_fragmentation_in_percent) avg_fragmentation_in_percent,
			T2.name, 
			T2.type_desc,
			T3.name/*, 
			T4.avg_fragment_size_in_pages, 
			T4.avg_page_space_used_in_percent, 
			T4.avg_fragmentation_in_percent */
	FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) T1
	INNER JOIN sys.tables T2 WITH (NOLOCK) 
		ON (T1.object_id = T1.object_id)
	INNER JOIN sys.indexes T3
		ON (T1.object_id = T3.object_id) 
	WHERE T1.object_id > (
							SELECT ISNULL(MAX(object_id), 0) FROM #Temp)
	SET @Contador = @Contador + 1
	


	

;WITH Tamanho_Tabelas AS (
SELECT obj.name, prt.rows
FROM sys.objects obj
JOIN sys.indexes idx on obj.object_id= idx.object_id
JOIN sys.partitions prt on obj.object_id= prt.object_id
JOIN sys.allocation_units alloc on alloc.container_id= prt.partition_id
WHERE obj.type= 'U' AND idx.index_id IN (0, 1)and prt.rows> 1000
GROUP BY obj.name, prt.rows)

SELECT * FROM sys.indexes 
WHERE index_id = 0

SELECT * FROM sys.objects 
WHERE type = 'U'
 
--insert into #Atualiza_Estatisticas(Ds_Comando,Nr_Linha)
SELECT /*'UPDATE STATISTICS ' + B.name+ ' ' + */B.name 'TableName'/*, A.name,*//*+ ' WITH FULLSCAN', D.rows*/
		,A.name   AS StatisticsName,   
		STATS_DATE(object_id,   stats_id) AS StatisticsUpdatedDate
FROM sys.stats A
join sys.sysobjects B on A.object_id = B.id
join sys.sysindexes C on C.id = B.id and A.name= C.Name
JOIN Tamanho_Tabelas D on  B.name= D.Name
WHERE  C.rowmodctr > 100
and C.rowmodctr> D.rows*.005
and substring( B.name,1,3) not in ('sys','dtp')
ORDER BY StatisticsUpdatedDate DESC


SELECT * FROM sys.dm_tran_active_transactions

UPDATE STATISTICS Faturamento_Agendamento_Contrato WITH SAMPLE 20000 ROWS


SELECT * FROM Faturamento_Agendamento_Contrato





DBCC SHOW_STATISTICS ('dbo.Faturamento_Agendamento', '_WA_Sys_0000000B_557E92D1')
WITH HISTOGRAM


SELECT * FROM clientes_contratos
SELECT * FROM clientes
SELECT * FROM contratos


UPDATE STATISTICS NomeTabela(nomeStatisticas) WITH SAMPLE 20 PERCENT 





SELECT * FROM sys.partitions 
ORDER BY rows DESC 


SELECT DISTINCT T2.name, T1.rows FROM sys.partitions	T1
INNER JOIN sys.tables T2
	ON (T1.object_id = T2.object_id)
WHERE T2.Name = 'clientes_contratos_valores'


SELECT * FROM sys.indexes 











SELECT sc.name, so.name FROM sys.columns sc
INNER JOIN sys.objects so
	ON (sc.object_id = so.object_id)
WHERE so.type = 'U'



;WITH Tamanho_Tabelas AS (
SELECT obj.name, prt.rows
FROM sys.objects obj
JOIN sys.indexes idx on obj.object_id= idx.object_id
JOIN sys.partitions prt on obj.object_id= prt.object_id
JOIN sys.allocation_units alloc on alloc.container_id= prt.partition_id
WHERE obj.type= 'U' AND idx.index_id IN (0, 1)and prt.rows> 1000
GROUP BY obj.name, prt.rows)


--insert into #Atualiza_Estatisticas(Ds_Comando,Nr_Linha)
SELECT /*'UPDATE STATISTICS ' + B.name+ ' ' + */B.name 'TableName'/*, A.name,*//*+ ' WITH FULLSCAN', D.rows*/
		,A.name   AS StatisticsName,   
		STATS_DATE(object_id,   stats_id) AS StatisticsUpdatedDate
FROM sys.stats A
join sys.sysobjects B on A.object_id = B.id
join sys.sysindexes C on C.id = B.id and A.name= C.Name
JOIN Tamanho_Tabelas D on  B.name= D.Name
WHERE  C.rowmodctr > 100
and C.rowmodctr> D.rows*.005
and substring( B.name,1,3) not in ('sys','dtp')
ORDER BY StatisticsUpdatedDate DESC




SELECT name FROM msdb.dbo.sysjobs
WHERE name LIKE '%stati%'


SELECT DISTINCT T2.name, T1.rows FROM sys.partitions	T1
INNER JOIN sys.tables T2
	ON (T1.object_id = T2.object_id)
--WHERE T2.Name = 'clientes_contratos_valores'


;WITH Tamanho_Tabelas AS (
SELECT obj.name, prt.rows
FROM sys.objects obj
JOIN sys.indexes idx on obj.object_id= idx.object_id
JOIN sys.partitions prt on obj.object_id= prt.object_id
JOIN sys.allocation_units alloc on alloc.container_id= prt.partition_id
WHERE obj.type= 'U' AND idx.index_id IN (0, 1)and prt.rows> 1000
GROUP BY obj.name, prt.rows)


SELECT DISTINCT * FROM (
				select 
					db_name(database_id) as banco, 
					object_name(T1.object_id) as objeto, 
					T2.name AS NameStats,
					STATS_DATE(T1.object_id,   stats_id) AS StatisticsUpdatedDate,
					T1.*
					
				from sys.dm_db_index_usage_stats T1
				INNER JOIN sys.stats T2
					ON (T1.object_id = T2.object_id)
				where T1.object_id in (select object_id from sys.objects where type = 'U') 
AND User_seeks <> 0
) T1 
WHERE T1.objeto IN (SELECT name FROM Tamanho_tabelas)
AND objeto = 'Faturamento_Agendamento_Contrato'
-- ORDER BY user_seeks DESC 
ORDER BY objeto  





SELECT T1.name, T2.name FROM sys.stats  T1
INNER JOIN sys.tables T2
	ON (T1.object_id = T2.object_id)


SELECT * FROM sys.databases 
WHERE name = 'Basicus_teste'





IF OBJECT_ID('Teste') > 0
	DROP TABLE Teste 
GO 

CREATE TABLE Teste (
	ID INT 
)
GO

INSERT INTO Teste 
	VALUES (1),
			(2),
			(3)
GO

IF OBJECT_ID ('UtgUpdate') > 0
	DROP TRIGGER UtgUpdate
GO

CREATE TRIGGER UtgUpdate 
ON Teste 
FOR INSERT
AS 
	BEGIN 
		IF (SELECT COUNT (*) FROM Teste) > 5
			BEGIN 
				DECLARE @Valor INT
				SELECT @valor = id FROM inserted
				PRINT ('Valor: '+CAST(@valor AS CHAR(1)))
				RAISERROR('Error de inserção', 15, 1)
				ROLLBACK
			END
		
		ELSE 
			PRINT('INSERIU')
	END
GO


INSERT INTO Teste 
	VALUES (1),
			(2),
			(3)



SELECT * FROM teste

DELETE FROM teste 



SELECT * FROM sys.server_triggers

SELECT * FROM sys.dm_exec_sessions
WHERE is_user_process = 1
	AND LOGIN_TIME >= DATEADD(MINUTE, -53, DATEADD(HOUR, -4, GETDATE()))
	AND original_login_name = 'DEVTeste'



IF OBJECT_ID('UtgNotDropTable') > 0
	DROP TRIGGER UtgNotDropTable
GO

CREATE TRIGGER UtgNotDropTable
ON DATABASE 
FOR ALTER_TABLE, DROP_TABLE 
AS 
	BEGIN 
		PRINT ('Não pode alterar ou dropar tabela')
		ROLLBACK
	END 



SELECT * FROM sys.triggers
WHERE name = 'UtgNotDropTable'


DROP TRIGGER UtgNotDropTable ON DATABASE 






-- VIEW PARTICIONADA
IF OBJECT_ID('VW_FactPartition') > 0
	DROP VIEW  VW_FactPartition
GO
DROP TABLE IF EXISTS FactClientesALL
GO
CREATE TABLE FactClientesALL (
	idCliente INT, 
	NomeCOmpleto VARCHAR(100), 
	NomeAbreviado VARCHAR(100),
	sexo TINYINT, 
	DataNascimento DATETIME2(3),
	DtAlteracao DATETIME2(3),
	CONSTRAINT PK_idClienteFactALL PRIMARY KEY (idCLiente),
	CONSTRAINT CK_DtAlteracaoFactClientesALL  CHECK(YEAR(dtAlteracao) IN (2016, 2017, 2018))
)
GO

DROP TABLE IF EXISTS FactClientes2016
GO
CREATE TABLE FactClientes2016 (
	idCliente INT, 
	NomeCOmpleto VARCHAR(100), 
	NomeAbreviado VARCHAR(100),
	sexo TINYINT, 
	DataNascimento DATETIME2(3),
	DtAlteracao DATETIME2(3),
	CONSTRAINT PK_idClienteFact2016 PRIMARY KEY (idCLiente),
	CONSTRAINT CK_DtAlteracaoFactClientes2016 CHECK(YEAR(dtAlteracao) IN (2016))
)
GO

DROP TABLE IF EXISTS FactClientes2017
GO
CREATE TABLE FactClientes2017 (
	idCliente INT, 
	NomeCOmpleto VARCHAR(100), 
	NomeAbreviado VARCHAR(100),
	sexo TINYINT, 
	DataNascimento DATETIME2(3),
	DtAlteracao DATETIME2(3),
	CONSTRAINT PK_idClienteFact2017 PRIMARY KEY (idCLiente),
	CONSTRAINT CK_DtAlteracaoFactClientes2017  CHECK(YEAR(dtAlteracao) IN (2017))
)
GO

DROP TABLE IF EXISTS FactClientes2018
GO
CREATE TABLE FactClientes2018 (
	idCliente INT, 
	NomeCOmpleto VARCHAR(100), 
	NomeAbreviado VARCHAR(100),
	sexo TINYINT, 
	DataNascimento DATETIME2(3),
	DtAlteracao DATETIME2(3),
	CONSTRAINT PK_idClienteFact2018 PRIMARY KEY (idCLiente),
	CONSTRAINT CK_DtAlteracaoFactClientes2018  CHECK(YEAR(dtAlteracao) IN (2018))
)
GO

-- INSERT IN TABLES

INSERT INTO FactClientesALL 
	SELECT idCliente, NomeCOmpleto, NomeAbreviado,  sexo, DataNascimento, DtAlteracao FROM clientes
	WHERE YEAR(DtAlteracao) = 2016
		UNION ALL
	SELECT idCliente, NomeCOmpleto, NomeAbreviado,  sexo, DataNascimento, DtAlteracao FROM clientes
	WHERE YEAR(DtAlteracao) = 2017
		UNION ALL 
	SELECT idCliente, NomeCOmpleto, NomeAbreviado,  sexo, DataNascimento, DtAlteracao FROM clientes
	WHERE YEAR(DtAlteracao) = 2018


INSERT INTO FactClientes2016 
	SELECT idCliente, NomeCOmpleto, NomeAbreviado,  sexo, DataNascimento, DtAlteracao FROM clientes
	WHERE YEAR(DtAlteracao) = 2016

INSERT INTO FactClientes2017
	SELECT idCliente, NomeCOmpleto, NomeAbreviado,  sexo, DataNascimento, DtAlteracao FROM clientes
	WHERE YEAR(DtAlteracao) = 2017

INSERT INTO FactClientes2018 
	SELECT idCliente, NomeCOmpleto, NomeAbreviado,  sexo, DataNascimento, DtAlteracao FROM clientes
	WHERE YEAR(DtAlteracao) = 2018

GO

-- CREATE VIEW PARTITIONED 

CREATE VIEW VW_FactPartition 
WITH SCHEMABINDING 
AS 
	SELECT idCliente, NomeCOmpleto, NomeAbreviado,  sexo, DataNascimento, DtAlteracao FROM dbo.FactClientes2016
		UNION ALL
	SELECT idCliente, NomeCOmpleto, NomeAbreviado,  sexo, DataNascimento, DtAlteracao FROM dbo.FactClientes2017
		UNION ALL 
	SELECT idCliente, NomeCOmpleto, NomeAbreviado,  sexo, DataNascimento, DtAlteracao FROM dbo.FactClientes2018

GO	
SET STATISTICS IO ON


SELECT  idCliente FROM VW_FactPartition
WHERE idCliente IN (
					45, 46, 51, 57 )
AND YEAR(DtAlteracao) >= 2017

SELECT idCliente FROM FactClientesALL	
WHERE YEAR(DtAlteracao) = 2017



SELECT  idCliente FROM VW_FactPartition
WHERE YEAR(DtAlteracao) = 2017

SELECT idCliente FROM FactClientesALL	
WHERE idCliente = (
					SELECT 51)  

AND YEAR(DtAlteracao) >= 2017




DROP PROC IF EXISTS #UspChecaIdClienteExists
GO
CREATE PROCEDURE #UspChecaIdClienteExists (
	@idCliente AS INT,
	@id AS INT = NULL OUTPUT
)
AS 
	BEGIN 
		BEGIN TRY 
			
			IF @idCliente <> 0
				SELECT @id = idCliente FROM clientes 
				WHERE idCliente = @idCliente 

				RETURN @id 
		END TRY 
		BEGIN CATCH 
			RAISERROR('Por favor, informe um valor que seja diferente de 0', 16, 1)
		END CATCH 
	END 
GO
DECLARE @id1 INT 
EXEC #UspChecaIdClienteExists @idCliente = 1, @id = @id1 OUTPUT 
IF @id1 <> 0 OR @id1 IS NOT NULL 
	BEGIN 
		PRINT('Numero informado existe')
	END
ELSE 
	PRINT ('Não existe ou é nulo')


DECLARE @Value INT 
EXEC @Value = #UspChecaIdClienteExists @idCliente = 1
PRINT (@Value)



--SCALAR FUNCTION 

CREATE FUNCTION FN_RetornaIdCliente 
	(
		@idCliente AS INT
	)
RETURNS INT 
AS 
	BEGIN 
		DECLARE @id INT 
		SELECT @id = idCLiente FROM Clientes WHERE idCliente = @idCliente 
		RETURN @id 
	END

SELECT dbo.FN_RetornaIdCliente(1)


-- TABLE VALUE FUNCTION 
CREATE FUNCTION FN_tableValueFunction
(
	@idCliente AS INT
)
RETURNS TABLE
	RETURN (
				SELECT * FROM clientes WHERE idCliente = @idCliente 
			)


SELECT * FROM dbo.FN_tableValueFunction(1)

CREATE FUNCTION FN_tableValueFunctionMultiStartament
(
	@idCliente AS INT 
)
RETURNS @Table TABLE (
	idCliente INT,
	NomeCompleto VARCHAR(1000)
)
AS 
	BEGIN 
		INSERT INTO @Table 
			SELECT TOP 1 idCliente, NomeCompleto+'  '+NomeAbreviado FROM clientes
		RETURN 
	END 

SELECT * FROM dbo.FN_tableValueFunctionMultiStartament(1)

DROP FUNCTION FN_retornaIdCliente 
DROP FUNCTION FN_tableValueFunction 
DROP FUNCTION FN_tableValueFunctionMultiStartament 




SELECT  
		(physical_memory_in_use_kb/1024) AS Memory_usedby_Sqlserver_MB,  
		(locked_page_allocations_kb/1024) AS Locked_pages_used_Sqlserver_MB,  
		(total_virtual_address_space_kb/1024) AS Total_VAS_in_MB,  
		process_physical_memory_low,  
		process_virtual_memory_low  
FROM sys.dm_os_process_memory;  







SET TRANSACTION ISOLATION LEVEL REPEATABLE READ 
BEGIN TRAN
SELECT TOP 1 * FROM clientes 
COMMIT



SELECT resource_type, resource_database_id,
		request_mode, request_type,
		request_status,
		request_session_id FROM sys.dm_tran_locks
WHERE resource_type <> 'Database'
AND request_session_id = 71

SP_WHOISACTIVE








SELECT * FROM sys.dm_exec_query_stats 
SELECT * FROM sys.dm_exec_sql_text
SELECT * FROM sys.dm_exec_requests
SELECT * FROM sys.dm_exec_session_wait_stats
SELECT T1.*, T2.text FROM sys.dm_exec_connections T1
CROSS APPLY sys.dm_exec_sql_text(T1.most_recent_sql_handle) T2

SELECT SUM(Pages_in_bytes) TotalBytes, type FROM sys.dm_os_memory_objects
GROUP BY type 
ORDER BY TotalBytes DESC

SELECT T1.text, T2.wait_type FROM (
	SELECT T1.*, T2.text FROM sys.dm_exec_connections T1
	CROSS APPLY sys.dm_exec_sql_text(T1.most_recent_sql_handle) T2
) AS T1 
INNER JOIN sys.dm_exec_session_wait_stats T2
	ON (T1.session_id = T2.session_id)
ORDER BY T2.waiting_tasks_count DESC

SELECT * FROM sys.dm_exec_session_wait_stats


SELECT * FROM sys.dm_exec_query_stats