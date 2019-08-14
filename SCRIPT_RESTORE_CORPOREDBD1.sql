--Restore_CorporeBD1_Teste

ALTER DATABASE CorporeBD1_Teste
SET SINGLE_USER 
WITH ROLLBACK IMMEDIATE;
GO

RESTORE DATABASE [CorporeBD1_Teste] FROM
DISK = N'\\10.85.0.6\sqlbkp\CorporeBd1_backup_2017-03-11 21-05-00.bak' WITH  FILE = 1, Replace,

MOVE N'Corpore_Data' TO N'G:\Data\CorporeBD1_Teste.mdf',
MOVE N'Corpore_Log' TO N'G:\Data\CorporeBD1_Teste.ldf',

STATS = 10

ALTER DATABASE CorporeBD1_Teste
SET MULTI_USER 
WITH ROLLBACK IMMEDIATE;
GO

------------------------------------------------------------------------------------
--Acerta usuario CorporeBD1_Teste

 SP_DROPUSER SYSDBA
 GO
 EXEC SP_CHANGEDBOWNER sa
 GO

/* 2� Parte */

/* Cria��o dos usu�rios RM e SYSDBA */

IF NOT EXISTS(SELECT * FROM MASTER.DBO.SYSLOGINS WHERE NAME = 'rm')
   CREATE LOGIN rm WITH PASSWORD = 'rmmasterkey',CHECK_POLICY=OFF
GO


EXEC SP_CHANGEDBOWNER rm
GO

IF NOT EXISTS(SELECT * FROM MASTER.DBO.SYSLOGINS WHERE NAME = 'sysdba')
   CREATE LOGIN sysdba WITH PASSWORD = 'masterkey',CHECK_POLICY=OFF 
GO
 
sp_adduser sysdba,sysdba
GO

GRANT SELECT ON GPARAMS TO sysdba
GO
GRANT SELECT , UPDATE ON GUSUARIO TO sysdba
GO
GRANT SELECT ON GPERMIS  TO sysdba
GO
GRANT SELECT ON GACESSO  TO sysdba
GO
GRANT SELECT ON GSISTEMA  TO sysdba
GO
GRANT SELECT ON GCOLIGADA  TO sysdba
GO
GRANT SELECT ON GUSRPERFIL TO sysdba
GO
GRANT SELECT ON GSERVICO TO sysdba
GO
GRANT SELECT ON GPARAMETROSSISTEMA TO sysdba
GO
GRANT SELECT,INSERT ON GDATALOG TO sysdba
GO
GRANT SELECT ON GMAILPARAMS TO sysdba
GO
GRANT SELECT ON GUPGATUALIZACAO TO sysdba
GO
GRANT SELECT,INSERT,DELETE,UPDATE ON GSESSAOFLUIG TO sysdba
GO
GRANT SELECT,INSERT,DELETE,UPDATE ON GULTIMOCONTEXTOUSUARIO TO sysdba
GO

SP_DEFAULTLANGUAGE 'RM','ENGLISH'
GO
SP_DEFAULTLANGUAGE 'SYSDBA','ENGLISH'
GO


SELECT * FROM GCOLIGADA g