--applies to Azure Synapse

 -- Get a list of tables and views in the current database
 select table_catalog [database], table_schema [schema], table_name [name], table_type [type]
 from INFORMATION_SCHEMA.TABLES
 go

--count number of rows in each table
select schema_name(tab.schema_id) + '.' + tab.name as [table],
       sum(part.rows) as [rows]
   from sys.tables as tab
        inner join sys.partitions as part
            on tab.object_id = part.object_id
where part.index_id IN (1, 0) -- 0 - table without PK, 1 table with PK
group by schema_name(tab.schema_id) + '.' + tab.name
order by sum(part.rows) desc
go

--list all user defined stored procedures and functions
select routine_schema as schema_name,
       routine_name,
       routine_type as type,
       data_type as return_type,
       routine_definition as definition
from information_schema.routines
where routine_schema not in ('sys', 'information_schema',
                             'mysql', 'performance_schema')
order by routine_schema,
         routine_name;
go

--list all user defined stored procedures' execution stats
select db_name(proc_stats.database_id) as dbname,
    sc.name as [schema],
    obj.name,
    proc_stats.last_execution_time,
    obj.modify_date,
    obj.create_date,
    proc_stats.execution_count,
    proc_stats.last_elapsed_time,
    proc_stats.min_elapsed_time,
    proc_stats.max_elapsed_time
from sys.dm_pdw_nodes_exec_procedure_stats as proc_stats
inner join sys.objects as obj
    on obj.object_id = proc_stats.object_id
inner join sys.schemas as sc
    on obj.schema_id = sc.schema_id
where obj.type = 'P'
order by dbname,
    sc.name,
    obj.name
go


--list all stored procedures dependencies
select distinct(type), type_desc from sys.all_objects
--FN,SQL_SCALAR_FUNCTION
--P,SQL_STORED_PROCEDURE
--U,USER_TABLE
--V,VIEW
--X,EXTENDED_STORED_PROCEDURE

select 
    name,
    object_id,
    schema_id,
    type,
    type_desc,
    create_date,
    modify_date
from sys.objects


select * from sys.sql_expression_dependencies




SELECT OBJECT_NAME(referencing_id) AS referencing_entity_name,   
    o.type_desc AS referencing_desciption,   
    COALESCE(COL_NAME(referencing_id, referencing_minor_id), '(n/a)') AS referencing_minor_id,   
    referencing_class_desc, referenced_class_desc,  
    referenced_server_name, referenced_database_name, referenced_schema_name,  
    referenced_entity_name,   
    COALESCE(COL_NAME(referenced_id, referenced_minor_id), '(n/a)') AS referenced_column_name,  
    is_caller_dependent, is_ambiguous  
FROM sys.sql_expression_dependencies AS sed  
INNER JOIN sys.objects AS o ON sed.referencing_id = o.object_id  
WHERE referencing_id = OBJECT_ID(N'dbo.usp_query_cust_table');  
GO 





--test like '%%'
select * from dbo.synapse_usp
where definition like '%ANALYSE.DT_RANGE_ANALYSIS_CUMULATIVE_CONTRIBUTION_CONFIG%'
or definition like '%[ANALYSE].[DT_RANGE_ANALYSIS_CUMULATIVE_CONTRIBUTION_CONFIG%]'



drop view dbo.v_outer_join

create view v_outer_join as
select 
    p.schema_name,
    p.routine_name as usp_name,
    p.definition,
    t.name1 as table_name1,
    t.name2 as table_name2
from 
    dbo.synapse_usp p, dbo.synapse_table t
where 1 =1;



/*
--'[' is a wildcard in sql like
create view v_does_contain_raw as
select 
    schema_name as usp_schema,
    usp_name,
    table_name1 as table_name,
        CAST(
            CASE
                WHEN (definition like '%'+table_name1 +'%') OR (definition like '%'+ table_name2 +'%')
                THEN 1
                ELSE 0
            END AS bit
        ) as does_contain,
        definition
from 
    dbo.v_outer_join
*/

create view [dbo].[v_does_contain_raw] as
select 
    schema_name as usp_schema,
    usp_name,
    table_name1 as table_name,
        CAST(
            CASE 
                WHEN REPLACE(REPLACE(definition,'[',''),']','') like '%'+table_name1 +'%'
                THEN 1
                --WHEN definition like '%'+ table_name2 +'%'
                --THEN 1
                ELSE 0
            END AS bit
        ) as does_contain,
        definition
from 
    dbo.v_outer_join
GO


ALTER view [dbo].[v_sp_does_contain_raw] as 
SELECT [schema_name]
      ,[usp_name]
      ,[definition]
      ,[schema_referenced]
      ,[usp_referenced]
      ,[usp_name1]
      ,[usp_name2]
      ,CAST(
            CASE
                WHEN (REPLACE(REPLACE(definition,'[',''),']','') like '%'+ usp_name1 +'%') 
                --OR (definition like '%'+ usp_name2 +'%')
                THEN 1
                ELSE 0
            END AS bit
        ) as does_contain_usp
  FROM [dbo].[v_sp_outer_join] 
GO




select * from dbo.v_does_contain_raw
where does_contain = 1



 create table usp2table(
  id int identity(1,1),
  usp_schema varchar(50),
  usp_name varchar(100),
  usp_full_name varchar(150),
  table_schema varchar(50),
  table_name varchar(100),
  table_full_name varchar(150)
  )


CREATE TABLE [dbo].[usp2usp](
    [id] [int] IDENTITY(1,1) NOT NULL,
    [usp_schema] [varchar](50) NULL,
    [usp_name] [varchar](100) NULL,
    [usp_full_name] [varchar](150) NULL,
    [ref_usp_schema] [varchar](50) NULL,
    [ref_usp_name] [varchar](100) NULL,
    [ref_usp_full_name] [varchar](150) NULL
) ON [PRIMARY]
GO

insert into dbo.usp2table
(
  usp_schema,
  usp_name,
  usp_full_name,
  table_schema,
  table_name,
  table_full_name
)
    SELECT
    usp_schema,
    usp_name,
    usp_schema+'.'+usp_name as usp_full_name,
    SUBSTRING(table_name, 0, charindex('.', table_name, 0)) as table_schema,
    SUBSTRING(table_name, charindex('.', table_name, 0)+1,len(table_name)) as table_name,
    table_name as table_full_name
    FROM [dbo].[v_does_contain_raw]
  WHERE [does_contain] = 1


insert into dbo.usp2usp
(
  usp_schema,
  usp_name,
  usp_full_name,
  ref_usp_schema,
  ref_usp_name,
  ref_usp_full_name
)
    SELECT
    schema_name,
    usp_name,
    schema_name + '.' + usp_name as usp_full_name,
    schema_referenced,
    usp_referenced,
    usp_name1 
    FROM [dbo].[v_sp_does_contain_raw]
  WHERE [does_contain_usp] = 1
  and schema_name + '.' + usp_name <> usp_name1 



  create view v_sp_outer_join as
select 
    p.schema_name,
    p.routine_name as usp_name,
    p.definition,
    t.schema_name as schema_referenced,
    t.routine_name as usp_referenced,
    t.schema_name + '.' + t.routine_name as usp_name1,
    '[' + t.schema_name + '].[' + t.routine_name + ']' as usp_name2
from 
    dbo.synapse_usp p, dbo.synapse_sp t
where 1 =1;


