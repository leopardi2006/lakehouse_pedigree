
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
