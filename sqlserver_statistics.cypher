//load usp2table.csv to graph db

LOAD CSV WITH HEADERS FROM 'file:///table.csv' AS line
CREATE (:Table {name: line.name1, schema: line.schema, short_name: line.name, type: line.type, alias: line.name2, row_count: toInteger(line.row_count)})



MATCH (t:Table)
WHERE t.type = 'ANALYSE'
SET t:Analyse
RETURN t


MATCH (t:Table)
WHERE t.type = 'PREPARE'
SET t:Prepare
RETURN t

MATCH (t:Table)
WHERE LEFT(t.type,7) = 'PUBLISH'
SET t:Publish
RETURN t

MATCH (t:Table)
WHERE t.type = 'dbo'
SET t:dbo
RETURN t


//laod usp2usp.csv to graph db