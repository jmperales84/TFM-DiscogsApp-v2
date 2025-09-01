UNWIND $rows AS r
MERGE (l:Label {label_id: r.label_id})
ON CREATE SET l.name = r.name
ON MATCH  SET l.name = r.name;