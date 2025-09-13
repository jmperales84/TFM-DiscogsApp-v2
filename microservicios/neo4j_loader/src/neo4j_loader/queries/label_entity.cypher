UNWIND $rows AS r
MERGE (l:Label {label_id: r.label_id})
SET l.name = r.name;