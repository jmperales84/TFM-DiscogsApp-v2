UNWIND $rows AS r
MATCH (a:Album {album_id: r.album_id})
MATCH (l:Label {label_id: r.label_id})
MERGE (a)-[:RELEASED_BY]->(l);