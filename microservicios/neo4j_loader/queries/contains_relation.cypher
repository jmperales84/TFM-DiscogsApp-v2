UNWIND $rows AS r
MATCH (a:Album {album_id: r.album_id})
MATCH (w:Work  {work_id:  r.work_id})
MERGE (a)-[:CONTAINS]->(w);