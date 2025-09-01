UNWIND $rows AS r
WITH r WHERE r.role IN ['musician','leader']
MATCH (a:Album {album_id: r.album_id})
MATCH (p:Artist {artist_id: r.artist_id})
MERGE (p)-[:PLAYS_IN]->(a);