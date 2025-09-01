UNWIND $rows AS r
WITH r WHERE r.role = 'leader'
MATCH (a:Album {album_id: r.album_id})
MATCH (p:Artist {artist_id: r.artist_id})
MERGE (p)-[:LEADS]->(a);