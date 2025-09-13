UNWIND $rows AS r
MERGE (p:Artist {artist_id: r.artist_id})
SET p.name = r.name;