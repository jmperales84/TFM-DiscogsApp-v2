UNWIND $rows AS r
MERGE (p:Artist {artist_id: r.artist_id})
ON CREATE SET p.name = r.name
ON MATCH  SET p.name = r.name;