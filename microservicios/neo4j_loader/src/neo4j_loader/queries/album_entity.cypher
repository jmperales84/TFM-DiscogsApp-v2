UNWIND $rows AS r
MERGE (a:Album {album_id: r.album_id})
ON CREATE SET a.ensemble = r.ensemble, a.title = r.title, a.year = r.year, a.label = r.label,
              a.styles = r.styles, a.cover_url = r.cover_url
ON MATCH  SET a.ensemble = r.ensemble, a.title = r.title, a.year = r.year, a.label = r.label,
              a.styles = r.styles, a.cover_url = r.cover_url;