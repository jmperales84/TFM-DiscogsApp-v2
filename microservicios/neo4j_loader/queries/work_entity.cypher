UNWIND $rows AS r
MERGE (w:Work {work_id: r.work_id})
ON CREATE SET w.work_title = r.work_title
ON MATCH  SET w.work_title = r.work_title;