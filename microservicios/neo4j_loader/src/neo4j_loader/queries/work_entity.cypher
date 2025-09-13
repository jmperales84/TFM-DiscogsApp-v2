UNWIND $rows AS r
MERGE (w:Work {work_id: r.work_id})
SET w.work_title = r.work_title;