-- Run in one transaction after backup. Raw observations and fact identities stay intact.
-- Re-running this catalog update is safe; it does not replay or delete observations.
INSERT INTO market_dw.dim_position
    (position_id,position_name,in_scope,role_family,classification_version)
VALUES ('data_analyst','Data Analyst',true,'Data Analytics','topcv-cleaning-v1')
ON CONFLICT (position_id) DO UPDATE SET
    position_name=EXCLUDED.position_name,
    in_scope=EXCLUDED.in_scope,
    role_family=EXCLUDED.role_family,
    classification_version=EXCLUDED.classification_version;

UPDATE market_dw.dim_position
SET in_scope=false
WHERE position_id='frontend_developer';

UPDATE market_dw.fct_job_snapshot
SET position_status='outside_requested_positions'
WHERE position_id='frontend_developer'
  AND position_status IS DISTINCT FROM 'outside_requested_positions';

-- Data Analyst was previously retained under a stable out-of-scope identifier.
UPDATE market_dw.fct_job_snapshot AS f
SET position_id='data_analyst',position_status='classified'
FROM market_dw.dim_position AS p
WHERE f.position_id=p.position_id
  AND p.position_name='Data Analyst'
  AND p.position_id LIKE 'other:%';

UPDATE market_dw.dim_position
SET in_scope=false
WHERE position_name='Data Analyst' AND position_id LIKE 'other:%';

GRANT SELECT ON market_dw.dim_position TO market_reader;
