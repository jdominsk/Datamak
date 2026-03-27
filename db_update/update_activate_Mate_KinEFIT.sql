-- Activate some of the pairs of pfile and gfile
update data_equil set active=1, shot_number='129038', shot_time=0.400 where pfile = 'p129038.00400' and gfile = 'g129038.00400';
update data_equil set active=1, shot_number='132588', shot_time=0.650 where pfile = 'p132588.00650' and gfile = 'g132588.00650';
update data_equil set active=1, shot_number='139057', shot_time=0.557 where pfile = 'p139057.00557' and gfile = 'g139057.00557';
update data_equil set active=1, shot_number='141300', shot_time=0.501 where pfile = 'p141300.00501' and gfile = 'g141300.00501';
update data_equil set active=1, shot_number='141309', shot_time=0.505 where pfile = 'p141309.00505' and gfile = 'g141309.00505';

-- Setup request for running simulations
INSERT INTO gk_study (data_equil_id, gk_code_id, COMMENT)
SELECT de.id, gc.id, 'auto-added'
FROM data_equil AS de
JOIN data_origin AS do
    ON do.id = de.data_origin_id
JOIN gk_code AS gc
    ON gc.name = 'GX'
LEFT JOIN gk_study AS gs
    ON gs.data_equil_id = de.id
WHERE gs.id IS NULL
AND de.active = 1
AND do.name IN ('Kinetic EFIT (Mate)', 'Mate Kinetic EFIT');
