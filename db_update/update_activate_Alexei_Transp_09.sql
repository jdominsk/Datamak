UPDATE data_equil 
SET active = 1,shot_number = '133964', shot_time = 0.501 WHERE transpfile = '133964I20.CDF';

UPDATE data_equil
SET active = 1, shot_number = '133964', shot_time = 0.501 WHERE transpfile = '133964I63.CDF';

UPDATE data_equil
SET active = 1, shot_number = '133964', shot_time = 0.501 WHERE transpfile = '133964I85.CDF';

INSERT INTO data_equil (data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, shot_time, active)
SELECT data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, 0.696, active FROM data_equil WHERE transpfile = '133964I20.CDF'
LIMIT 1;

INSERT INTO data_equil (data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, shot_time, active)
SELECT data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, 0.696, active FROM data_equil WHERE transpfile = '133964I63.CDF'
LIMIT 1;

INSERT INTO data_equil (data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, shot_time, active)
SELECT data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, 0.696, active FROM data_equil WHERE transpfile = '133964I85.CDF'
LIMIT 1;

INSERT INTO data_equil (data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, shot_time, active)
SELECT data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, 0.865, active FROM data_equil WHERE transpfile = '133964I20.CDF'
LIMIT 1;

INSERT INTO data_equil (data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, shot_time, active)
SELECT data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, 0.865, active FROM data_equil WHERE transpfile = '133964I63.CDF'
LIMIT 1;

INSERT INTO data_equil (data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, shot_time, active)
SELECT data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, 0.865, active FROM data_equil WHERE transpfile = '133964I85.CDF'
LIMIT 1;

INSERT INTO data_equil (data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, shot_time, active)
SELECT data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, 1.015, active FROM data_equil WHERE transpfile = '133964I20.CDF'
LIMIT 1;

INSERT INTO data_equil (data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, shot_time, active)
SELECT data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, 1.015, active FROM data_equil WHERE transpfile = '133964I63.CDF'
LIMIT 1;

INSERT INTO data_equil (data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, shot_time, active)
SELECT data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, 1.015, active FROM data_equil WHERE transpfile = '133964I85.CDF'
LIMIT 1;

INSERT INTO data_equil (data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, shot_time, active)
SELECT data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, 0.350, active FROM data_equil WHERE transpfile = '133964I20.CDF'
LIMIT 1;

INSERT INTO data_equil (data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, shot_time, active)
SELECT data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, 0.350, active FROM data_equil WHERE transpfile = '133964I63.CDF'
LIMIT 1;

INSERT INTO data_equil (data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, shot_time, active)
SELECT data_origin_id, folder_path, pfile, pfile_content, gfile, gfile_content, transpfile, shot_number, 0.350, active FROM data_equil WHERE transpfile = '133964I85.CDF'
LIMIT 1;

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
AND do.name = 'Alexei Transp 09 (semi-auto)';
