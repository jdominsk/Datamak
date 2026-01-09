INSERT INTO gk_code (name, version)
SELECT 'GX', 'unknown'
WHERE NOT EXISTS (
    SELECT 1 FROM gk_code WHERE name = 'GX'
);

INSERT INTO gk_code (name, version)
SELECT 'CGYRO', 'unknown'
WHERE NOT EXISTS (
    SELECT 1 FROM gk_code WHERE name = 'CGYRO'
);
