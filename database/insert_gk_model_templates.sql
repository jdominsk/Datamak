-- Populate gk_model with GX templates.
-- Requires SQLite readfile() support (sqlite3 CLI).
-- Run from repo root so the relative paths resolve.

INSERT OR IGNORE INTO gk_model (is_linear, is_adiabatic, is_electrostatic, input_template, gk_code_id, active)
VALUES (
    1,
    1,
    1,
    'gx_template_miller_linear_adiabe.in',
    (SELECT id FROM gk_code WHERE name = 'GX' ORDER BY id LIMIT 1),
    1
);

INSERT OR IGNORE INTO gk_model (is_linear, is_adiabatic, is_electrostatic, input_template, gk_code_id)
VALUES (
    1,
    0,
    0,
    'gx_template_miller_linear_kine.in',
    (SELECT id FROM gk_code WHERE name = 'GX' ORDER BY id LIMIT 1)
);

INSERT OR IGNORE INTO gk_model (is_linear, is_adiabatic, is_electrostatic, input_template, gk_code_id)
VALUES (
    0,
    1,
    0,
    'gx_template_miller_nonlinear_adiabe.in',
    (SELECT id FROM gk_code WHERE name = 'GX' ORDER BY id LIMIT 1)
);

INSERT OR IGNORE INTO gk_model (is_linear, is_adiabatic, is_electrostatic, input_template, gk_code_id)
VALUES (
    0,
    0,
    0,
    'gx_template_miller_nonlinear_kine.in',
    (SELECT id FROM gk_code WHERE name = 'GX' ORDER BY id LIMIT 1)
);

INSERT OR IGNORE INTO gk_model (is_linear, is_adiabatic, is_electrostatic, input_template, gk_code_id)
VALUES (
    0,
    0,
    1,
    'gx_template_miller_nonlinear_kine_electrostatic_lowbeta.in',
    (SELECT id FROM gk_code WHERE name = 'GX' ORDER BY id LIMIT 1)
);
