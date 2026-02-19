CREATE SCHEMA IF NOT EXISTS GLOBAL_QUALITY_DB;
USE GLOBAL_QUALITY_DB;
-- Esto crea una carpeta segura dentro de tu base de datos donde SÍ tienes permisos
CREATE VOLUME workspace.global_quality_db.mis_checkpoints;

-- ==========================================
-- 1. TABLAS DIMENSIONALES
-- ==========================================

CREATE OR REPLACE TABLE DIM_PLANT (
    PLT_COD STRING COMMENT 'Código Planta (PK)',
    PLT_NAM STRING COMMENT 'Nombre Planta',
    CTRY_COD STRING COMMENT 'País',
    PLT_TYP STRING COMMENT 'Tipo de Producción'
) USING DELTA;

INSERT INTO DIM_PLANT VALUES 
('PLT_ES01', 'Planta Sur - Huelva', 'ES', 'Lácteos y Frutas'),
('PLT_FR02', 'Planta Central - Lyon', 'FR', 'Lácteos Base'),
('PLT_DE03', 'Planta Norte - Múnich', 'DE', 'Alternativas Veganas');


CREATE OR REPLACE TABLE DIM_MATERIAL (
    MAT_COD STRING COMMENT 'Código Material SAP (PK)',
    MAT_DSC STRING COMMENT 'Descripción',
    MAT_TYP_COD STRING COMMENT 'Familia',
    TTSN_LEVEL STRING COMMENT 'Complejidad / Tiempo Trabajo Socialmente Necesario'
) USING DELTA;


INSERT INTO DIM_MATERIAL VALUES 
('MAT_100', 'Yogur Natural 125g', 'YOG_BAS', 'BAJO'),
('MAT_200', 'Yogur Fresa 125g', 'YOG_FRU', 'ALTO'),
('MAT_300', 'Postre Soja Natural', 'VEG_BAS', 'MEDIO');


CREATE OR REPLACE TABLE DIM_CONTAMINANT (
    CONT_COD STRING COMMENT 'Código Contaminante',
    CONT_DSC STRING COMMENT 'Nombre Químico',
    LIM_MAX_MDF_VAL DOUBLE COMMENT 'Límite Máximo Legal',
    QTY_UOM STRING COMMENT 'Unidad de Medida'
) USING DELTA;

INSERT INTO DIM_CONTAMINANT VALUES 
('C_PHOS', 'Phosphonic Acid / Fosfonatos', 0.05, 'mg/kg'),
('C_CHLO', 'Chlorate / Cloratos', 0.01, 'mg/kg'),
('C_SALM', 'Salmonella spp.', 0.00, 'cfu/25g');


-- ==========================================
-- 2. TABLAS DE HECHOS (La realidad diaria)
-- ==========================================



CREATE OR REPLACE TABLE FACT_PRODUCTION (
    PRD_DATE DATE,
    PLT_COD STRING,
    LINE_COD STRING,
    BAT_COD STRING COMMENT 'Lote de Fabricación',
    MAT_COD STRING,
    PRD_QTY_KG DOUBLE
) USING DELTA;

INSERT INTO FACT_PRODUCTION VALUES 
('2026-02-19', 'PLT_ES01', 'LIN_A', 'BAT_ES_001', 'MAT_100', 15000.00), -- Lote bueno
('2026-02-19', 'PLT_ES01', 'LIN_B', 'BAT_ES_002', 'MAT_200', 22000.00), -- Lote CONTAMINADO (Fresa) para nuestras pruebas
('2026-02-19', 'PLT_FR02', 'LIN_A', 'BAT_FR_001', 'MAT_100', 50000.00); -- Lote Francés bueno

CREATE OR REPLACE TABLE FACT_PALLET_STOCK (
    PALLET_COD STRING COMMENT 'ID Único del Pallet (SSCC)',
    BAT_COD STRING,
    MAT_COD STRING,
    PLT_COD STRING,
    ENVS_TST TIMESTAMP COMMENT 'Momento exacto de envasado',
    STATUS_IND STRING COMMENT 'Estado: RELEASED, BLOCKED, QUALITY_PENDING'
) USING DELTA;

INSERT INTO FACT_PALLET_STOCK VALUES 
('PAL_9901', 'BAT_ES_002', 'MAT_200', 'PLT_ES01', '2026-02-19 10:05:00', 'PENDING'),
('PAL_9902', 'BAT_ES_002', 'MAT_200', 'PLT_ES01', '2026-02-19 10:45:00', 'PENDING'),
('PAL_9903', 'BAT_ES_002', 'MAT_200', 'PLT_ES01', '2026-02-19 11:15:00', 'PENDING'),
('PAL_8801', 'BAT_FR_001', 'MAT_100', 'PLT_FR02', '2026-02-19 08:30:00', 'RELEASED'),
('PAL_8802', 'BAT_FR_001', 'MAT_100', 'PLT_FR02', '2026-02-19 09:00:00', 'RELEASED');

CREATE OR REPLACE TABLE FACT_QUALITY_SAMPLES (
    SAM_ID STRING,
    BAT_COD STRING,
    CONT_COD STRING,
    MDF_VAL DOUBLE COMMENT 'Valor Medido',
    FAIL_IND INT COMMENT '1 = Fallo, 0 = OK'
) USING DELTA;

-- ==========================================
-- 3. TABLAS DE HECHOS (La realidad diaria)
-- ==========================================

INSERT INTO workspace.global_quality_db.f_metadata_logics 
VALUES (
    'PALLET_QUALITY_BLOCKER', 
    TRUE, 
    'workspace.global_quality_db.fact_pallet_stock', 
    'TABLE', 
    'vw_micro_batch_source', 
    'VIEW', 
    'BAT_COD',
    '[
        {
            "action": "update",
            "condition": "source.FINAL_FAIL_IND = 1 AND target.STATUS_IND != ''BLOCKED_QUALITY''",
            "set": {"STATUS_IND": "''BLOCKED_QUALITY''"}
        },
        {
            "action": "update",
            "condition": "source.FINAL_FAIL_IND = 0 AND target.STATUS_IND != ''RELEASED''",
            "set": {"STATUS_IND": "''RELEASED''"}
        }
    ]'
);


UPDATE workspace.global_quality_db.f_metadata_logics
SET LOGIC_PAYLOAD = '[
    {
        "action": "update",
        "condition": "source.FINAL_FAIL_IND = 1 AND target.STATUS_IND != \'BLOCKED_QUALITY\'",
        "set": {"STATUS_IND": "\'BLOCKED_QUALITY\'"}
    },
    {
        "action": "update",
        "condition": "source.FINAL_FAIL_IND = 0 AND target.STATUS_IND != \'RELEASED\'",
        "set": {"STATUS_IND": "\'RELEASED\'"}
    }
]'
WHERE PROCESS_NAME = 'PALLET_QUALITY_BLOCKER';
