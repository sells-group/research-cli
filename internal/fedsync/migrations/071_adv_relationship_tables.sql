-- Normalized relationship tables for cross-advisor analytics.

CREATE TABLE IF NOT EXISTS fed_data.adv_custodian_relationships (
    crd_number      INTEGER NOT NULL,
    custodian_name  VARCHAR(200) NOT NULL,
    relationship    VARCHAR(50) DEFAULT 'custodian',
    updated_at      TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (crd_number, custodian_name)
);

CREATE TABLE IF NOT EXISTS fed_data.adv_bd_affiliations (
    crd_number     INTEGER NOT NULL,
    bd_name        VARCHAR(200) NOT NULL,
    bd_crd         INTEGER,
    relationship   VARCHAR(50) DEFAULT 'affiliated',
    updated_at     TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (crd_number, bd_name)
);

CREATE TABLE IF NOT EXISTS fed_data.adv_service_providers (
    crd_number     INTEGER NOT NULL,
    provider_name  VARCHAR(200) NOT NULL,
    provider_type  VARCHAR(50) NOT NULL,
    updated_at     TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (crd_number, provider_name, provider_type)
);

CREATE INDEX IF NOT EXISTS idx_custodian_rel_name
    ON fed_data.adv_custodian_relationships (custodian_name);
CREATE INDEX IF NOT EXISTS idx_bd_aff_name
    ON fed_data.adv_bd_affiliations (bd_name);
CREATE INDEX IF NOT EXISTS idx_svc_provider_type
    ON fed_data.adv_service_providers (provider_type);
