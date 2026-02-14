CREATE TABLE IF NOT EXISTS fed_data.adv_private_funds (
    crd_number      INTEGER     NOT NULL,
    fund_id         VARCHAR(20) NOT NULL,
    fund_name       VARCHAR(300),
    fund_type       VARCHAR(100),
    gross_asset_value BIGINT,
    net_asset_value   BIGINT,
    PRIMARY KEY (crd_number, fund_id)
);
