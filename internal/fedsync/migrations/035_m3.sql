CREATE TABLE IF NOT EXISTS fed_data.m3_data (
    category    VARCHAR(50) NOT NULL,
    data_type   VARCHAR(20) NOT NULL,
    year        SMALLINT    NOT NULL,
    month       SMALLINT    NOT NULL,
    value       BIGINT,
    PRIMARY KEY (category, data_type, year, month)
);
