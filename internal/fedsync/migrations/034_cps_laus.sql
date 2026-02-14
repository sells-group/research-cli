CREATE TABLE IF NOT EXISTS fed_data.laus_data (
    series_id   VARCHAR(20) NOT NULL,
    year        SMALLINT    NOT NULL,
    period      VARCHAR(3)  NOT NULL,
    value       NUMERIC(12,1),
    PRIMARY KEY (series_id, year, period)
);
