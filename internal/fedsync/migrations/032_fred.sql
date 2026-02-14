CREATE TABLE IF NOT EXISTS fed_data.fred_series (
    series_id   VARCHAR(30) NOT NULL,
    obs_date    DATE        NOT NULL,
    value       NUMERIC,
    PRIMARY KEY (series_id, obs_date)
);
