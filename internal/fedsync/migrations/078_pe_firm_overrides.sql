-- 078: PE firm website URL overrides
-- Manual overrides for PE firm website URLs when automatic resolution
-- from ADV/EDGAR produces incorrect results (e.g., subsidiary sites, social media).

CREATE TABLE IF NOT EXISTS fed_data.pe_firm_overrides (
    pe_firm_id          BIGINT NOT NULL REFERENCES fed_data.pe_firms(pe_firm_id),
    website_url_override VARCHAR(500) NOT NULL,
    notes               TEXT,
    created_by          VARCHAR(100) NOT NULL DEFAULT 'manual',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pe_firm_id)
);

-- Pre-seed known PE/aggregator firm websites.
-- Uses name matching since pe_firm_id values are auto-generated.
INSERT INTO fed_data.pe_firm_overrides (pe_firm_id, website_url_override, notes, created_by)
SELECT pf.pe_firm_id, seed.url, seed.notes, 'seed'
FROM (VALUES
    ('Focus Financial Partners',    'https://focusfinancialpartners.com',   'Major RIA aggregator'),
    ('Hightower',                   'https://hightoweradvisors.com',        'Major RIA aggregator'),
    ('CI Financial',                'https://cifinancial.com',              'Canadian wealth platform'),
    ('Mercer Global Advisors',      'https://merceradvisors.com',           'National RIA consolidator'),
    ('Wealth Enhancement Group',    'https://wealthenhancement.com',        'RIA aggregator'),
    ('Cerity Partners',             'https://ceritypartners.com',           'PE-backed RIA platform'),
    ('Beacon Pointe Advisors',      'https://beaconpointe.com',             'RIA aggregator'),
    ('Captrust',                    'https://captrust.com',                 'Major RIA consolidator'),
    ('Carson Group',                'https://carsongroup.com',              'Advisor services platform'),
    ('Mariner Wealth Advisors',     'https://marinerwealthadvisors.com',    'National RIA platform'),
    ('Savant Wealth',               'https://savantwealth.com',             'RIA aggregator'),
    ('Creative Planning',           'https://creativeplanning.com',         'Major RIA aggregator'),
    ('Sanctuary Wealth',            'https://sanctuarywealth.com',          'Advisor independence platform'),
    ('Advisor Group',               'https://advisorgroup.com',             'Broker-dealer network'),
    ('LPL Financial',               'https://lpl.com',                      'Largest independent BD'),
    ('Clayton, Dubilier & Rice',    'https://cdr.com',                      'PE firm'),
    ('Ares Management',             'https://aresmgmt.com',                 'Alternative investment manager'),
    ('GTCR',                        'https://gtcr.com',                     'PE firm'),
    ('Warburg Pincus',              'https://warburgpincus.com',            'Global PE firm'),
    ('TPG',                         'https://tpg.com',                      'Global alternative asset firm'),
    ('Genstar Capital',             'https://genstarcapital.com',           'PE firm'),
    ('TA Associates',               'https://ta.com',                       'Global growth PE firm'),
    ('Hellman & Friedman',          'https://hfrp.com',                     'PE firm'),
    ('Stone Point Capital',         'https://stonepoint.com',               'Financial services PE'),
    ('Parthenon Capital',           'https://parthenoncapital.com',         'Financial services PE'),
    ('Reverence Capital Partners',  'https://reverencecapital.com',         'Financial services PE'),
    ('Kudu Investment Management',  'https://kuduinvestment.com',           'RIA minority stake investor'),
    ('Emigrant Partners',           'https://emigrantpartners.com',         'RIA equity partner')
) AS seed(name, url, notes)
JOIN fed_data.pe_firms pf ON pf.firm_name ILIKE '%' || seed.name || '%'
ON CONFLICT (pe_firm_id) DO NOTHING;
