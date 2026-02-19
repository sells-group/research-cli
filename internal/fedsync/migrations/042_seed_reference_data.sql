-- Seed reference tables: naics_codes, fips_codes, sic_crosswalk.
-- Idempotent: uses INSERT ... ON CONFLICT DO NOTHING.

-- =============================================================================
-- NAICS 2-digit sector codes (20 sectors)
-- =============================================================================
INSERT INTO fed_data.naics_codes (code, title, sector) VALUES
('11', 'Agriculture, Forestry, Fishing and Hunting', '11'),
('21', 'Mining, Quarrying, and Oil and Gas Extraction', '21'),
('22', 'Utilities', '22'),
('23', 'Construction', '23'),
('31', 'Manufacturing', '31'),
('32', 'Manufacturing', '32'),
('33', 'Manufacturing', '33'),
('42', 'Wholesale Trade', '42'),
('44', 'Retail Trade', '44'),
('45', 'Retail Trade', '45'),
('48', 'Transportation and Warehousing', '48'),
('49', 'Transportation and Warehousing', '49'),
('51', 'Information', '51'),
('52', 'Finance and Insurance', '52'),
('53', 'Real Estate and Rental and Leasing', '53'),
('54', 'Professional, Scientific, and Technical Services', '54'),
('55', 'Management of Companies and Enterprises', '55'),
('56', 'Administrative and Support and Waste Management and Remediation Services', '56'),
('61', 'Educational Services', '61'),
('62', 'Health Care and Social Assistance', '62'),
('71', 'Arts, Entertainment, and Recreation', '71'),
('72', 'Accommodation and Food Services', '72'),
('81', 'Other Services (except Public Administration)', '81'),
('92', 'Public Administration', '92')
ON CONFLICT DO NOTHING;

-- =============================================================================
-- FIPS state codes (50 states + DC + territories)
-- =============================================================================
INSERT INTO fed_data.fips_codes (fips_state, fips_county, state_name, state_abbr) VALUES
('01', '000', 'Alabama', 'AL'),
('02', '000', 'Alaska', 'AK'),
('04', '000', 'Arizona', 'AZ'),
('05', '000', 'Arkansas', 'AR'),
('06', '000', 'California', 'CA'),
('08', '000', 'Colorado', 'CO'),
('09', '000', 'Connecticut', 'CT'),
('10', '000', 'Delaware', 'DE'),
('11', '000', 'District of Columbia', 'DC'),
('12', '000', 'Florida', 'FL'),
('13', '000', 'Georgia', 'GA'),
('15', '000', 'Hawaii', 'HI'),
('16', '000', 'Idaho', 'ID'),
('17', '000', 'Illinois', 'IL'),
('18', '000', 'Indiana', 'IN'),
('19', '000', 'Iowa', 'IA'),
('20', '000', 'Kansas', 'KS'),
('21', '000', 'Kentucky', 'KY'),
('22', '000', 'Louisiana', 'LA'),
('23', '000', 'Maine', 'ME'),
('24', '000', 'Maryland', 'MD'),
('25', '000', 'Massachusetts', 'MA'),
('26', '000', 'Michigan', 'MI'),
('27', '000', 'Minnesota', 'MN'),
('28', '000', 'Mississippi', 'MS'),
('29', '000', 'Missouri', 'MO'),
('30', '000', 'Montana', 'MT'),
('31', '000', 'Nebraska', 'NE'),
('32', '000', 'Nevada', 'NV'),
('33', '000', 'New Hampshire', 'NH'),
('34', '000', 'New Jersey', 'NJ'),
('35', '000', 'New Mexico', 'NM'),
('36', '000', 'New York', 'NY'),
('37', '000', 'North Carolina', 'NC'),
('38', '000', 'North Dakota', 'ND'),
('39', '000', 'Ohio', 'OH'),
('40', '000', 'Oklahoma', 'OK'),
('41', '000', 'Oregon', 'OR'),
('42', '000', 'Pennsylvania', 'PA'),
('44', '000', 'Rhode Island', 'RI'),
('45', '000', 'South Carolina', 'SC'),
('46', '000', 'South Dakota', 'SD'),
('47', '000', 'Tennessee', 'TN'),
('48', '000', 'Texas', 'TX'),
('49', '000', 'Utah', 'UT'),
('50', '000', 'Vermont', 'VT'),
('51', '000', 'Virginia', 'VA'),
('53', '000', 'Washington', 'WA'),
('54', '000', 'West Virginia', 'WV'),
('55', '000', 'Wisconsin', 'WI'),
('56', '000', 'Wyoming', 'WY'),
-- Territories
('60', '000', 'American Samoa', 'AS'),
('66', '000', 'Guam', 'GU'),
('69', '000', 'Northern Mariana Islands', 'MP'),
('72', '000', 'Puerto Rico', 'PR'),
('78', '000', 'U.S. Virgin Islands', 'VI')
ON CONFLICT DO NOTHING;

-- =============================================================================
-- SIC to NAICS crosswalk (core mappings from Census crosswalk)
-- Financial services, professional services, and key industrial sectors
-- =============================================================================
INSERT INTO fed_data.sic_crosswalk (sic_code, sic_description, naics_code, naics_description) VALUES
-- Finance and Insurance
('6020', 'State Commercial Banks', '522110', 'Commercial Banking'),
('6021', 'National Commercial Banks', '522110', 'Commercial Banking'),
('6022', 'State Commercial Banks - Federal Reserve', '522110', 'Commercial Banking'),
('6035', 'Savings Institutions, Federally Chartered', '522120', 'Savings Institutions'),
('6036', 'Savings Institutions, Not Federally Chartered', '522120', 'Savings Institutions'),
('6099', 'Services Allied with Banking, NEC', '522390', 'Other Activities Related to Credit Intermediation'),
('6111', 'Federal and Federally-Sponsored Credit Agencies', '522293', 'International Trade Financing'),
('6141', 'Personal Credit Institutions', '522210', 'Credit Card Issuing'),
('6153', 'Short-Term Business Credit Institutions', '522220', 'Sales Financing'),
('6159', 'Federal-Sponsored Credit Agencies, NEC', '522298', 'All Other Nondepository Credit Intermediation'),
('6162', 'Mortgage Bankers and Loan Correspondents', '522310', 'Mortgage and Nonmortgage Loan Brokers'),
('6163', 'Loan Brokers', '522310', 'Mortgage and Nonmortgage Loan Brokers'),
('6211', 'Security Brokers and Dealers', '523110', 'Investment Banking and Securities Dealing'),
('6282', 'Investment Advice', '523930', 'Investment Advice'),
('6311', 'Life Insurance', '524113', 'Direct Life Insurance Carriers'),
('6321', 'Accident and Health Insurance', '524114', 'Direct Health and Medical Insurance Carriers'),
('6331', 'Fire, Marine, and Casualty Insurance', '524126', 'Direct Property and Casualty Insurance Carriers'),
('6411', 'Insurance Agents, Brokers, and Service', '524210', 'Insurance Agencies and Brokerages'),
-- Professional Services
('7311', 'Services-Advertising Services', '541810', 'Advertising Agencies'),
('7371', 'Computer Programming, Data Processing', '541511', 'Custom Computer Programming Services'),
('7372', 'Prepackaged Software', '511210', 'Software Publishers'),
('7374', 'Computer Processing and Data Preparation', '518210', 'Data Processing, Hosting, and Related Services'),
('7389', 'Services-Misc Business Services NEC', '541990', 'All Other Professional, Scientific, and Technical Services'),
('8711', 'Engineering Services', '541330', 'Engineering Services'),
('8721', 'Accounting, Auditing, and Bookkeeping', '541211', 'Offices of Certified Public Accountants'),
('8741', 'Management Services', '541611', 'Administrative Management and General Management Consulting'),
('8742', 'Management Consulting Services', '541611', 'Administrative Management and General Management Consulting'),
('8999', 'Services, NEC', '541990', 'All Other Professional, Scientific, and Technical Services'),
-- Real Estate
('6512', 'Operators of Apartment Buildings', '531110', 'Lessors of Residential Buildings and Dwellings'),
('6531', 'Real Estate Agents and Managers', '531210', 'Offices of Real Estate Agents and Brokers'),
('6552', 'Land Subdividers and Developers', '531390', 'Other Activities Related to Real Estate'),
-- Manufacturing
('2011', 'Meat Packing Plants', '311611', 'Animal Slaughtering'),
('2041', 'Cereal Breakfast Foods', '311230', 'Breakfast Cereal Manufacturing'),
('2086', 'Bottled and Canned Soft Drinks', '312111', 'Soft Drink Manufacturing'),
('2834', 'Pharmaceutical Preparations', '325411', 'Medicinal and Botanical Manufacturing'),
('3559', 'Special Industry Machinery, NEC', '333249', 'Other Industrial Machinery Manufacturing'),
('3674', 'Semiconductors and Related Devices', '334413', 'Semiconductor and Related Device Manufacturing'),
('3721', 'Aircraft', '336411', 'Aircraft Manufacturing'),
('3812', 'Defense Electronics and Communications', '334511', 'Search, Detection, Navigation, Guidance, and Aeronautical Systems'),
-- Healthcare
('8011', 'Offices and Clinics of Doctors of Medicine', '621111', 'Offices of Physicians'),
('8021', 'Offices and Clinics of Dentists', '621210', 'Offices of Dentists'),
('8049', 'Offices and Clinics of Other Health Practitioners', '621399', 'Offices of All Other Miscellaneous Health Practitioners'),
('8062', 'General Medical and Surgical Hospitals', '622110', 'General Medical and Surgical Hospitals'),
('8082', 'Home Health Care Services', '621610', 'Home Health Care Services'),
-- Construction
('1521', 'General Contractors-Residential Buildings', '236115', 'New Single-Family Housing Construction'),
('1522', 'General Contractors-Residential Buildings', '236116', 'New Multifamily Housing Construction'),
('1541', 'General Contractors-Industrial Buildings', '236210', 'Industrial Building Construction'),
('1542', 'General Contractors-Nonresidential Buildings', '236220', 'Commercial and Institutional Building Construction')
ON CONFLICT DO NOTHING;
