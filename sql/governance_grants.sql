GRANT USAGE ON CATALOG novarota TO `bi_analysts`;
GRANT USAGE ON CATALOG novarota TO `data_engineers`;

GRANT ALL PRIVILEGES ON SCHEMA novarota.bronze TO `data_engineers`;
REVOKE ALL PRIVILEGES ON SCHEMA novarota.bronze FROM `bi_analysts`;

GRANT SELECT ON SCHEMA novarota.silver TO `data_engineers`;

GRANT USAGE, SELECT ON SCHEMA novarota.gold TO `bi_analysts`;
GRANT USAGE, SELECT ON SCHEMA novarota.gold TO `data_scientists`;