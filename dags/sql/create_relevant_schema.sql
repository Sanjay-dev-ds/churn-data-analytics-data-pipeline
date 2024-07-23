CREATE SCHEMA IF NOT EXISTS telco_internal ;
create external schema IF NOT EXISTS telco_external
from data catalog
database 'telco_meta_db'
iam_role 'arn:aws:iam::058264127733:role/telcoIAMrole'
create external database if not exists;