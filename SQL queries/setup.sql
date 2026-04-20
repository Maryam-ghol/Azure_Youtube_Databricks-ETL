-- Use this script only once for the first time to create the catalog and schemas

CREATE CATALOG IF NOT EXISTS workspace_yt_mar;

CREATE SCHEMA IF NOT EXISTS workspace_yt_mar.bronze;
CREATE SCHEMA IF NOT EXISTS workspace_yt_mar.silver;
CREATE SCHEMA IF NOT EXISTS workspace_yt_mar.gold;