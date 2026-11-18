resource "snowflake_warehouse" "secops_wh" {
    name                = "SECOPS_WH"
    warehouse_size      = "X-SMALL"
    auto-suspend        = 300
    initially_suspended = true
}

resource "snowflake_database" "secopps_db" {
    name = "SECOPPS_DB"
    comment = "database for real-time kafka network security logs and analytics"
}

resource "snowflake_schema" "raw_schema" {
    database = snowflake_database.secopps_db.name 
    name = "RAW"
    comment = "Raw landing schema for kafka secops data"
}