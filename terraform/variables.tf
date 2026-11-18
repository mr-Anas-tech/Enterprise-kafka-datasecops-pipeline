variable "snowflake_org_name" {
    type      = string
    description = "snowflake_organization_name"
}

variable "snowflake_acc_name" {
    type      = string
    description = "snowflake_account_name"
}

variable "snowflake_user" {
    type      = string 
    description = "snowflake Admin user Name"
}

variable "snowflake_password" {
    type      = string
    description ="snowflake user password"
    sensitive = true
}