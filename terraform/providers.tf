terraform {
    cloud{
        organization = "mr-Anas"
        workspaces {
            name = "secops-snowflake-workspace"
        }
    }

    required_providers  {
        snowflake = {
            source= "Snowflake-Labs/snowflake"
            version = "~> 0.87.0"
        }
    }
}

provider "snowflake" {
    account = "${var.snowflake_org_name}-${var.snowflake_acc_name}"
    user = var.snowflake_user
    password = var.snowflake_password
    role = "SYSADMIN"
}
