variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "name_prefix" { type = string }
variable "tags" { type = map(string) }
variable "storage_account_id" { type = string }

resource "azurerm_databricks_workspace" "main" {
  name                = "${var.name_prefix}-dbw"
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "premium"
  tags                = var.tags

  custom_parameters {
    no_public_ip = true
  }
}

output "workspace_url" {
  value = azurerm_databricks_workspace.main.workspace_url
}

output "workspace_id" {
  value = azurerm_databricks_workspace.main.id
}
