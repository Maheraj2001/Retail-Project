variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "name_prefix" { type = string }
variable "tags" { type = map(string) }
variable "keyvault_id" { type = string }
variable "storage_account_id" { type = string }

resource "azurerm_data_factory" "main" {
  name                = "${var.name_prefix}-adf"
  location            = var.location
  resource_group_name = var.resource_group_name
  tags                = var.tags

  identity {
    type = "SystemAssigned"
  }

  github_configuration {
    account_name    = "your-github-account"
    repository_name = "datalakehouse-poc"
    branch_name     = "main"
    root_folder     = "/pipelines/adf"
    git_url         = "https://github.com"
  }
}

output "data_factory_name" {
  value = azurerm_data_factory.main.name
}

output "data_factory_id" {
  value = azurerm_data_factory.main.id
}

output "data_factory_identity" {
  value = azurerm_data_factory.main.identity[0].principal_id
}
