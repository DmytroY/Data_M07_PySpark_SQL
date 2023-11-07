
# Spark ETL on Databricks
## Prerequisites
Installed latest:   
- Azure CLI, 
- terraform,  
- spark installed to /opt/spark/

Login to Azure:
```
az login
```

## Terraform   
Create Azure Resource Group and Storage Account, get Storage Account key:
```
az group create --name tf-state-rg \
  --location westeurope

az storage account create --name sa451 \
  --location westeurope \
  --resource-group tf-state-rg

az storage account keys list --account-name sa451
```
Use key from abow, create a container so Terraform can store the state management file:
```
az storage container create --account-name sa451 \
  --name tfstate \
  --public-access off \
  --account-key <account-key>
```
## Data
Put your data to the Azure storage account blob container. Now it can be accessible from pyspark console. Just start pyspark this way:
```
pyspark \
  --conf spark.hadoop.fs.azure.account.key.<acc>.dfs.core.windows.net=<key> \
  --packages org.apache.hadoop:hadoop-azure:3.2.0,com.microsoft.azure:azure-storage:8.6.3,org.apache.spark:spark-avro_2.12:3.4.0
```
where:   
**acc** - storage account name,   
**key** - storage account key.  

You can access the data by such path:  
```
data_path = f"abfs://{container}>@{account}.dfs.core.windows.net"


======= original readme ========
* Add your code in `src/main/` if needed
* Test your code with `src/tests/` if needed
* Modify notebooks for your needs
* Deploy infrastructure with terraform
```
terraform init
terraform plan -out terraform.plan
terraform apply terraform.plan
....
terraform destroy
```
* Launch notebooks on Databricks cluster
