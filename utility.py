# Databricks notebook source
# Put all functions in same cell so they can always access each other? Spec for get_client_config being used in others

# COMMAND ----------

# Untested for blob

def mount_storage(container, storage_account, mount_point, type="lake", client_config=None):
    if client_config is None:
        client_config = get_client_config()
    
    if type == "lake":
        protocol = "abfss"
    elif type == "blob":
        protocol = "wasbs"
    else: 
        raise Exception("Enter a valid storage type (blob or lake).")
        
    source = f"{protocol}://{container}@{storage_account}.dfs.core.windows.net/"
    
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_config["clientId"],
          "fs.azure.account.oauth2.client.secret": client_config["clientSecret"],
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/" + client_config["tenantId"] + "/oauth2/token"}

    if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
        dbutils.fs.mount(source = source,
            mount_point = mount_point,
            extra_configs = configs)

# COMMAND ----------

# Not implemented
# how long should the queue sas be active??
# Perhaps needs resource group as parameter

def get_queue_sas(storage_account, client_config=None):
    
    queue_sas = "BlobEndpoint=https://oatlystorage.blob.core.windows.net/;QueueEndpoint=https://oatlystorage.queue.core.windows.net/;FileEndpoint=https://oatlystorage.file.core.windows.net/;TableEndpoint=https://oatlystorage.table.core.windows.net/;SharedAccessSignature=sv=2020-08-04&ss=q&srt=sco&sp=rwdlacup&se=2023-03-08T03:40:26Z&st=2022-03-07T19:40:26Z&spr=https&sig=1iGDeVbChDfa%2BIwMcSmp0mRM04laIV2zIGWXYPWPg4o%3D"
    
    return queue_sas

# COMMAND ----------

# Not fully implemented
# Implement queuesas in here?
# resource group not really client config... used in cloudfiles, perhaps should be coded as global variable and used in DeltaStream implementation

def get_client_config():
    
    resourceGroup = "oatly"
    subscriptionId="6a624d1d-870b-401a-91b7-098d3f5bd304"
    tenantId="9f40a027-c490-4662-ac74-cf7f14839042"
    clientId="2d3df4ab-3482-419e-b3cd-09c4b188680c"
    clientSecret="Zen7Q~aWOstzEYvJcpKnVRP2Y6d4M6eFd38uP"

    return {
        "resourceGroup": resourceGroup,
        "subscriptionId": subscriptionId,
        "tenantId": tenantId,
        "clientId": clientId,
        "clientSecret": clientSecret,
    }

