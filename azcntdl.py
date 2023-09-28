from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential
import os

# Replace with your Azure Blob Storage credentials
storage_account_name = "rfcappstorage"
tenant_id = "b31c6b9d-6ab4-4c0a-9b86-94f76df3f85b"
client_id = "94ef8877-3389-4c51-ad69-e0cb52d9c965"
client_secret = "IN98Q~39SDJqNw5xbzL92RsZvo1qIdWfJD1nUc0R"
dl_path = "./dldump"

# Create a credential using your application's client id, client secret, and tenant id
credential = ClientSecretCredential(
    tenant_id=tenant_id,
    client_id=client_id,
    client_secret=client_secret
)

# Create a BlobServiceClient using the storage account name and the credential
bsc = BlobServiceClient(
    account_url=f"https://{storage_account_name}.blob.core.windows.net",
    credential=credential
)

os.makedirs(dl_path, exist_ok=True)

# List all containers in the storage account
containers = bsc.list_containers()

# Print the names of all containers
for container in containers:
    print(f"Processing Container:\t{container.name}")
    container_client = bsc.get_container_client(container.name)
    blob_list = container_client.list_blobs()
        
    for blob in blob_list:
        print(f"Downloading Blob:\t{blob.name}")
        blob_client = container_client.get_blob_client(blob.name)
        dl_file_path = os.path.join(dl_path, blob.name)
        os.makedirs(os.path.dirname(dl_file_path), exist_ok=True)
        with open(dl_file_path, 'wb') as dl_file:
            dl_file.write(blob_client.download_blob().readall())

        print(f"Downloaded Blob {blob.name} to {dl_file_path}")
    
print("All data downloaded.")        
