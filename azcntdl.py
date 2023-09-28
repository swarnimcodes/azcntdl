import os
import yaml
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential
import datetime

# Load the configuration from the YAML file
with open("config.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)

# Custom Logging
def logger(loglevel: str, message: str) -> None:
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    logfile_path = "azcntdl.log"
    logmsg = f"{timestamp}  -  {loglevel}  -  {message}"
    with open(logfile_path, 'a') as logf:
        logf.write(f"{logmsg}\n")

def main():
    # Iterate through each configuration
    for account_config in config:
        storage_account_name = account_config["storage_account_name"]
        tenant_id = account_config["tenant_id"]
        client_id = account_config["client_id"]
        client_secret = account_config["client_secret"]
        dl_path = account_config["dl_path"]

        # Create a credential using the current account's client id, client secret, and tenant id
        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )

        # Create a BlobServiceClient using the current storage account's name and the credential
        bsc = BlobServiceClient(
            account_url=f"https://{storage_account_name}.blob.core.windows.net",
            credential=credential
        )

        os.makedirs(dl_path, exist_ok=True)

        # List all containers in the storage account
        containers = bsc.list_containers()

        # Print the names of all containers for the current storage account
        for container in containers:
            print(f"Processing Container ({storage_account_name}):\t{container.name}")
            logger(
                loglevel = "INFO",
                message = f"Processing Container ({storage_account_name}):\t{container.name}"
            )
            container_client = bsc.get_container_client(container.name)
            blob_list = container_client.list_blobs()

            for blob in blob_list:
                print(f"Downloading Blob ({storage_account_name}):\t{blob.name}")
                logger(
                    loglevel = "INFO",
                    message = f"Downloading Blob ({storage_account_name}):\t{blob.name}"
                )
                blob_client = container_client.get_blob_client(blob.name)
                dl_file_path = os.path.join(dl_path, blob.name)
                os.makedirs(os.path.dirname(dl_file_path), exist_ok=True)
                with open(dl_file_path, 'wb') as dl_file:
                    dl_file.write(blob_client.download_blob().readall())

                print(f"Downloaded Blob {blob.name} to {dl_file_path}")
                logger(
                    loglevel = "INFO",
                    message = f"Downloaded Blob {blob.name} to {dl_file_path}"
                )

    print("All data downloaded.")
    logger(
        loglevel = "INFO",
        message = "All data downloaded!"
    )


if __name__ == "__main__":
    main()

# TODO: resume partial downloads
# TODO: log file functionality
# TODO: mailing functionality