import os
import yaml
import datetime
import zipfile


from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential
from sendgrid import SendGridAPIClient, Mail


# Load the configuration from the YAML file
with open("config.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)

# Parsing Config Variables
global_config      =  config.get("global", {})
logfile_directory  =  config['global']['logfile_directory']
storage_accounts   =  config['storage_accounts']
to_mail            =  config['global']['to_mail']
sg_api             =  config['global']['sendgrid_api_key'] 

email_conditions   =  []

# Temporary Log File Path
logfile_path      = "./azcntdl.log"

# Custom Logging
def logger(loglevel: str, message: str) -> None:
    timestamp     = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    logmsg        = f"{timestamp}  -  {loglevel}  -  {message}"
    
    with open(logfile_path, 'a') as logf:
        logf.write(f"{logmsg}\n")
    
    return None


# Zip up and store away the config file
def compressLog() -> None:
    zipts             = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")    
    ziplogfile_name   = f"azure_datadump_log_{zipts}.zip" 
    zipfile_path      = os.path.join(logfile_directory, ziplogfile_name)

    with zipfile.ZipFile(zipfile_path, 'w', zipfile.ZIP_LZMA) as zipf:
        zipf.write(logfile_path, os.path.basename(logfile_path))

    os.remove(logfile_path)
    
    return None

def sendMail() -> None:
    from_mail = "noreply@mastersofterp.co.in"
    ts_mail = datetime.datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    subject = f"Azure Download Dump Notification {ts_mail}"
    sg = SendGridAPIClient(api_key=sg_api)
    email_content = "\n".join(f"{condition}" for condition in email_conditions)

    message = Mail(
        from_email           =   from_mail,
        to_emails            =   to_mail,
        subject              =   subject,
        plain_text_content   =   email_content
    )
    
    try:
        response = sg.send(message)
        if response.status_code == 202:
            print(f"Email successfully sent to: {(mail for mail in to_mail)}")
    except Exception as e:
        print(f"{str(e)}")
    return None


# Should the file be downloaded/resumed/redownloaded?
def downloadBlob(blob_client, dl_file_path) -> None:
    blob_props = blob_client.get_blob_properties()
    blob_size  = blob_props['size']
    blob_name  = blob_props['name']
    if os.path.exists(dl_file_path):
        local_size = os.path.getsize(dl_file_path)

        if local_size < blob_size:
            # Complete partial downloads
            logger(
                loglevel = "INFO",
                message  = f"Partial Download Detected. Resumed download of Blob: {blob_name} to {dl_file_path}"
            )
            with open(dl_file_path, 'ab') as localfile:
                blob_client.download_blob(offset=local_size).readinto(localfile)               
            logger(
                loglevel = "INFO",
                message  = f"Completely downloaded the partial downloaded file {blob_name}"
            )
        elif local_size == blob_size:
            # Skip completed downloads
            logger(
                loglevel  = "INFO",
                message   = f"Skipping download for {blob_name}. Local file size matches blob size.\n\n"
            )

        else:
            # Delete unexpected download and redownload
            os.remove(dl_file_path)
            with open(dl_file_path, 'wb') as localfile:
                blob_client.download_blob().readinto(localfile)
            logger(
                loglevel = "INFO",
                message  = f"Downloaded Blob: {blob_name} to {dl_file_path}\n\n"
            )
                
    else:
        # Download as usual
        with open(dl_file_path, 'wb') as localfile:
            blob_client.download_blob().readinto(localfile)
        logger(
                loglevel = "INFO",
                message  = f"Downloaded Blob: {blob_name} to {dl_file_path}\n\n"
            )

    return None


def main() -> None:
    logger(
        loglevel = "INFO",
        message  = "PROGRAM HAS STARTED EXECUTING\n\n"
    )
    # Iterate through each configuration
    for account_config in storage_accounts:
        storage_account_name  =  account_config["storage_account_name"]
        tenant_id             =  account_config["tenant_id"]
        client_id             =  account_config["client_id"]
        client_secret         =  account_config["client_secret"]
        dl_path               =  account_config["dl_path"]

        # Create a credential using the current account's client id, client secret, and tenant id
        credential = ClientSecretCredential(
            tenant_id     =  tenant_id,
            client_id     =  client_id,
            client_secret =  client_secret
        )

        # Create a BlobServiceClient using the current storage account's name and the credential
        bsc = BlobServiceClient(
            account_url =  f"https://{storage_account_name}.blob.core.windows.net",
            credential  =  credential
        )

        os.makedirs(dl_path, exist_ok=True)

        # List all containers in the storage account
        containers = bsc.list_containers()

        # Print the names of all containers for the current storage account
        for container in containers:
            print(f"Processing Container ({storage_account_name}):\t{container.name}")
            logger(
                loglevel = "INFO",
                message  = f"Processing Container ({storage_account_name}):\t{container.name}"
            )
            container_client = bsc.get_container_client(container.name)
            blob_list = container_client.list_blobs()

            for blob in blob_list:
                print(f"Evaluating Blob ({storage_account_name}):\t{blob.name}")
                logger(
                    loglevel  =  "INFO",
                    message   =  f"Evaluating Blob ({storage_account_name}):\t{blob.name}"
                )
                blob_client   =  container_client.get_blob_client(blob.name)
                dl_file_path  =  os.path.join(dl_path, blob.name)
                os.makedirs(os.path.dirname(dl_file_path), exist_ok=True)
                
                # Send details to another function
                # That function will figure out what to do
                try:
                    downloadBlob(blob_client, dl_file_path)
                except Exception as e:
                    blob_name = os.path.basename(dl_file_path)
                    logger(
                        loglevel   =   "ERROR",
                        message    =   f"Exception: {str(e)}"
                    )
                    email_conditions.append(
                        f"Failed to download {blob_name}"
                        + f"Exception: {str(e)}"
                    )

    print("All data downloaded. Program has finished executing.")
    logger(
        loglevel = "INFO",
        message  = "PROGRAM HAS FINISHED EXECUTING."
    )

    return None


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger(
            loglevel = "ERROR",
            message  = f"Error ocurred while executing program\nException:\t{e}"
        )
        email_conditions.append(
            "Error ocurred while executing Program"
            + f"Exception: {str(e)}"
        )
    finally:
        try:
            compressLog()
        except Exception as e:
            email_conditions.append(
                f"Failed to save/compress log: Exception{str(e)}"
            )
        if email_conditions:
            sendMail()


# TODO: check for invalid filenames
# DONE: zip log file and save it somewhere else
# DONE: resume partial downloads -- done
# DONE: mailing functionality
# DONE: log file functionality
