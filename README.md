# azcntdl
Download data from your Azure Storage accounts.

## Features
- Downloading all containers in your storage account.
- Skipping already downloaded files/blobs
- Resume partially downloaded downloads seamlessly
- Mailing functionality using SendGrid in case of errors

## Sample Config
Place the config file in the same directory from where you are executing the program.
Or else, change the code to suit your needs.

```yaml
# CONFIG FILE

## Global variables
global:
  logfile_directory: <your-logfile-storage-directory-location>
  sendgrid_api_key: <your-api-key>
  to_mail:
    - email1@gmail.com
    - another_email_id@example.com

## Storage account configurations
storage_accounts:
  - storage_account_name: <your-storage-account>
    tenant_id: <tenant-id-of-the-storage-account>
    client_id: <client-id-of-the-storage-account>
    client_secret: <client-secret-of-the-storage-account>
    dl_path: <directory-to-store-containers>
  
  # - storage_account_name: another_storage_account
  #   tenant_id: tenant_id
  #   client_id: client_id
  #   client_secret: client_secret
  #   dl_path: dl path
```