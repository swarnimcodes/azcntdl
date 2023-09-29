# azcntdl
Download data from your Azure Storage accounts.

## Sample Config

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