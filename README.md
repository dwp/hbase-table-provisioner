# hbase-table-provisioner

## hbase_table_provisioner

This repo contains Makefile to fit the standard pattern.
This repo is a base to create new non-Terraform repos, adding the githooks submodule, making the repo ready for use.

After cloning this repo, please run:  
`make bootstrap`

## Environment Variables

| Parameter name                | Sample Value               | Further info
|-------------------------------|----------------------------|--------------
| HBASE_ZOOKEEPER_PARENT            | /hbase                            | 
| HBASE_ZOOKEEPER_PORT              | 2181                              | 
| HBASE_ZOOKEEPER_QUORUM            | hbase                             | 
| HBASE_RPC_TIMEOUT_MILLISECONDS    | 1200                             |
| HBASE_CLIENT_TIMEOUT_MS           | 1200                              |
| HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD_MS | 12000                    | 
| HBASE_OPERATION_TIMEOUT_MILLISECONDS | 1000                       |
| HBASE_PAUSE_MILLISECONDS          | 50                                |
| HBASE_RETRIES                     | 3                                 |
| HBASE_COLUMN_FAMILY               | cf                                |
| HBASE_COLUMN_QUALIFIER            | record                            |
| HBASE_REGION_REPLICATION_COUNT    | 3                                | Replication count per region created in Hbase
| HBASE_REGION_TARGET_SIZE          | 200                               | Number of regions per region server to aim for.
| HBASE_REGION_SERVER_COUNT         | 150                               | Number of region servers the cluster is using - this value should be input by Terraform
| HBASE_COALESCE_COLLECTION_REGEX_PATTERN | (?<database>[\w-]+)\.(?<collection>[\w-]+) | Regex pattern used to split collection-table name of S3 files into two groups for variable setting.
| S3_BUCKET                         | s3://bucket                       | Ingest bucket name - this value should be input by Terraform
| S3_BASE_PATH                      | /business/mongo                   | Base path prefix where UC database export files are held. Note: This is not to include the adb, cdb etc prefixes. They should be handed in as collection paths.
| S3_COLLECTION_PATHS               | adb/2020-06-23,cdb/2020-06-23     | Prefix for exported UC database files. This is the same prefix values used by HDI.
| S3_FILENAME_FORMAT_REGEX          | [\\w-]+\\.[\\w-]+\\.[0-9]+\\.json\\.gz\\.enc  | Regex pattern that matches the filenames of the data files within the aforementioned S3 location.
| S3_COLLECTION_NAME_REGEX_PATTERN  | ([-\w]+\.[-.\w]+)\.[0-9]+\.json\.gz\.enc  | Regex pattern that matches the filenames of the data files within the aforementioned S3 location with groups.
