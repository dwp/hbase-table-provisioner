# hbase-table-provisioner

Hbase to Provisioner is a Kotlin application which calculates the number of Hbase regions a collection requires to be correctly split prior to a dataload, to prevent the Hbase splitting event occurring.

The application calculates the splitting by understanding the total bytes of all of the collections and the total bytes per collection; where a collections data is split across multiple data files.
With the information of the total bytes for all collections, the application divides total bytes by the maximum regions across the cluster (region servers * ideal region count per server); this creates a comparable unit known as region_unit.
For each collection, the total bytes of the collection is divided by the region unit to find the number of regions it should hold on the cluster. Larger size clusters receive a large number of regions compared to small regions.
The number of regions a collection requires is then used against a byte map to find the region start & stop positions in bytecode. This information is used on the Hbase API to create the table and create the required splitting prior to a data load.

## Outstanding Work
- Create integration tests
- Modify makefile & Docker compose to support integration tests
- Complete README for instructions on how to run the application
- Remove redundant filenameFormatDataExtensionPattern
- Update readme with application flow
- Add unit test in s3 reader to have multiple items in csv list to prove split et al

## Instructions for this repo

After cloning this repo, please run:  
`make bootstrap`

## Instructions for running

### Integration tests

### Running locally

## Notes about aws batch

In Batch that we use in AWS, the `COLLECTIONS_PREFIX_PATHS` are specified as a Command argument, not an environment variable.

This is so that it can be dynamic on each run and not fixed in the Terraform.
See `hbase-table-provisioner` in [docker-compose.yaml](docker-compose.yaml)

## Environment Variables

| Parameter name                                   | Sample Value               | Further info |
|--------------------------------------------------|----------------------------|------------- |
| CONTAINER_VERSION                                | sha:12345                  | - |
| ENVIRONMENT                                      | development                | - |
| APPLICATION                                      | h-t-p                      | - |
| COMPONENT                                        | jar-file                   | - |
| APP_VERSION                                      | v0.1.2                     | - |
| LOG_LEVEL                                        | INFO                       | - |
| HBASE_ZOOKEEPER_PARENT                           | /hbase                     | - |
| HBASE_ZOOKEEPER_PORT                             | 2181                       | - |
| HBASE_ZOOKEEPER_QUORUM                           | hbase                      | - |
| HBASE_RPC_READ_TIMEOUT_MILLISECONDS              | 1200                       | - |
| HBASE_CLIENT_TIMEOUT_MS                          | 1200                       | - |
| HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD_MILLISECONDS | 12000                      | - |
| HBASE_CLIENT_OPERATION_TIMEOUT_MILLISECONDS      | 1000                       | - |
| HBASE_PAUSE_MILLISECONDS                         | 50                         | - |
| HBASE_RETRIES                                    | 3                          | - |
| HBASE_COLUMN_FAMILY                              | cf                         | - |
| HBASE_COLUMN_QUALIFIER                           | record                     | - |
| HBASE_REGION_REPLICATION_COUNT                   | 3                          | Replication count per region created in Hbase |
| HBASE_REGION_TARGET_SIZE                         | 200                        | Number of regions per region server to aim for. |
| HBASE_REGION_SERVER_COUNT                        | 150                        | Number of region servers the cluster is using - this value should be input by Terraform |
| HBASE_COALESCE_COLLECTION_REGEX_PATTERN          | (?<database>[\w-]+)\.(?<collection>[\w-]+) | Regex pattern used to split collection-table name of S3 files into two groups for variable setting. |
| COLLECTIONS_INPUT_BUCKET                         | bucket-id                     | Ingest bucket name - this value should be input by Terraform. |
| COLLECTIONS_INPUT_BASE_PATH                      | /business/mongo               | Base path prefix where UC database export files are held. Note: This is not to include the adb, cdb etc prefixes. They should be handed in as collection paths. |
| COLLECTIONS_PREFIX_PATHS                         | adb/2020-06-23,cdb/2020-06-23 | Prefix for exported UC database files. This is the same prefix values used by HDI. |
| COLLECTIONS_FILENAME_FORMAT_REGEX                | ^[\w]+\-[\w]+\/[\w]+\/[\w]+\/\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01])\/[\w-]+\.[\w-]+\.[0-9]+\.json.gz.enc  | Regex pattern that matches the filenames of the data files within the aforementioned S3 location. |
| COLLECTIONS_NAME_REGEX_PATTERN                   | ([-\w]+\.[-.\w]+)\.[0-9]+\.json\.gz\.enc  | Regex pattern that matches the filenames of the data files within the aforementioned S3 location with groups. |
| S3_CLIENT_REGION                                 | eu-west-2                   | - |
| S3_MAX_ATTEMPTS                                  | 5                           | - |
| S3_INITIAL_BACKOFF_MILLIS                        | 1000                        | - |
| S3_BACKOFF_MULTIPLIER                            | 2                           | - |
| SPRING_PROFILES_ACTIVE                           | "LOCAL_S3" or "AWS_S3"      | - |
