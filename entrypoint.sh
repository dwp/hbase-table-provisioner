#!/bin/sh

S3_PREFIXES="${1:-NOT_SET_IN_ENTRYPOINT}"

echo "Running jar using entrypoint S3_PREFIXES=${S3_PREFIXES}"

java -Dcollections.prefix_paths="${S3_PREFIXES}" -jar ./hbase-table-provisioner.jar
