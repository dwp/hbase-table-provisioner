#! /bin/bash

source ./environment.sh

aws configure set aws_access_key_id "${AWS_ACCESS_KEY_ID}"
aws configure set aws_secret_access_key "${AWS_SECRET_ACCESS_KEY}"
aws configure set default.region "${AWS_REGION}"
aws configure set region "${AWS_REGION}"
aws configure list

declare -i BUCKET_COUNT=$(aws_s3 ls | grep "${S3_BUCKET}" | wc -l)

if [[ $BUCKET_COUNT -eq 0 ]]; then
    aws_s3 mb "s3://${S3_BUCKET}"
    aws_s3api put-bucket-acl --bucket "${S3_BUCKET}" --acl public-read
    aws_s3 mb "s3://${S3_MANIFEST_BUCKET}"
    aws_s3api put-bucket-acl --bucket "${S3_MANIFEST_BUCKET}" --acl public-read
    aws_s3 mb "s3://dw-local-crl" --region "${AWS_REGION}"
    aws_s3api put-bucket-acl --bucket "dw-local-crl" --acl public-read
    aws_s3api put-object --bucket "dw-local-crl" --key crl/
else
    stderr Not making bucket \'"$S3_BUCKET"\': already exists.
fi


stderr creating sample data

set -e
cd /test-data
for file in */*/*; do
  echo ${file}
  aws_s3 cp "${file}" "s3://${S3_BUCKET}/${file}"
done

aws_s3 ls s3://${S3_BUCKET} --recursive
set +e
