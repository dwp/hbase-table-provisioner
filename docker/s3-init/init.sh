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
else
    stderr Not making bucket \'"$S3_BUCKET"\': already exists.
fi


stderr creating sample data

cd /test-data
for file in */*/*; do
  echo ${file}
  aws_s3 cp "${file}" "s3://${S3_BUCKET}/${S3_PREFIX}/${file}"
done

aws_s3 ls s3://${S3_BUCKET} --recursive

stderr making corporate storage input
aws_s3 mb s3://corporate-storage-input

declare -A topic_sizes=(["hyphenated-database.collection1"]=1 ["database.collection2"]=5 ["database.collection3"]=10)

for year in 2020 2021; do
  for month in $(seq 1 3); do
    month=$(printf "%02d" $month)
    for day in $(seq 9 14); do
      day=$(printf "%02d" $day)
      for topic in "${!topic_sizes[@]}"; do
        database=${topic%.*}
        database=${database/-/_}
        collection=${topic#*.}
        size=${topic_sizes[$topic]}
        for file in $(seq 3); do
          target=s3://corporate-storage-input/corporate_storage/ucfs_main/$year/$month/$day/$database/$collection/db.${topic}_${file}_1000_1010.jsonl.gz
          temp_file=$(mktemp)
          dd if=/dev/zero of=$temp_file count=$size bs=1K
          aws_s3 cp $temp_file $target &
        done
        wait
      done
    done
  done
done
