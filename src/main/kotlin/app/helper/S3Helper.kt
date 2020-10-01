package app.helper

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.amazonaws.services.s3.model.S3ObjectSummary

interface S3Helper {
    fun listObjectsV2Result(awsS3Client: AmazonS3, request: ListObjectsV2Request, objectSummaries: MutableList<S3ObjectSummary>): ListObjectsV2Result?
}
