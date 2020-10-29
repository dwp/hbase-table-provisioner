package app.helper

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result

interface S3Helper {
    fun getListOfS3ObjectsResult(awsS3Client: AmazonS3, request: ListObjectsV2Request): ListObjectsV2Result
}
