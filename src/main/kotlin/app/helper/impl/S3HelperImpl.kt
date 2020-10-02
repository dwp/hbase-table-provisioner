package app.helper.impl

import app.exception.S3Exception
import app.helper.S3Helper
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Component

@Component
class S3HelperImpl(private val maxAttempts: Int,
                   private val initialBackoffMillis: Long,
                   private val backoffMultiplier: Double) : S3Helper {

//    @Retryable(value = [S3Exception::class],
//            maxAttempts = maxAttempts,
//            backoff = Backoff(delay = initialBackoffMillis, multiplier = backoffMultiplier))
    @Throws(S3Exception::class)
    override fun getListOfS3ObjectsResult(awsS3Client: AmazonS3, request: ListObjectsV2Request): ListObjectsV2Result? {
        try {
            return awsS3Client.listObjectsV2(request)
        } catch (ex: Exception) {
            throw S3Exception("Error retrieving object summary from S3")
        }
    }
}
