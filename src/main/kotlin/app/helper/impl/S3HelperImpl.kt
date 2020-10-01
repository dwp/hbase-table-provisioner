package app.helper.impl

import app.exception.S3Exception
import app.helper.S3Helper
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.amazonaws.services.s3.model.S3ObjectSummary
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Component

@Component
class S3HelperImpl : S3Helper {

    @Retryable(value = [S3Exception::class],
            maxAttempts = maxAttempts,
            backoff = Backoff(delay = initialBackoffMillis, multiplier = backoffMultiplier))
    @Throws(S3Exception::class)
    override fun listObjectsV2Result(awsS3Client: AmazonS3, request: ListObjectsV2Request, objectSummaries: MutableList<S3ObjectSummary>): ListObjectsV2Result? {
        try {
            val result = awsS3Client.listObjectsV2(request)
            objectSummaries.addAll(result.objectSummaries)
            return result
        } catch (ex: Exception) {
            throw S3Exception("Error retrieving object summary from S3")
        }
    }

    companion object {
        const val maxAttempts = 5
        const val initialBackoffMillis = 1000L
        const val backoffMultiplier = 2.0
    }
}
