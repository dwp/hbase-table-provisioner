package app.helper.impl

import app.exception.S3Exception
import app.helper.S3Helper
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Component
class S3HelperImpl(private val maxAttempts: Int,
                   private val initialBackoffMillis: Long,
                   private val backoffMultiplier: Long) : S3Helper {

    @Throws(S3Exception::class)
    override fun getListOfS3ObjectsResult(awsS3Client: AmazonS3, request: ListObjectsV2Request): ListObjectsV2Result {

        var result = ListObjectsV2Result()

        var success = false
        var attempts = 0
        var exception: Exception? = null

        while (!success && attempts < maxAttempts) {
            try {
                result = awsS3Client.listObjectsV2(request)
                success = true
            } catch (e: Exception) {
                val delayMillis = if (attempts == 0) initialBackoffMillis
                else (initialBackoffMillis * attempts * backoffMultiplier)

                logger.warn("Failed to get s3 object result: ${e.message}", "attempt_number" to "${attempts + 1}",
                        "max_attempts" to "$maxAttempts", "retry_delay" to "$delayMillis", "error_message" to "${e.message}")

                Thread.sleep(delayMillis)

                exception = e
            } finally {
                attempts++
            }
        }

        if (!success) {
            if (exception != null) {
                throw S3Exception("Error retrieving object summary from S3", exception)
            }
        }

        return result
    }

    companion object {
        val logger = DataworksLogger.getLogger(S3HelperImpl::class.toString())
    }
}
