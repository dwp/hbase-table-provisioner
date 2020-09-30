package app.helper.impl

import app.helper.S3HelperService
import org.springframework.stereotype.Service

@Service
class S3Helper() : S3HelperService {

        @Retryable(value = [S3Exception::class],
                maxAttempts = maxAttempts,
                backoff = Backoff(delay = initialBackoffMillis, multiplier = backoffMultiplier))
        @Throws(S3Exception::class)
        fun getS3ObjectInputStream(os: S3ObjectSummary, s3Client: AmazonS3, bucketName: String): S3ObjectInputStream {
            try {
                return s3Client.getObject(bucketName, os.key).objectContent
            }
            catch (ex: Exception) {
                throw S3Exception("Error retrieving object from S3")
            }
        }

        @Retryable(value = [S3Exception::class],
                maxAttempts = maxAttempts,
                backoff = Backoff(delay = initialBackoffMillis, multiplier = backoffMultiplier))
        @Throws(S3Exception::class)
        fun listObjectsV2Result(awsS3Client: AmazonS3, request: ListObjectsV2Request, objectSummaries: MutableList<S3ObjectSummary>): ListObjectsV2Result? {
            try {
                val results1 = awsS3Client.listObjectsV2(request)
                objectSummaries.addAll(results1.objectSummaries)
                return results1
            }
            catch (ex: Exception) {
                throw S3Exception("Error retrieving object summary from S3")
            }
        }

        companion object {
            val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(S3HelperService::class.toString())
            const val maxAttempts = 5
            const val initialBackoffMillis = 1000L
            const val backoffMultiplier = 2.0
        }
}
