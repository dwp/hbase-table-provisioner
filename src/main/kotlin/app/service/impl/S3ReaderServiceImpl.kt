package app.service.impl

import app.service.S3ReaderService
import com.amazonaws.AmazonServiceException
import com.amazonaws.SdkClientException
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.io.IOException


@Service
class S3ReaderServiceImpl(
        val s3Client: AmazonS3Client,
        val bucket: String,
        val path: String
) : S3ReaderService {

    override fun getCollections(): List<String> {

        try {
            val request = ListObjectsV2Request().withBucketName(bucket).withPrefix(path).withMaxKeys(1000)
            var result: ListObjectsV2Result
            do {
                result = s3Client.listObjectsV2(request)
                for (objectSummary in result.objectSummaries) {
                    logger.info("Retrieved S3 objects", "key" to objectSummary.key, "size" to objectSummary.size.toString())
                }
                // If there are more than maxKeys keys in the bucket, get a continuation token
                // and list the next objects.
                val token = result.nextContinuationToken
                logger.debug("Paginated results, using continuation token to fetch more results", "token" to token.toString())
                request.continuationToken = token
            } while (result.isTruncated)
        } catch (e: AmazonServiceException) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace()
            logger.error("Amazon S3 failed to process the request", "error" to e.localizedMessage)
        } catch (e: SdkClientException) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace()
            logger.error("Amazon S3 couldn't be reached or the response wasn't parseable", "error" to e.localizedMessage)
        }


        return listOf()
    }

    companion object {
        val logger = DataworksLogger.getLogger(S3ReaderServiceImpl::class.toString())
    }
}
