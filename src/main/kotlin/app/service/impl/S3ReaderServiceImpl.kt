package app.service.impl

import app.domain.CollectionSummary
import app.service.S3ReaderService
import app.util.getCollectionFromPath
import com.amazonaws.AmazonServiceException
import com.amazonaws.SdkClientException
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.amazonaws.services.s3.model.S3ObjectSummary
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Service
class S3ReaderServiceImpl(val s3Client: AmazonS3, val bucket: String, val path: String) : S3ReaderService {

    override fun getCollectionSummaries(): List<CollectionSummary> {

        val directoryOfCollectionFolders = getListOfObjectsInPath(path)

        if (directoryOfCollectionFolders == null || directoryOfCollectionFolders.isEmpty()) {
            logger.error("No collection summaries to be viewed")
            return listOf()
        }

        val collectionSummaryList = mutableListOf<CollectionSummary>()

        directoryOfCollectionFolders.forEach {
            val collectionList = getListOfObjectsInPath(it.key)

            val collectionSummary = getCollectionSummary(it.key, collectionList!!)

            logger.info("Got a collection summary",
                    "key" to collectionSummary.key,
                    "size" to collectionSummary.size.toString())

            collectionSummaryList.add(collectionSummary)
        }

        return collectionSummaryList
    }

    private fun getListOfObjectsInPath(objectPath: String): MutableList<S3ObjectSummary>? {
        try {
            val request = ListObjectsV2Request().withBucketName(bucket).withPrefix(objectPath).withMaxKeys(1000)
            var result: ListObjectsV2Result

            do {
                result = s3Client.listObjectsV2(request)
                for (objectSummary in result.objectSummaries) {
                    logger.info("Retrieved S3 objects",
                            "key" to objectSummary.key,
                            "size" to objectSummary.size.toString())
                }
                // If there are more than maxKeys keys in the bucket, get a continuation token
                // and list the next objects.
                val token = result.nextContinuationToken
                logger.debug("Paginated results, using continuation token to fetch more results",
                        "token" to token.toString())
                request.continuationToken = token

            } while (result.isTruncated)

            return result.objectSummaries

        } catch (e: AmazonServiceException) {
            logger.error("Amazon S3 failed to process the request", "error" to e.localizedMessage)
        } catch (e: SdkClientException) {
            logger.error("Amazon S3 couldn't be reached or the response wasn't able to be parsed", "error" to e.localizedMessage)
        }

        return mutableListOf()
    }

    private fun getCollectionSummary(key: String, collectionSummaries: MutableList<S3ObjectSummary>) = CollectionSummary(getCollectionFromPath(key), collectionSummaries.sumBy { it.size.toInt() })

    companion object {
        val logger = DataworksLogger.getLogger(S3ReaderServiceImpl::class.toString())
    }
}
