package app.service.impl

import app.helper.impl.S3HelperImpl
import app.service.S3ReaderService
import app.util.CoalescingUtil
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.amazonaws.services.s3.model.S3ObjectSummary
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Service
class S3ReaderServiceImpl(val s3Client: AmazonS3,
                          val s3Helper: S3HelperImpl,
                          val bucket: String,
                          val basePath: String,
                          val sourceDatabasePaths: List<String>,
                          val filenameFormatRegexPattern: String,
                          val filenameFormatDataExtensionPattern: String,
                          val collectionNameRegexPattern: String) : S3ReaderService {

    override fun getCollectionSummaries(): MutableMap<String, Long> {

        val collectionDetailsMap = mutableMapOf<String, Long>()

        sourceDatabasePaths.forEach { sourceDatabasePath ->
            logger.info("Getting collection details for source path", "source_path" to sourceDatabasePath)
            collectionDetailsMap.putAll(getCollectionNamesAndSizesInPath(sourceDatabasePath))
        }

        logger.info("Retrieved collections from S3", "number_of_collections" to collectionDetailsMap.size.toString())

        return collectionDetailsMap
    }

    private fun getCollectionNamesAndSizesInPath(sourceDatabasePath: String): MutableMap<String, Long> {
        try {
            val fullBasePath = "$basePath/$sourceDatabasePath"
            val request = ListObjectsV2Request().withBucketName(bucket).withPrefix(fullBasePath).withMaxKeys(1000)
            var results: ListObjectsV2Result?
            val objectSummaries: MutableList<S3ObjectSummary> = mutableListOf()

            do {
                logger.info("Getting list of S3 objects for cluster", "bucket" to bucket, "s3_prefix" to fullBasePath)

                results = s3Helper.getListOfS3ObjectsResult(s3Client, request)
                objectSummaries.addAll(results!!.objectSummaries)
                request.continuationToken = results.nextContinuationToken

                logger.info("Got list of S3 objects for cluster",
                        "bucket" to bucket,
                        "s3_prefix" to fullBasePath,
                        "results_size" to results.objectSummaries?.size.toString())

            } while (results != null && results.isTruncated)

            val filteredObjects = filterResultsForDataFilesOnly(objectSummaries, filenameFormatRegexPattern)

            return deduplicateAndCarryOverCollectionPropertiesAsOne(filteredObjects)
        } catch (e: Exception) {
            logger.error("Error getting collection from S3", e)
            return mutableMapOf()
        }
    }

    private fun filterResultsForDataFilesOnly(s3Results: MutableList<S3ObjectSummary>, filenameFormatRegexPattern: String): List<S3ObjectSummary> {

        logger.info("Filtering over collection results from S3")

        val matchedConditionObjects = mutableListOf<S3ObjectSummary>()

        s3Results.forEach { collectionObject ->
            if (filenameFormatRegexPattern.toRegex().containsMatchIn(collectionObject.key)) {
                matchedConditionObjects.add(collectionObject)
            }
        }

        return matchedConditionObjects
    }

    private fun deduplicateAndCarryOverCollectionPropertiesAsOne(filteredObjects: List<S3ObjectSummary>): MutableMap<String, Long> {

        logger.info("Removing duplicate collection results", "collection_size" to filteredObjects.size.toString())

        val topicByteSizeMap = mutableMapOf<String, Long>()

        filteredObjects.forEach { collection ->
            val key = collection.key

            Regex(collectionNameRegexPattern).find(key)?.let {

                val (topicName) = it.destructured
                val coalesced = CoalescingUtil().coalescedCollection(topicName)

                if (coalesced in topicByteSizeMap) {
                    topicByteSizeMap[coalesced] = topicByteSizeMap[coalesced]!! + collection.size
                } else {
                    topicByteSizeMap[coalesced] = collection.size
                }
            }
        }

        logger.info("Removed duplicates and calculated byte size", "deduplicated_collection_size" to topicByteSizeMap.size.toString())

        return topicByteSizeMap
    }

    companion object {
       val logger = DataworksLogger.getLogger(S3ReaderServiceImpl::class.toString())
    }
}
