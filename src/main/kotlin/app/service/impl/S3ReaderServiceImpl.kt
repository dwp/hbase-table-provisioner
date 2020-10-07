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
                          val inputBucket: String,
                          val inputBasePath: String,
                          val prefixPaths: String,
                          val filenameFormatRegexPattern: String,
                          val filenameFormatDataExtensionPattern: String,
                          val nameRegexPattern: String) : S3ReaderService {

    override fun getCollectionSummaries(): MutableMap<String, Long> {

        val collectionDetailsMap = mutableMapOf<String, Long>()

        logger.info("PREFIX PATHS", "prefix_paths" to prefixPaths)

        prefixPaths.split(",").forEach { prefixPath ->
            logger.info("Getting collection details for source path", "prefix_path" to prefixPath)
            collectionDetailsMap.putAll(getCollectionNamesAndSizesInPath(prefixPath))
        }

        logger.info("Retrieved collections from S3", "number_of_collections" to collectionDetailsMap.size.toString())

        return collectionDetailsMap
    }

    private fun getCollectionNamesAndSizesInPath(sourceDatabasePath: String): MutableMap<String, Long> {
        try {
            val fullBasePath = "$inputBasePath/$sourceDatabasePath"
            val request = ListObjectsV2Request().withBucketName(inputBucket).withPrefix(fullBasePath).withMaxKeys(1000)
            var results: ListObjectsV2Result?
            val objectSummaries: MutableList<S3ObjectSummary> = mutableListOf()

            do {
                logger.info("Getting list of S3 objects for cluster", "bucket" to inputBucket, "s3_prefix" to fullBasePath)

                results = s3Helper.getListOfS3ObjectsResult(s3Client, request)
                objectSummaries.addAll(results.objectSummaries)
                request.continuationToken = results.nextContinuationToken

                logger.info("Got list of S3 objects for cluster",
                        "bucket" to inputBucket,
                        "s3_prefix" to fullBasePath,
                        "results_size" to results.objectSummaries?.size.toString())

            } while (results != null && results.isTruncated)

            val filteredObjects = filterResultsForDataFilesOnly(objectSummaries)

            return deduplicateAndCarryOverCollectionPropertiesAsOne(filteredObjects)
        } catch (e: Exception) {
            logger.error("Error getting collection from S3", e)
            return mutableMapOf()
        }
    }

    private fun filterResultsForDataFilesOnly(s3Results: MutableList<S3ObjectSummary>): List<S3ObjectSummary> {

        logger.info("Filtering over collection results from S3",
                "result_size" to s3Results.size.toString(),
                "filtering_regex" to filenameFormatRegexPattern
        )

        val matchedConditionObjects = mutableListOf<S3ObjectSummary>()

        s3Results.forEach { collectionObject ->

            logger.debug("Filtering for a collection object",
                    "collection_object_key" to collectionObject.key
            )

            if (filenameFormatRegexPattern.toRegex().containsMatchIn(collectionObject.key)) {
                matchedConditionObjects.add(collectionObject)
            }
        }

        logger.info("Matched results via filtering",
                "matchedConditionObjects" to matchedConditionObjects.size.toString(),
                "filtering_regex" to filenameFormatRegexPattern
        )

        return matchedConditionObjects
    }

    private fun deduplicateAndCarryOverCollectionPropertiesAsOne(filteredObjects: List<S3ObjectSummary>): MutableMap<String, Long> {

        logger.info("Scrubbing S3 object key to reveal collection and topic name",
                "collection_size" to filteredObjects.size.toString(),
                "name_regex_pattern" to nameRegexPattern
        )

        val topicByteSizeMap = mutableMapOf<String, Long>()

        filteredObjects.forEach { collection ->
            val key = collection.key

            logger.info("De-duping collection object",
                    "collection_object_key" to key
            )

            Regex(nameRegexPattern).find(key)?.let {

                val (topicName) = it.destructured
                val coalesced = CoalescingUtil().coalescedCollection(topicName)

                logger.info("Topic name found by regex for collection scrubbing",
                "topic_name" to coalesced)

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
