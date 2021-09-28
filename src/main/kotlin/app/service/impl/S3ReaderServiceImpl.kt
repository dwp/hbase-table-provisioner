package app.service.impl

import app.helper.S3Helper
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
                          val s3Helper: S3Helper,
                          val inputBucket: String,
                          val inputBasePath: String,
                          val prefixPaths: String,
                          val filenameFormatRegexPattern: String,
                          val nameRegexPattern: String) : S3ReaderService {

    override fun collectionSizes(): Map<String, Long> =
        objectSummaries().asSequence()
            .map {  Pair(it.key, it.size) }
            .mapNotNull { (key, size) ->
                filenameRegex.find(key)?.let {
                    val (database, collection) = it.destructured
                    Pair("${database.replace('-', '_')}:${collection.replace(Regex("[-.]"), "_")}", size)
                }
            }.groupingBy { (topic, _) -> topic }
            .fold(0L) { i, (_, size) -> i + size }


    private final tailrec fun objectSummaries(accumulated: List<S3ObjectSummary> = listOf(), continuationToken: String? = null): List<S3ObjectSummary> {
        val response = s3Client.listObjectsV2(ListObjectsV2Request().apply {
            withBucketName(inputBucket)
            withPrefix(inputBasePath)
            withContinuationToken(continuationToken)
        })

        return if (!response.isTruncated) {
            accumulated + response.objectSummaries
        } else {
            objectSummaries(accumulated + response.objectSummaries, response.nextContinuationToken)
        }
    }

    override fun getCollectionSummaries(): MutableMap<String, Long> {

        val collectionDetailsMap = mutableMapOf<String, Long>()
        logger.info("Prefix paths", "prefix_paths" to prefixPaths)
        if (prefixPaths.isBlank() || prefixPaths.startsWith("NOT_SET")) {
            logger.error("Prefix paths must be specified but was not", "prefix_paths" to prefixPaths)
            throw IllegalArgumentException("Prefix paths must be specified but was '$prefixPaths'")
        }

        prefixPaths.split(",").forEach { prefixPath ->
            logger.info("Getting collection details for source path", "prefix_path" to prefixPath)
            getCollectionNamesAndSizesInPath(prefixPath).forEach { (key, size) ->
                if (collectionDetailsMap.containsKey(key)) {
                    collectionDetailsMap[key]?.let {
                        collectionDetailsMap.put(key, it + size)
                    }
                }
                else {
                    collectionDetailsMap.put(key, size)
                }
            }
        }

        logger.info("Retrieved collections from S3", "number_of_collections" to collectionDetailsMap.size.toString())
        if (collectionDetailsMap.isEmpty()){
            logger.error("No collections found to process in S3", "number_of_collections" to collectionDetailsMap.size.toString())
            throw IllegalArgumentException("No collections found to process in S3")
        }

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

            }
            while (results != null && results.isTruncated)

            val filteredObjects = filterResultsForDataFilesOnly(objectSummaries)

            return deduplicateAndCarryOverCollectionPropertiesAsOne(filteredObjects, sourceDatabasePath)
        }
        catch (e: Exception) {
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

    private fun deduplicateAndCarryOverCollectionPropertiesAsOne(filteredObjects: List<S3ObjectSummary>, sourceDatabasePath: String): MutableMap<String, Long> {
        logger.info("Removing duplicates and calculating byte size",
            "collection_size" to filteredObjects.size.toString(),
            "name_regex_pattern" to nameRegexPattern
        )

        val topicByteSizeMap = mutableMapOf<String, Long>()
        val topicS3ToCoalescedNames = mutableMapOf<String, String>()

        filteredObjects.forEach { collection ->
            val key = collection.key

            logger.info("De-duping collection object",
                "collection_object_key" to key
            )

            Regex(nameRegexPattern).find(key)?.let {

                val (topicName) = it.destructured
                val coalesced = CoalescingUtil().coalescedCollection(topicName)
                topicS3ToCoalescedNames[topicName] = coalesced

                if (coalesced in topicByteSizeMap) {
                    topicByteSizeMap[coalesced] = topicByteSizeMap[coalesced]!! + collection.size
                }
                else {
                    topicByteSizeMap[coalesced] = collection.size
                }
            }
        }

        // single collection names log for HDl and HDI to use - do not remove this plz thx
        logger.info(
            "Removed duplicates and calculated byte size",
            "prefix_path" to sourceDatabasePath,
            "deduped_collection_size" to topicByteSizeMap.size.toString(),
            "s3_topic_names" to topicS3ToCoalescedNames.keys.sorted().toString(),
            "coalesced_topic_names" to topicS3ToCoalescedNames.values.sorted().toString(),
        )

        return topicByteSizeMap
    }

    val filenameRegex by lazy {  Regex(filenameFormatRegexPattern) }

    companion object {
        val logger = DataworksLogger.getLogger(S3ReaderServiceImpl::class.toString())

    }
}
