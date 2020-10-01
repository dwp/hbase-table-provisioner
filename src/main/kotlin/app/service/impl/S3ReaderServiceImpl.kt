package app.service.impl

import app.domain.CollectionSummary
import app.helper.S3HelperService
import app.service.S3ReaderService
import app.util.getCollectionFromPath
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.amazonaws.services.s3.model.S3ObjectSummary
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Service
class S3ReaderServiceImpl(val s3Client: AmazonS3,
                          val bucket: String,
                          val basePath: String,
                          val s3Helper: S3Helper,
                          val sourceDatabasePaths: List<String>,
                          val filenameFormatRegexPattern: String,
                          val filenameFormatDataExtensionPattern: String,
                          val collectionNameRegexPattern: String) : S3ReaderService {

    override fun getCollectionSummaries(): MutableMap<String, Long> {
//        s3://bucket/basepath/sourceDbPath/DateIncluded/collection.tables.file
// Fn returns list of objects; each object is an instance of collection, containing collection name and size properties
        val collectionDetailsMap = mutableMapOf<String, Long>()

        sourceDatabasePaths.forEach() { sourceDatabasePath ->
            collectionDetailsMap.putAll(getCollectionNamesAndSizesInPath(bucket, basePath, sourceDatabasePath))

        }

        return collectionDetailsMap
    }

    private fun getCollectionNamesAndSizesInPath(fullS3BasePath: String, sourceDatabasePath: String): MutableMap<String, Long> {
        // Create objects for each collection
        //  s3://bucket/basepath/sourceDbPath/DateIncluded/?
        try {
            val request = ListObjectsV2Request().withBucketName(bucket).withPrefix(sourceDatabasePath).withMaxKeys(1000)
            var results: ListObjectsV2Result?
            val objectSummaries: MutableList<S3ObjectSummary> = mutableListOf()

            do {
                results = s3Helper.listObjectsV2Result(s3Client, request, objectSummaries)
                request.continuationToken = results?.nextContinuationToken

            } while (results != null && results.isTruncated)

            val filteredObjects = filterResultsForDataFilesOnly(objectSummaries, filenameFormatRegexPattern, filenameFormatDataExtensionPattern)

            return deduplicateAndCarryOverCollectionPropertiesAsOne(filteredObjects)

        }

    }

    private fun filterResultsForDataFilesOnly(s3Results: MutableList<S3ObjectSummary>, filenameFormatRegexPattern: String, filenameFormatDataExtensionPattern: String): List {
//        Filters object summary from S3 for unique collection names and information

        val matchedConditionObjects = listOf()
        val fullFilenameFormatRegex = "$filenameFormatRegexPattern$filenameFormatDataExtensionPattern"

        s3Results.forEach() { collectionObject ->
            if (fullFilenameFormatRegex.toRegex().containsMatchIn(collectionObject.key)) {
                matchedConditionObjects.add(collectionObject)
            }
        }
        return matchedConditionObjects

    }

    private fun deduplicateAndCarryOverCollectionPropertiesAsOne(filteredObjects: List<S3ObjectSummary>): MutableMap<String, Long> {
//    Deduplicate objects by collection name and combine size values
//       s3://bucket/adb/2020-06-23/accepted-data.UpdateMongoLock_acceptedDataService.0001.json.gz.enc

        val topicByteSizeMap = mutableMapOf<String, Long>()

        filteredObjects.forEach { collection ->
            val key = collection.key

            Regex(collectionNameRegexPattern).find(key)?.let {
                val (topicName) = it.destructured

                if (topicName in topicByteSizeMap) {
                    topicByteSizeMap[topicName] = topicByteSizeMap[topicName]!! + collection.size
                } else {
                    topicByteSizeMap[topicName] = collection.size
                }
            }

        }

        return topicByteSizeMap

    }


    private fun getCollectionSummary(key: String, collectionSummaries: MutableList<S3ObjectSummary>) = CollectionSummary(getCollectionFromPath(key), collectionSummaries.sumBy { it.size.toInt() })

    companion object {
        val logger = DataworksLogger.getLogger(S3ReaderServiceImpl::class.toString())
    }
}
