package app.service.impl

import app.domain.CollectionSummary
import app.domain.S3ObjectSummaryPair
import app.helper.impl.S3Helper
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
class S3ReaderServiceImpl(val s3Client: AmazonS3,
                          val bucket: String,
                          val basePath: String,
                          val sourceDatabasePaths: List<String>,
                          val filenameFormatRegexPattern: String,
                          val filenameFormatDataExtensionPattern: String,
                          val keyPairGeneratorService: KeyPairGeneratorImpl,
                          val s3Helper: S3Helper) : S3ReaderService {

    override fun getCollectionSummaries(): List<CollectionSummary> {
//        s3://bucket/basepath/sourceDbPath/DateIncluded/collection.tables.file
// Fn returns list of objects; each object is an instance of collection, containing collection name and size properties

        val collectionObjectsList = sourceDatabasePaths.forEach() { sourceDatabasePath ->
            getCollectionObjectsInPath(bucket, basePath, sourceDatabasePath)

        }
        return collectionObjectsList
    }

    fun getCollectionObjectsInPath(fullS3BasePath: String, sourceDatabasePath: String): List {
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
            deduplicateAndCarryOverCollectionPropertiesAsOne(filteredObjects)

            val list = listOf()
            return

        }
    }

    private fun filterResultsForDataFilesOnly(s3Results: MutableList<S3ObjectSummary>, filenameFormatRegexPattern: String, filenameFormatDataExtensionPattern: String) : List {
//        Filters object summary from S3 for unique collection names and information

        val matchedConditionObjects = listOf()
        val fileNameFormatRegex = "$filenameFormatRegexPattern$filenameFormatDataExtensionPattern"

        s3Results.forEach() { collectionObject ->
            if (fileNameFormatRegex.toRegex().containsMatchIn(collectionObject.key) )
                matchedConditionObjects.add(collectionObject)
        }
        return matchedConditionObjects

    }

    private fun deduplicateAndCarryOverCollectionPropertiesAsOne(filteredObjects: List) {
//    Deduplicate objects by collection name and combine size values

        val matchedCollectionObjects = listOf()

        filteredObjects
        }

    }











        private fun getListOfObjectsInPath(objectPath: String): List<S3ObjectSummaryPair> {
            try {
                val request = ListObjectsV2Request().withBucketName(bucket).withPrefix(objectPath).withMaxKeys(1000)
                var results: ListObjectsV2Result?
                val objectSummaries: MutableList<S3ObjectSummary> = mutableListOf()

                do {
                    logger.info("Getting paginated results", "s3_location", "s3://$bucketName/$fullPrefix")
                    results = s3Helper.listObjectsV2Result(s3Client, request, objectSummaries)
                    request.continuationToken = results?.nextContinuationToken
                } while (results != null && results.isTruncated)

                val objectSummaryKeyMap = objectSummaries.map { it.key to it }.toMap()
                val keyPairs =
                        keyPairGeneratorService.generateKeyPairs(objectSummaries.map { it.key },
                                filenameFormatRegexPattern.toRegex(),
                                filenameFormatDataExtensionPattern.toRegex())

                val pairs = keyPairs
                        .map {
                            val obj = objectSummaryKeyMap[it.dataKey]
                            S3ObjectSummaryPair(obj)
                        }
                        .filter { pair -> pair.data != null }
                        .filter { pair ->
                            val data = pair.data!!
                            if (data.size == 0L) {
                                logger.info("Ignoring zero-byte pair", "data_key", data.key)
                                logger.info("Processed records in file", "records_processed", "0", "file_name", data.key)
                            }

                            data.size > 0
                        }

                logger.info("Found valid key pairs", "s3_keypairs_found", "${pairs.size}", "s3_location", "s3://$bucket/$basePath")
                return pairs

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
