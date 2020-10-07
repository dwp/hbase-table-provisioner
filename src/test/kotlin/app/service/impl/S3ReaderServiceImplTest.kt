package app.service.impl

import app.helper.impl.S3HelperImpl
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.nhaarman.mockitokotlin2.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class S3ReaderServiceImplTest {

    @Test
    fun shouldReturnListOfOneCollectionSummariesWhenRequestingMinimumFilesFromS3() {

        val bucket = "bucket"
        val basePath = "/business/mongo"
        val sourceDatabasePaths = "adb/2020-06-23"
        val filenameFormatRegexPattern = "[\\w-]+\\.[\\w-]+\\.[0-9]+\\.json\\.gz\\.enc"
        val collectionNameRegexPattern = "([-\\w]+\\.[-.\\w]+)\\.[0-9]+\\.json\\.gz\\.enc"

        val objectSummary = S3ObjectSummary()
        objectSummary.bucketName = bucket
        objectSummary.size = 100L
        objectSummary.key = "accepted-data.UpdateMongoLock_acceptedDataService.0001.json.gz.enc"

        val objectSummaries1 = listOf(objectSummary)

        val result1 = mock<ListObjectsV2Result> {
            on { objectSummaries } doReturn objectSummaries1
            on { nextContinuationToken } doReturn "1"
            on { isTruncated } doReturn false
        }

        val prefix = "$basePath/${sourceDatabasePaths[0]}"

        val request1 = ListObjectsV2Request().withBucketName(bucket).withPrefix(prefix).withMaxKeys(1000)

        val s3ClientMock = mock<AmazonS3> {
            on { listObjectsV2(request1) } doReturn result1
        }

        val s3HelperMock = mock<S3HelperImpl> {
            on { getListOfS3ObjectsResult(any(), any()) } doReturn result1
        }

        val service = service(s3ClientMock, s3HelperMock, bucket, basePath, sourceDatabasePaths, filenameFormatRegexPattern, collectionNameRegexPattern)

        val collectionSummaries = service.getCollectionSummaries()

        assertThat(collectionSummaries.size).isEqualTo(1)
    }

    private fun service(s3Client: AmazonS3, s3HelperMock: S3HelperImpl, bucket: String, basePath: String,
                        sourceDatabasePaths: String, filenameFormatRegexPattern: String, collectionNameRegexPattern: String)
            = S3ReaderServiceImpl(s3Client, s3HelperMock, bucket, basePath, sourceDatabasePaths, filenameFormatRegexPattern, "", collectionNameRegexPattern)
}
