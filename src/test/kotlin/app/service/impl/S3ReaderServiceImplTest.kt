package app.service.impl

import app.helper.S3Helper
import app.helper.impl.S3HelperImpl
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.nhaarman.mockitokotlin2.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class S3ReaderServiceImplTest {

    @Test
    fun shouldAccumulateCdlInputSizesPerCollection() {
        val result1 = summariesResult(summaries("2021/09/19"), "CONTINUATION_TOKEN_1")
        val result2 = summariesResult(summaries("2021/09/20"), "CONTINUATION_TOKEN_2")
        val result3 = summariesResult(summaries("2021/09/21"))

        val amazonS3 = mock<AmazonS3> {
            on { listObjectsV2(any<ListObjectsV2Request>()) } doReturnConsecutively listOf(result1, result2, result3)
        }
        val service = service(amazonS3, mock(), filenameRe = cdlPattern)
        val results = service.collectionSizes()
        verify(amazonS3, times(3)).listObjectsV2(any<ListObjectsV2Request>())
        verifyNoMoreInteractions(amazonS3)
        assertEquals(10, results.size)
        results.forEach { (_, value) -> assertEquals(value, 30) }
    }

    @Test
    fun shouldAccumulateSizesWhenCollectionAppearsInMultipleClusters() {
        val adbSourcePath = "adb/2020-06-23"
        val cdbSourcePath = "cdb/2020-06-23"
        val sourceDatabasePaths = "$adbSourcePath,$cdbSourcePath"

        val amazonS3 = mock<AmazonS3>()

        val adbAgentCoreSummaries = (1 .. 100).map {
            s3ObjectSummary(it.toLong(),
                "$basePath/$adbSourcePath",
                "agent-core.agentToDo%s.%04d.json.gz.enc".format(if (it % 2 == 0) "Archive" else "", it))
        }


        val cdbAgentCoreSummaries = (1 .. 100).map {
            s3ObjectSummary(it.toLong(),
                "$basePath/$cdbSourcePath", "agent-core.agentToDo%s.%04d.json.gz.enc".format(if (it % 2 == 0) "Archive" else "", it))
        }

        val adbResult = mock<ListObjectsV2Result> {
            on { objectSummaries } doReturn adbAgentCoreSummaries
            on { isTruncated } doReturn false
        }

        val cdbResult = mock<ListObjectsV2Result> {
            on { objectSummaries } doReturn cdbAgentCoreSummaries
            on { isTruncated } doReturn false
        }

        val clientCaptor = argumentCaptor<AmazonS3>()
        val requestCaptor = argumentCaptor<ListObjectsV2Request>()
        val s3Helper = mock<S3Helper> {
            on {
                getListOfS3ObjectsResult(clientCaptor.capture(), requestCaptor.capture())
            } doReturnConsecutively listOf(adbResult, cdbResult)
        }
        val service = service(amazonS3, s3Helper, sourceDatabasePaths)
        val sizes = service.getCollectionSummaries()
        assertEquals(1, sizes.size)

        sizes.entries.forEach { (collection, size) ->
            val expected = (1.. 100).sum() * 2
            assertEquals("agent_core:agentToDo", collection)
            assertEquals(expected.toLong(), size)
        }

        val (adbPrefix, cdbPrefix) = requestCaptor.allValues.map(ListObjectsV2Request::getPrefix)
        assertEquals("$basePath/$adbSourcePath", adbPrefix)
        assertEquals("$basePath/$cdbSourcePath", cdbPrefix)
    }

    private fun s3ObjectSummary(objectSize: Long, prefix: String, fileName: String): S3ObjectSummary = mock {
        on { size } doReturn objectSize
        on { bucketName } doReturn bucket
        on { key } doReturn "$prefix/$fileName"
    }

    @Test
    fun shouldReturnListOfOneCollectionSummariesWhenRequestingMinimumFilesFromS3() {

        val sourceDatabasePaths = "adb/2020-06-23"

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

        val service = service(s3ClientMock, s3HelperMock, sourceDatabasePaths)

        val collectionSummaries = service.getCollectionSummaries()

        assertThat(collectionSummaries.size).isEqualTo(1)
    }

    private fun service(s3Client: AmazonS3, s3HelperMock: S3Helper, sourceDatabasePaths: String = "", filenameRe: String = filenameFormatRegexPattern)
            = S3ReaderServiceImpl(s3Client, s3HelperMock, bucket, basePath, sourceDatabasePaths, filenameRe, collectionNameRegexPattern)

    private fun summariesResult(objectSummaries1: List<S3ObjectSummary>,
                                continuationToken: String? = null): ListObjectsV2Result =
        mock {
            on { isTruncated } doReturn !continuationToken.isNullOrBlank()
            on { nextContinuationToken } doReturn continuationToken
            on { objectSummaries } doReturn objectSummaries1
        }

    private fun summaries(day: String): List<S3ObjectSummary> =
        List(10) { index ->
            mock {
                on { size } doReturn 10
                on { key } doReturn "directory/sub-directory/$day/database/collection$index/db.database.collection${index}_${index}_${100 + index}_${100 + index}.jsonl.gz"
            }
        }

    companion object {
        const val bucket = "bucket"
        const val basePath = "/business/mongo"
        const val filenameFormatRegexPattern = "[\\w-]+\\.[\\w-]+\\.[0-9]+\\.json\\.gz\\.enc"
        const val collectionNameRegexPattern = "([-\\w]+\\.[-.\\w]+)\\.[0-9]+\\.json\\.gz\\.enc"
        const val cdlPattern = """(?:db\.)?([-.\w]+)\.([-.\w]+)_\d+_\d+_\d+.jsonl.gz$"""
    }
}
