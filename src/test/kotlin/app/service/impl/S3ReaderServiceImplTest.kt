package app.service.impl

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.nhaarman.mockitokotlin2.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class S3ReaderServiceImplTest {

    @Test
    fun shouldReturnListOfCollectionSummariesWhenRequestingFromS3() {

        val bucket = "bucket"
        val path = "collection"

        val objectSummaries1 = listOf<S3ObjectSummary>()

        val result1 = mock<ListObjectsV2Result> {
            on { objectSummaries } doReturn objectSummaries1
            on { nextContinuationToken } doReturn "1"
            on { isTruncated } doReturn false
        }

        val request1 = ListObjectsV2Request().withBucketName(bucket).withPrefix(path).withMaxKeys(1000)

        val s3Client = mock<AmazonS3> {
            on { listObjectsV2(request1) } doReturn result1
        }

        val service = service(s3Client, bucket, path)

        val collectionSummaries = service.getCollectionSummaries()

        assertThat(collectionSummaries.size).isEqualTo(1)
        verify(s3Client, times(1)).listObjectsV2(request1)
    }

    private fun service(s3Client: AmazonS3, bucket: String, path: String) = S3ReaderServiceImpl(s3Client, bucket, path)
}
