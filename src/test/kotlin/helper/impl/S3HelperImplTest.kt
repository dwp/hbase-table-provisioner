package helper.impl

import app.exception.S3Exception
import app.helper.impl.S3HelperImpl
import com.amazonaws.SdkClientException
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.doThrow
import com.nhaarman.mockitokotlin2.mock
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail

class S3HelperImplTest {

    @Test
    fun shouldSuccessfullyReadFromS3WhenRequestingWithAValidRequest() {

        val request = ListObjectsV2Request()
        val result = ListObjectsV2Result()

        val s3ClientMock = mock<AmazonS3> {
            on { listObjectsV2(request) } doReturn result
        }

        val s3Helper = S3HelperImpl(1, 1000, 2.0)

        s3Helper.getListOfS3ObjectsResult(s3ClientMock, request)
    }

    @Test
    fun shouldFailReadingFromS3WhenRequestingWithAnInvalidRequest() {

        val request = ListObjectsV2Request()

        val s3ClientMock = mock<AmazonS3> {
            on { listObjectsV2(request) } doThrow SdkClientException::class
        }

        val s3Helper = S3HelperImpl(1, 1000, 2.0)

        try {
            s3Helper.getListOfS3ObjectsResult(s3ClientMock, request)
            fail("Expected an exception")
        } catch (expected: S3Exception) {
            assertThat("app.exception.S3Exception: Error retrieving object summary from S3").isEqualTo(expected.toString())
        }
    }
}
