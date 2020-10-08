package app.service.impl

import com.nhaarman.mockitokotlin2.*
import org.junit.jupiter.api.Test

class TableProvisionerServiceImplTest {

    @Test
    fun shouldProvisionHbaseTablesWhenRequestedGivenCollectionsExist() {

        val collectionSummaries = mockCollectionSummaries()

        val s3ReaderServiceMock = mock<S3ReaderServiceImpl> {
            on {
                getCollectionSummaries()
            } doReturn collectionSummaries
        }

        val hbaseTableCreatorMock = mock<HbaseTableCreatorServiceImpl>()

        val regionTargetSize = 1
        val regionServerCount = 3

        val service = TableProvisionerServiceImpl(s3ReaderServiceMock, hbaseTableCreatorMock, regionTargetSize, regionServerCount, 10)

        service.provisionHbaseTable()

        verify(s3ReaderServiceMock, times(1)).getCollectionSummaries()
        verify(hbaseTableCreatorMock, times(2)).createHbaseTableFromProps(any(), any(), any())
    }

    @Test
    fun shouldNotProvisionedHbaseTablesWhenRequestedGivenNoCollectionsExist() {

        val s3ReaderServiceMock = mock<S3ReaderServiceImpl> {
            on {
                getCollectionSummaries()
            } doReturn mutableMapOf()
        }

        val hbaseTableCreatorMock = mock<HbaseTableCreatorServiceImpl>()

        val regionTargetSize = 1
        val regionServerCount = 3

        val service = TableProvisionerServiceImpl(s3ReaderServiceMock, hbaseTableCreatorMock, regionTargetSize, regionServerCount, 10)

        service.provisionHbaseTable()

        verify(s3ReaderServiceMock, times(1)).getCollectionSummaries()
        verifyZeroInteractions(hbaseTableCreatorMock)
    }

    private fun mockCollectionSummaries(): MutableMap<String, Long> =
            mutableMapOf("collection_1" to 100, "collection_2" to 100)
}
