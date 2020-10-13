package app.service.impl

import com.nhaarman.mockitokotlin2.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import kotlin.time.ExperimentalTime

class TableProvisionerServiceImplTest {

    @ExperimentalTime
    @Test
    suspend fun shouldProvisionHbaseTablesWhenRequestedGivenCollectionsExist() {

        val collectionSummaries = mockCollectionSummaries()

        val s3ReaderServiceMock = mock<S3ReaderServiceImpl> {
            on {
                getCollectionSummaries()
            } doReturn collectionSummaries
        }

        val hbaseTableCreatorMock = mock<HbaseTableCreatorServiceImpl>()

        val regionTargetSize = 1
        val regionServerCount = 3
        val regionReplicationCount = 3

        val service = TableProvisionerServiceImpl(s3ReaderServiceMock, hbaseTableCreatorMock, regionTargetSize,
            regionServerCount, 10, regionReplicationCount, 10)

        service.provisionHbaseTable()

        verify(s3ReaderServiceMock, times(1)).getCollectionSummaries()
        val collectionCaptor = argumentCaptor<String>()
        val capacityCaptor = argumentCaptor<Int>()
        val splitsCaptor = argumentCaptor<List<ByteArray>>()
        verify(hbaseTableCreatorMock, times(2)).createHbaseTableFromProps(collectionCaptor.capture(),
                                                                                       capacityCaptor.capture(),
                                                                                       splitsCaptor.capture())

        collectionCaptor.allValues.forEachIndexed { index, s ->
            val expected = if (index == 0) "collection_2" else "collection_1"
            assertEquals(expected, s)
        }
    }

    @ExperimentalTime
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
        val regionReplicationCount = 3

        val service = TableProvisionerServiceImpl(s3ReaderServiceMock, hbaseTableCreatorMock, regionTargetSize,
            regionServerCount, 10, regionReplicationCount, 10)

        service.provisionHbaseTable()

        verify(s3ReaderServiceMock, times(1)).getCollectionSummaries()
        verifyZeroInteractions(hbaseTableCreatorMock)
    }

    private fun mockCollectionSummaries(): MutableMap<String, Long> =
            mutableMapOf("collection_1" to 100, "collection_2" to 300)
}
