package app.service.impl

import app.service.HbaseTableCreatorService
import app.service.S3ReaderService
import com.nhaarman.mockitokotlin2.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import kotlin.time.ExperimentalTime

@ExperimentalTime
class TableProvisionerServiceImplTest {

    @Test
    suspend fun usesAdHocSpecifications() {
        val s3 = s3()
        val hbaseTableCreatorService = mock<HbaseTableCreatorService>()
        val adhocTableCount = 10
        val adhocSpecifications = (1 .. adhocTableCount + 1).map {
            Pair("database:collection$it", it * 10)
        }.toMap()

        val service = tableProvisionerService(s3, hbaseTableCreatorService, adhocSpecifications)

        service.provisionHbaseTables()
        val collectionCaptor = argumentCaptor<String>()
        val splitCaptor = argumentCaptor<List<ByteArray>>()
        verify(hbaseTableCreatorService, times(adhocTableCount)).createHbaseTableFromProps(collectionCaptor.capture(),
                                                                                          splitCaptor.capture())

        collectionCaptor.allValues.forEachIndexed { index, tableName ->
            assertEquals("database:collection$index", tableName)
        }

        splitCaptor.allValues.forEachIndexed { index, splits ->
            assertEquals(index * 10, splits.size)
        }

        verifyNoMoreInteractions(hbaseTableCreatorService)
        verifyZeroInteractions(s3)
    }

    private fun tableProvisionerService(s3: S3ReaderService,
                                        hbaseTableCreatorService: HbaseTableCreatorService,
                                        adHocSpecifications: Map<String, Int>): TableProvisionerServiceImpl =
            TableProvisionerServiceImpl(s3, hbaseTableCreatorService, regionTargetSize,
                    regionServerCount, 10, regionReplicationCount, 10,
                    adHocSpecifications)

    @Test
    suspend fun shouldProvisionHbaseTablesWhenRequestedGivenCollectionsExist() {
        val s3 = s3()
        val hbaseTableCreator = mock<HbaseTableCreatorServiceImpl>()

        val service = tableProvisionerService(s3, hbaseTableCreator,
                mapOf("database:collection1" to 10, "database:collection2" to 20))

        service.provisionHbaseTables()

        verify(s3, times(1)).getCollectionSummaries()
        val collectionCaptor = argumentCaptor<String>()
        val splitsCaptor = argumentCaptor<List<ByteArray>>()
        verify(hbaseTableCreator, times(2)).createHbaseTableFromProps(collectionCaptor.capture(),
                                                                                       splitsCaptor.capture())

        collectionCaptor.allValues.forEachIndexed { index, s ->
            val expected = if (index == 0) "collection_2" else "collection_1"
            assertEquals(expected, s)
        }
    }

    private fun s3(): S3ReaderService =
            mock {
                on {
                    getCollectionSummaries()
                } doReturn mockCollectionSummaries()
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
            regionServerCount, 10, regionReplicationCount, 10, mapOf())

        service.provisionHbaseTables()

        verify(s3ReaderServiceMock, times(1)).getCollectionSummaries()
        verifyZeroInteractions(hbaseTableCreatorMock)
    }

    private fun mockCollectionSummaries(): MutableMap<String, Long> =
            mutableMapOf("collection_1" to 100, "collection_2" to 300)

    private val regionTargetSize = 1
    private val regionServerCount = 3
    private val regionReplicationCount = 3

}
