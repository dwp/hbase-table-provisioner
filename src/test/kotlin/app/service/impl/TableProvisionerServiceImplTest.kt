package app.service.impl

import app.configuration.HBaseConfiguration
import app.service.HbaseTableCreatorService
import app.service.S3ReaderService
import com.nhaarman.mockitokotlin2.*
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertIterableEquals
import org.junit.jupiter.api.Test
import kotlin.time.ExperimentalTime

@ExperimentalTime
class TableProvisionerServiceImplTest {

    @Test
    fun processesMultipleAdhocSpecifications() {
        val map = HBaseConfiguration.adhocSpecifications("$TABLE_1,$table1Regions|$TABLE_2,$table2Regions")
        assertEquals(mapOf(TABLE_1 to table1Regions, TABLE_2 to table2Regions), map)
    }

    @Test
    fun processesSingleAdhocSpecifications() {
        val map = HBaseConfiguration.adhocSpecifications("$TABLE_1,$table1Regions")
        assertEquals(mapOf(TABLE_1 to table1Regions), map)
    }

    @Test
    fun ignoresEmptySpecifications() {
        val map = HBaseConfiguration.adhocSpecifications("")
        assertEquals(mapOf<String, Int>(), map)
    }

    @Test
    fun ignoresNotSet() {
        val map = HBaseConfiguration.adhocSpecifications("NOT_SET")
        assertEquals(mapOf<String, Int>(), map)
    }


    @Test
    fun shouldProcessCdlInputs() = runBlocking {
        val hbaseTableCreatorService = mock<HbaseTableCreatorService>()
        val s3Service = mock<S3ReaderService> {
            on { collectionSizes() } doReturn mapOf(TABLE_1 to 1_000,
                TABLE_2 to 5_000,
                TABLE_3 to 20_000)
        }
        val regionTargetSize = 10
        val regionServerCount = 5
        val chunkSize = 3
        val regionReplicationCount = 2
        val largeTableThreshold = 10_000

        val service = TableProvisionerServiceImpl(s3Service, hbaseTableCreatorService,
            regionTargetSize, regionServerCount, chunkSize, regionReplicationCount, largeTableThreshold, "CDL", mapOf())

        service.provisionHbaseTables()
        verify(s3Service, times(1)).collectionSizes()
        verifyNoMoreInteractions(s3Service)
        val tableNameCaptor = argumentCaptor<String>()
        val splitCaptor = argumentCaptor<List<ByteArray>>()
        verify(hbaseTableCreatorService, times(3)).createHbaseTableFromProps(tableNameCaptor.capture(),
            splitCaptor.capture())

        assertIterableEquals(listOf(TABLE_1, TABLE_2, TABLE_3), tableNameCaptor.allValues.sorted())

        splitCaptor.allValues.forEachIndexed { index, value ->
            when (tableNameCaptor.allValues[index]) {
                TABLE_1 -> {
                    assertEquals(expectedRegionCount(regionTargetSize, regionServerCount, 1, 26), value.size)
                }
                TABLE_2 -> {
                    assertEquals(expectedRegionCount(regionTargetSize, regionServerCount, 5, 26), value.size)

                }
                TABLE_3 -> {
                    assertEquals(expectedRegionCount(regionTargetSize, regionServerCount, 20, 26), value.size)
                }
            }
        }
    }

    private fun expectedRegionCount(regionTargetSize: Int, regionServerCount: Int, proportion: Int, total: Int) =
        (regionTargetSize * regionServerCount / 2) * proportion / total

    @Test
    suspend fun usesAdHocSpecifications() {
        val s3 = s3()
        val hbaseTableCreatorService = mock<HbaseTableCreatorService>()
        val adhocTableCount = 10
        val adhocSpecifications = (1..adhocTableCount).map {
            Pair("database:collection$it", it * 10)
        }.toMap()

        val service = tableProvisionerService(s3, hbaseTableCreatorService, adhocSpecifications)

        service.provisionHbaseTables()
        val collectionCaptor = argumentCaptor<String>()
        val splitCaptor = argumentCaptor<List<ByteArray>>()
        verify(hbaseTableCreatorService, times(adhocTableCount)).createHbaseTableFromProps(collectionCaptor.capture(),
            splitCaptor.capture())

        collectionCaptor.allValues.forEachIndexed { index, tableName ->
            assertEquals("database:collection${index + 1}", tableName)
        }

        splitCaptor.allValues.forEachIndexed { index, splits ->
            assertEquals(((index + 1) * 10) - 1, splits.size)
        }

        verifyNoMoreInteractions(hbaseTableCreatorService)
        verifyZeroInteractions(s3)
    }


    @Test
    suspend fun shouldProvisionHbaseTablesWhenRequestedGivenCollectionsExist() {
        val s3 = s3()
        val hbaseTableCreator = mock<HbaseTableCreatorServiceImpl>()

        val service = tableProvisionerService(s3, hbaseTableCreator,
            mapOf(TABLE_1 to 10, TABLE_2 to 20))

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
            regionServerCount, 10, regionReplicationCount, 10, "", mapOf())

        service.provisionHbaseTables()

        verify(s3ReaderServiceMock, times(1)).getCollectionSummaries()
        verifyZeroInteractions(hbaseTableCreatorMock)
    }

    private fun tableProvisionerService(s3: S3ReaderService,
                                        hbaseTableCreatorService: HbaseTableCreatorService,
                                        adHocSpecifications: Map<String, Int>): TableProvisionerServiceImpl =
        TableProvisionerServiceImpl(s3, hbaseTableCreatorService, regionTargetSize,
            regionServerCount, 10, regionReplicationCount, 10, "",
            adHocSpecifications)

    private fun mockCollectionSummaries(): MutableMap<String, Long> =
        mutableMapOf("collection_1" to 100, "collection_2" to 300)

    companion object {
        private const val TABLE_1 = "database:collection1"
        private const val TABLE_2 = "database:collection2"
        private const val TABLE_3 = "database:collection3"
        
        private const val regionTargetSize = 1
        private const val regionServerCount = 3
        private const val regionReplicationCount = 3
        private const val table1Regions = 10
        private const val table2Regions = 20
    }
}
