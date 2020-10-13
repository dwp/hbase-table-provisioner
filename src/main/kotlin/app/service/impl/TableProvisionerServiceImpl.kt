package app.service.impl

import app.service.TableProvisionerService
import app.util.calculateSplits
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.logging.DataworksLogger
import kotlin.math.ceil
import kotlin.time.ExperimentalTime

@Service
class TableProvisionerServiceImpl(private val s3ReaderService: S3ReaderServiceImpl,
                                  private val hbaseTableCreatorServiceImpl: HbaseTableCreatorServiceImpl,
                                  @Qualifier("regionTargetSize")
                                  private val regionTargetSize: Int,
                                  @Qualifier("regionServerCount")
                                  private val regionServerCount: Int,
                                  private val chunkSize: Int,
                                  private val regionReplicationCount: Int,
                                  private val largeTableThreshold: Int) : TableProvisionerService {

    @ExperimentalTime
    override fun provisionHbaseTable() {

        logger.info("Running provisioner for Hbase tables",
                "region_target_size" to "$regionTargetSize",
                "region_server_count" to "$regionServerCount",
                "chunk_size" to "$chunkSize",
                "region_replication" to "$regionReplicationCount")

        val collectionDetailsMap: MutableMap<String, Long> = s3ReaderService.getCollectionSummaries()

        if (collectionDetailsMap.isEmpty()) {
            logger.error("No collections to be created in Hbase")
            return
        }
        logger.info("Found collections to be created in Hbase", "collection_count" to "${collectionDetailsMap.size}")
        logger.info("List of collections to be created in Hbase", "collection_list" to "${collectionDetailsMap.keys}")

        val totalBytes = getTotalBytesForAllCollections(collectionDetailsMap)
        val totalRegionsForAllRegionServers = regionTargetSize * regionServerCount
        val totalRegionsForAllTables = totalRegionsForAllRegionServers / regionReplicationCount
        val regionUnit = totalBytes / totalRegionsForAllTables

        logger.info("Provisioning tables for collections",
                "number_of_collections" to "${collectionDetailsMap.size}",
                "region_target_size" to "$regionTargetSize",
                "region_server_count" to "$regionServerCount",
                "total_regions" to "$totalRegionsForAllTables",
                "total_bytes" to "$totalBytes",
                "region_unit" to regionUnit.toString(),
                "chunk_size" to "$chunkSize",
                "region_replication" to "$regionReplicationCount"
        )

        // Sort the collections by size, this ensures that smaller tables are not being created asynchronously
        // when a synchronous large table creation request is sent off.
        collectionDetailsMap.entries
            .sortedBy { (_, size) -> -size}
            .chunked(chunkSize)
            .forEachIndexed { chunkIndex, chunk ->
                runBlocking {
                    chunk.forEachIndexed { tableIndex, (collectionName, size) ->
                        logger.info(
                            "Provisioning table", "collection_name" to "$collectionName",
                            "table_number" to "${chunkSize * chunkIndex + tableIndex}",
                            "total_table_number" to "${collectionDetailsMap.size}",
                            "region_replication" to "$regionReplicationCount", "size" to "$size",
                            "collection_size_percentage" to String.format("%.02f%%", (size.toFloat() / totalBytes) * 100))
                        val collectionRegionSize = calculateCollectionRegionSize(regionUnit, size)
                        val splits = calculateSplits(collectionRegionSize)
                        if (splits.size > largeTableThreshold) {
                            hbaseTableCreatorServiceImpl.createHbaseTableFromProps(collectionName, collectionRegionSize, splits)
                        }
                        else {
                            launch {
                                hbaseTableCreatorServiceImpl.createHbaseTableFromProps(collectionName, collectionRegionSize, splits)
                            }
                        }
                    }
                }
            }


        logger.info("Provisioned all tables for collections",
                "number_of_collections" to "${collectionDetailsMap.size}",
                "region_target_size" to "$regionTargetSize",
                "region_server_count" to "$regionServerCount",
                "total_regions" to "$totalRegionsForAllTables",
                "total_bytes" to "$totalBytes",
                "region_unit" to regionUnit.toString(),
                "chunk_size" to "$chunkSize",
                "region_replication" to "$regionReplicationCount"
        )
    }

    private fun getTotalBytesForAllCollections(collectionDetailsMap: MutableMap<String, Long>) = collectionDetailsMap.values.sum()

    private fun calculateCollectionRegionSize(regionUnit: Long, collectionSize: Long) = ceil(collectionSize.toDouble() / regionUnit).toInt()

    companion object {
        val logger = DataworksLogger.getLogger(TableProvisionerServiceImpl::class.toString())
    }
}
