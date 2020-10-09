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

@Service
class TableProvisionerServiceImpl(private val s3ReaderService: S3ReaderServiceImpl,
                                  private val hbaseTableCreatorServiceImpl: HbaseTableCreatorServiceImpl,
                                  @Qualifier("regionTargetSize")
                                  private val regionTargetSize: Int,
                                  @Qualifier("regionServerCount")
                                  private val regionServerCount: Int,
                                  private val chunkSize: Int,
                                  private val regionReplicationCount: Int) : TableProvisionerService {

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

        var currentChunk = 0
        collectionDetailsMap.entries.chunked(chunkSize).forEach {
            runBlocking {
                it.forEach { (collectionName, size) ->
                    launch(Dispatchers.IO) {
                        logger.info("Provisioning table",
                                "current_chunk" to "${currentChunk++}",
                                "chunk_size" to "$chunkSize",
                                "region_replication" to "$regionReplicationCount")
                        val collectionRegionSize = calculateCollectionRegionSize(regionUnit, size)
                        logger.info("Size of collection in percentage",
                                "collection_name" to "${collectionName}",
                                "collection_size_percentage" to "${(size / totalBytes) * 100}"
                        )
                        val splits = calculateSplits(collectionRegionSize)
                        hbaseTableCreatorServiceImpl.createHbaseTableFromProps(collectionName, collectionRegionSize, splits)
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
