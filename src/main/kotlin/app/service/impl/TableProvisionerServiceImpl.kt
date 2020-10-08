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
                                  private val regionServerCount: Int) : TableProvisionerService {

    override fun provisionHbaseTable() {

        logger.info("Running provisioner for Hbase tables",
            "region_target_size" to "$regionTargetSize",
            "region_server_count" to "$regionServerCount")

        val collectionDetailsMap: MutableMap<String, Long> = s3ReaderService.getCollectionSummaries()

        if (collectionDetailsMap.isEmpty()) {
            logger.error("No collections to be created in Hbase")
            return
        }
        logger.info("Found collections to be created in Hbase", "collection_count" to "${collectionDetailsMap.size}")

        val totalBytes = getTotalBytesForAllCollections(collectionDetailsMap)
        val totalRegions = regionTargetSize * regionServerCount
        val regionUnit = totalBytes / totalRegions

        logger.info("Provisioning tables for collections",
            "number_of_collections" to "${collectionDetailsMap.size}",
            "region_target_size" to "$regionTargetSize",
            "region_server_count" to "$regionServerCount",
            "total_regions" to "$totalRegions",
            "total_bytes" to "$totalBytes",
            "region_unit" to regionUnit.toString())

        val max_chunks = 10
        var current_chunk = 0
        val split = collectionDetailsMap.entries.chunked(max_chunks)

        split.forEach {
            runBlocking {
                it.forEach { (collectionName, size) ->
                    launch(Dispatchers.IO) {
                        logger.info("Provisioning table",
                            "current_chunk" to "${current_chunk++}",
                            "max_chunks" to "$max_chunks")
                        val collectionRegionSize = calculateCollectionRegionSize(regionUnit, size)
                        val splits = calculateSplits(collectionRegionSize)
                        hbaseTableCreatorServiceImpl.createHbaseTableFromProps(collectionName, collectionRegionSize, splits)
                    }
                }
            }
        }

        logger.info("Provisioned all tables for collections",
            "number_of_collections" to "${collectionDetailsMap.size}",
            "max_chunks" to "$max_chunks"
        )
    }

    private fun getTotalBytesForAllCollections(collectionDetailsMap: MutableMap<String, Long>) = collectionDetailsMap.values.sum()

    private fun calculateCollectionRegionSize(regionUnit: Long, collectionSize: Long) = ceil(collectionSize.toDouble() / regionUnit).toInt()

    companion object {
        val logger = DataworksLogger.getLogger(TableProvisionerServiceImpl::class.toString())
    }
}
