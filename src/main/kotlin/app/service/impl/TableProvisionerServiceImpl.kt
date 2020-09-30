package app.service.impl

import app.domain.CollectionSummary
import app.service.TableProvisionerService
import app.util.calculateSplits
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.logging.DataworksLogger
import kotlin.math.ceil

@Service
class TableProvisionerServiceImpl(private val s3ReaderService: S3ReaderServiceImpl,
                                  private val hbaseTableCreatorServiceImpl: HbaseTableCreatorServiceImpl,
                                  private val regionTargetSize: Int,
                                  private val regionServerCount: Int) : TableProvisionerService {

    override fun provisionHbaseTable() {

        logger.info("Running provisioner for Hbase tables")

        val collectionDetailsMap = s3ReaderService.getCollectionSummaries()

        if (collectionDetailsMap.isEmpty()) {
            logger.error("No collections to be created in Hbase")
            return
        }

        val totalBytes = getTotalBytesForAllCollections(collectionDetailsMap)

        val totalRegions = regionTargetSize * regionServerCount
        val regionUnit = totalBytes / totalRegions

        logger.info("Provisioning tables for collections",
                "number_of_collections" to collectionDetailsMap.size.toString(),
                "region_unit" to regionUnit.toString())

        runBlocking {
            collectionDetailsMap.forEach {
                launch {
                    val collectionRegionSize = calculateCollectionRegionSize(regionUnit, it.value)
                    val splits = calculateSplits(collectionRegionSize)
                    hbaseTableCreatorServiceImpl.createHbaseTableFromProps(it.key, collectionRegionSize, splits)
                }
            }
        }
    }


    private fun getTotalBytesForAllCollections(collectionDetailsMap: MutableMap<String, Long>) = collectionDetailsMap.values.sum()

    private fun calculateCollectionRegionSize(regionUnit: Long, collectionSize: Long) = ceil(collectionSize.toDouble() / regionUnit).toInt()

    companion object {
        val logger = DataworksLogger.getLogger(TableProvisionerServiceImpl::class.toString())
    }
}
