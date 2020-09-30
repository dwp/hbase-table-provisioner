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

        val collectionSummaries = s3ReaderService.getCollectionSummaries()

        if (collectionSummaries.isEmpty()) {
            logger.info("No collections to be created in Hbase")
            return
        }

        val totalBytes = getTotalBytesForCollection(collectionSummaries)

        val totalRegions = regionTargetSize * regionServerCount
        val regionUnit = totalBytes / totalRegions

        logger.info("Provisioning tables for collections",
                "number_of_collections" to collectionSummaries.size.toString(),
                "region_unit" to regionUnit.toString())

        runBlocking {
            collectionSummaries.forEach {
                launch {
                    val collectionRegionSize = calculateCollectionRegionSize(regionUnit, it.size)
                    val splits = calculateSplits(collectionRegionSize)
                    hbaseTableCreatorServiceImpl.createHbaseTableFromProps(it.collectionName, collectionRegionSize, splits)
                }
            }
        }
    }


    private fun getTotalBytesForCollection(collectionSummaries: List<CollectionSummary>) = collectionSummaries.sumBy { it.size }

    private fun calculateCollectionRegionSize(regionUnit: Int, collectionSize: Int) = ceil(collectionSize.toDouble() / regionUnit).toInt()

    companion object {
        val logger = DataworksLogger.getLogger(TableProvisionerServiceImpl::class.toString())
    }
}
