package app.service.impl

import app.domain.CollectionSummary
import app.service.TableProvisionerService
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.logging.DataworksLogger
import kotlin.math.ceil

@Service
class TableProvisionerServiceImpl(private val s3ReaderService: S3ReaderServiceImpl,
                                  private val hbaseTableCreatorImpl: HbaseTableCreatorImpl,
                                  private val regionTargetSize: Int,
                                  private val regionServerCount: Int) : TableProvisionerService {

    override fun provisionHbaseTable() {

        logger.info("Running provisioner for Hbase tables")

        val collectionSummaries = s3ReaderService.getCollectionSummaries()

        val totalBytes = getTotalBytesForCollection(collectionSummaries)

        val totalRegions = regionTargetSize * regionServerCount
        val regionUnit = totalBytes / totalRegions

        collectionSummaries.forEach {
            val collectionRegionSize = calculateCollectionRegionSize(regionUnit, it.size)
            hbaseTableCreatorImpl.createHbaseTableFromProps(it.collectionName, collectionRegionSize)
        }
    }

    private fun getTotalBytesForCollection(collectionSummaries: List<CollectionSummary>) = collectionSummaries.sumBy { it.size }

    private fun calculateCollectionRegionSize(regionUnit: Int, collectionSize: Int) = ceil(collectionSize.toDouble() / regionUnit).toInt()

    companion object {
        val logger = DataworksLogger.getLogger(TableProvisionerServiceImpl::class.toString())
    }
}
