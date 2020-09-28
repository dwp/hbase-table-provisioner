package app.service.impl

import app.service.TableProvisionerService
import org.springframework.stereotype.Service
import kotlin.math.ceil

@Service
class TableProvisionerServiceImpl(val s3ReaderService: S3ReaderServiceImpl, val hbaseTableCreatorImpl: HbaseTableCreatorImpl) : TableProvisionerService {

    override fun provisionHbaseTable(regionSize: Int, regionServers: Int): Int {
        // Call S3 Client and get a list of collections + byte sizes of files
        val collectionSummaries = s3ReaderService.getCollectionSummaries()

        val totalBytes = Int
        collectionSummaries.forEach {
            totalBytes + it.size
        }

        val totalRegions = regionSize * regionServers
        val regionUnit = totalBytes / totalRegions

        collectionSummaries.forEach {
            val collectionRegionSize = calculateCollectionRegionSize(regionUnit, it.size)
            hbaseTableCreatorImpl.createHbaseTableFromProps(it.collectionName, collectionRegionSize)
        }

        // Calculate number of regions
        // Total of regions - calculated by multiplying number of region servers by the desired region count per server
        // Using the byte of each collection, calculate the percentage of regions that each collection should have
        // Total bytes / Total regions = RegionUnit
        // Per region, collection bytes / RegionUnit = Number of regions required. Round up rather than down
        // With number of regions per collection acquired, call Hbase create table function and specify splits


    }

    private fun calculateCollectionRegionSize(regionUnit: Int, collectionSize: Int) = ceil(collectionSize.toDouble() / regionUnit).toInt()

}

