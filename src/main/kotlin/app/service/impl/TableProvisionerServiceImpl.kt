package app.service.impl

import app.service.TableProvisionerService
import com.amazonaws.services.s3.AmazonS3Client
import org.springframework.stereotype.Service

@Service
class TableProvisionerServiceImpl(val s3ReaderService: S3ReaderServiceImpl) : TableProvisionerService {

    override fun provisionHbaseTable(a: Int, b: Int): Int {
        // TODO
        // Call S3 Client and get a list of collections + byte sizes of files
        val collections = s3ReaderService.getCollections()

        // Calculate number of regions
        // Total of regions - calculated by multiplying number of region servers by the desired region count per server
        // Using the byte of each collection, calculate the percentage of regions that each collection should have
        // Total bytes / Total regions = RegionUnit
        // Per region, collection bytes / RegionUnit = Number of regions required. Round up rather than down
        // With number of regions per collection acquired, call Hbase create table function and specify splits

        return a * b
    }

}

