package app.service.impl

import app.configuration.CollectionS3Configuration
import app.exception.TableExistsInHbase
import app.service.HbaseTableCreatorService
import org.apache.hadoop.hbase.*
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.io.compress.Compression
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Service
class HbaseTableCreatorServiceImpl(
        private val hbaseConnection: Connection,
        private val columnFamily: String,
        private val regionReplicationCount: Int) : HbaseTableCreatorService {

    override fun createHbaseTableFromProps(collectionName: String, regionCapacity: Int, splits: List<ByteArray>) {
//        accepted-data.UpdateMongoLock_acceptedDataService CHANGE TO accepted-data:UpdateMongoLock_acceptedDataService

        ensureNamespaceExists(collectionName)

        if (checkIfTableExists(collectionName)) {
            logger.error("Table already exists in hbase for collection", "collection_name" to collectionName)
            throw TableExistsInHbase("Table already exists in hbase for collection: $collectionName")
        } else {
            createHbaseTable(collectionName, regionCapacity, splits)
        }
    }

    fun ensureNamespaceExists(tableName: String) {
        val dataTableName = TableName.valueOf(tableName)
        val namespace = dataTableName.namespaceAsString

        if (!namespaces.contains(namespace)) {
            try {
                logger.info("Creating namespace", "namespace" to namespace)
                hbaseConnection.admin.createNamespace(NamespaceDescriptor.create(namespace).build())
            } catch (e: NamespaceExistException) {
                logger.info("Namespace already exists, probably created by another process", "namespace" to namespace)
            } finally {
                namespaces[namespace] = true
            }
        }
    }

    private fun createHbaseTable(collectionName: String, regionCapacity: Int, splits: List<ByteArray>) {

        logger.info("Creating Hbase table", "table_name" to collectionName, "region_capacity" to regionCapacity.toString())

        hbaseConnection.admin.createTable(HTableDescriptor.parseFrom(collectionName.toByteArray()).apply {
            addFamily(HColumnDescriptor(columnFamily)
                    .apply {
                        maxVersions = Int.MAX_VALUE
                        minVersions = 1
                        compressionType = Compression.Algorithm.GZ
                        compactionCompressionType = Compression.Algorithm.GZ
                    })
            regionReplication = regionReplicationCount
        }, splits.toTypedArray())

        logger.info("Created Hbase table", "table_name" to collectionName, "region_capacity" to regionCapacity.toString())
    }

    private fun checkIfTableExists(collectionName: String): Boolean {
        val dataTableName = TableName.valueOf(collectionName)
        val tableNameString = dataTableName.nameAsString

        return tables.contains(tableNameString)
    }

    private val namespaces by lazy {
        val extantNamespaces = mutableMapOf<String, Boolean>()

        hbaseConnection.admin.listNamespaceDescriptors()
                .forEach {
                    extantNamespaces[it.name] = true
                }

        extantNamespaces
    }

    private val tables by lazy {
        val names = mutableMapOf<String, Boolean>()

        hbaseConnection.admin.listTableNames().forEach {
            names[it.nameAsString] = true
        }

        names
    }

    companion object {
        val logger = DataworksLogger.getLogger(CollectionS3Configuration::class.toString())
    }
}
