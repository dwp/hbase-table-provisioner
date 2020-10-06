package app.service.impl

import app.configuration.CollectionsS3Configuration
import app.service.HbaseTableCreatorService
import org.apache.hadoop.hbase.*
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.io.compress.Compression
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Service
class HbaseTableCreatorServiceImpl(
        private val hbaseConnection: Connection,
        private val columnFamily: String,
        private val regionReplicationCount: Int) : HbaseTableCreatorService {

    override fun createHbaseTableFromProps(collectionName: String, regionCapacity: Int, splits: List<ByteArray>) {
        ensureNamespaceExists(collectionName)

        if (checkIfTableExists(collectionName)) {
            logger.warn("Table already exists in hbase for collection", "table_name" to collectionName)
        } else {
            createHbaseTable(collectionName, regionCapacity, splits)
        }
    }

    fun ensureNamespaceExists(collectionName: String) {

        val dataTableName = TableName.valueOf(collectionName)
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

        logger.info("Creating Hbase table",
                "table_name" to collectionName,
                "region_capacity" to regionCapacity.toString())

        val hbaseTableName = hbaseTableName(collectionName)

        val hbaseTable = HTableDescriptor(hbaseTableName).apply {
            addFamily(HColumnDescriptor(columnFamily)
                    .apply {
                        maxVersions = Int.MAX_VALUE
                        minVersions = 1
                        compressionType = Compression.Algorithm.GZ
                        compactionCompressionType = Compression.Algorithm.GZ
                    })
            regionReplication = regionReplicationCount
        }

        hbaseConnection.admin.createTable(hbaseTable, splits.toTypedArray())

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

    private fun hbaseTableName(name: String) = TableName.valueOf(name)

    companion object {
        val logger = DataworksLogger.getLogger(CollectionsS3Configuration::class.toString())
    }
}
