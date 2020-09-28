package app.service.impl

import app.configuration.CollectionS3Configuration
import app.service.HbaseTableCreator
import org.apache.hadoop.hbase.*
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.io.compress.Compression
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.logging.DataworksLogger


@Service
class HbaseTableCreatorImpl(
        private val hbaseConnection: Connection,
        private val dataFamily: ByteArray,
        private val dataQualifier: ByteArray,
        private val hbaseRegionReplication: Int) : HbaseTableCreator {

    override fun createHbaseTableFromProps(collectionName: String, regionSize: Int) {
        // Check if table already exists via ensureTable
        // Create table if it doesn't using
        ensureNamespaceExists(collectionName)
        createHbaseTable(collectionName, hbaseConnection)

        }
    }

    fun createHbaseTable(collectionName: String, hbaseConnection: Connection) {
        logger.info("Creating table '$collectionName'.")
        hbaseConnection.admin.createTable(HTableDescriptor(collectionName).apply {
            addFamily(HColumnDescriptor(dataFamily)
                    .apply {
                        maxVersions = Int.MAX_VALUE
                        minVersions = 1
                        compressionType = Compression.Algorithm.GZ
                        compactionCompressionType = Compression.Algorithm.GZ
                    })
            setRegionReplication(hbaseRegionReplication)
        })
    }

    fun ensureNamespaceExists(tableName: String) {
        val dataTableName = TableName.valueOf(tableName)
        val namespace = dataTableName.namespaceAsString

        if (!namespaces.contains(namespace)) {
            try {
                logger.info("Creating namespace '$namespace'.")
                hbaseConnection.admin.createNamespace(NamespaceDescriptor.create(namespace).build())
            } catch (e: NamespaceExistException) {
                logger.info("'$namespace' already exists, probably created by another process")
            } finally {
                namespaces[namespace] = true
            }
        }
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
