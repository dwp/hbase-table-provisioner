package app.service.impl

import app.configuration.CollectionsS3Configuration
import app.service.HbaseTableCreatorService
import kotlinx.coroutines.delay
import org.apache.hadoop.hbase.*
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.io.compress.Compression
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.logging.DataworksLogger
import org.apache.hadoop.hbase.TableExistsException
import kotlin.time.ExperimentalTime
import kotlin.time.seconds

@Service
class HbaseTableCreatorServiceImpl(
        private val hbaseConnection: Connection,
        private val columnFamily: String,
        private val regionReplicationCount: Int) : HbaseTableCreatorService {

    @ExperimentalTime
    override suspend fun createHbaseTableFromProps(collectionName: String, regionCapacity: Int, splits: List<ByteArray>) {
        ensureNamespaceExists(collectionName)

        if (checkIfTableExists(collectionName)) {
            logger.warn("Table already exists in hbase for collection",
                "table_name" to collectionName,
                "region_capacity" to regionCapacity.toString()
            )
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

    @ExperimentalTime
    private suspend fun createHbaseTable(collectionName: String, regionCapacity: Int, splits: List<ByteArray>) {
        try {
            logger.info(
                "Creating Hbase table",
                "table_name" to collectionName,
                "region_capacity" to regionCapacity.toString()
            )

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


            if (splits.isNotEmpty()) {
                logger.info("Creating table ansynchronoulsy", "table" to "$hbaseTable", "splits" to "${splits.size}")
                hbaseConnection.admin.createTableAsync(hbaseTable, splits.toTypedArray())
            }
            else {
                logger.info("No splits, creating table synchronously", "table" to "$hbaseTable", "splits" to "${splits.size}")
                hbaseConnection.admin.createTable(hbaseTable)
            }

            while (!hbaseConnection.admin.isTableAvailable(hbaseTableName)) {
                logger.info("Waiting for table to be available", "table" to "$hbaseTableName")
                delay(10.seconds)
            }

            logger.info("Created Hbase table","table_name" to collectionName,
                "region_capacity" to regionCapacity.toString())

        } catch (e: TableExistsException) {
            logger.warn(
                "Exception caught when attempting to create Hbase table",
                "table_name" to collectionName,
                "region_capacity" to regionCapacity.toString()
            )
        }
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
        val logger = DataworksLogger.getLogger(HbaseTableCreatorServiceImpl::class.toString())
    }
}
