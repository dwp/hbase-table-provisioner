package app.service.impl

import app.service.HbaseTableCreatorService
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.hadoop.hbase.*
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.io.compress.Compression
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.logging.DataworksLogger
import kotlin.time.*

@Service
class HbaseTableCreatorServiceImpl(
    private val hbaseConnection: Connection,
    private val columnFamily: String,
    private val regionReplicationCount: Int,
    private val creationTimeoutSeconds: Int): HbaseTableCreatorService {

    @ExperimentalTime
    override suspend fun createHbaseTableFromProps(collectionName: String, splits: List<ByteArray>) {
        try {
            ensureNamespaceExists(collectionName)
            if (checkIfTableExists(collectionName)) {
                logger.warn("Table already exists in hbase for collection",
                    "table_name" to collectionName)
            } else {
                createHbaseTable(collectionName, splits)
            }
        } catch (e: Exception) {
            logger.error("Failed to create table", e, "collection_name" to collectionName)
        }
    }

    fun ensureNamespaceExists(collectionName: String) {
        var namespace = ""
        try {
            val dataTableName = TableName.valueOf(collectionName)
            namespace = dataTableName.namespaceAsString
            if (!namespaces.contains(namespace)) {
                logger.info("Creating namespace", "namespace" to namespace)
                hbaseConnection.admin.createNamespace(NamespaceDescriptor.create(namespace).build())
            }
        } catch (e: NamespaceExistException) {
            logger.info("Namespace already exists, probably created by another process", "namespace" to namespace)
        } finally {
            if (namespace.isNotBlank()) {
                namespaces[namespace] = true
            }
        }
    }

    @ExperimentalTime
    private suspend fun createHbaseTable(collectionName: String, splits: List<ByteArray>) {
        try {
            logger.info("Creating Hbase table",
                "table_name" to collectionName)

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

            withTimeoutOrNull(creationTimeoutSeconds.seconds) {
                val duration = measureTime {
                    if (splits.isNotEmpty()) {
                        logger.info("Creating table asynchronously", "table" to "$hbaseTable",
                            "splits" to "${splits.size}", "operation_timeout" to "${creationTimeoutSeconds.seconds}")
                        hbaseConnection.admin.createTableAsync(hbaseTable, splits.toTypedArray())
                    } else {
                        logger.info("No splits, creating table synchronously",
                            "table" to "$hbaseTable",
                            "splits" to "${splits.size}",
                            "operation_timeout" to "${creationTimeoutSeconds.seconds}")
                        hbaseConnection.admin.createTable(hbaseTable)
                    }

                    while (!hbaseConnection.admin.isTableAvailable(hbaseTableName)) {
                        logger.info("Waiting for table to be available", "table" to "$hbaseTableName",
                            "operation_timeout" to "${creationTimeoutSeconds.seconds}")
                        delay(5.seconds)
                    }
                }

                logger.info("Created Hbase table", "table_name" to collectionName,
                    "duration" to "$duration",
                    "operation_timeout" to "${creationTimeoutSeconds.seconds}")
            }

        } catch (e: Exception) {
            logger.error("Exception caught when attempting to create Hbase table", e,
                "table_name" to collectionName)
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
