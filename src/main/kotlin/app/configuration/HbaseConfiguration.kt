package app.configuration

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import uk.gov.dwp.dataworks.logging.DataworksLogger
import kotlin.time.ExperimentalTime
import kotlin.time.hours

@Configuration
@ConfigurationProperties(prefix = "hbase")
data class HBaseConfiguration @ExperimentalTime constructor(
        var zookeeperParent: String? = "NOT_SET",
        var zookeeperQuorum: String? = "NOT_SET",
        var zookeeperPort: String? = "NOT_SET",
        var clientScannerTimeoutPeriodMilliseconds: String? = "NOT_SET",
        var clientOperationTimeoutMilliseconds: String? = "NOT_SET",
        var rpcReadTimeoutMilliseconds: String? = "NOT_SET",
        var retries: String? = "NOT_SET",
        var columnFamily: String? = "NOT_SET",
        var columnQualifier: String? = "NOT_SET",
        var coalesceCollectionRegexPattern: String? = "NOT_SET",
        var regionReplicationCount: String? = "NOT_SET",
        var regionTargetSize: String? = "NOT_SET",
        var regionServerCount: String? = "NOT_SET",
        var chunkSize: String? = "NOT_SET",
        var creationTimeoutSeconds: Int = 1.hours.inSeconds.toInt(),
        var largeTableThreshold: Int = 500) {

    fun hbaseConfiguration(): org.apache.hadoop.conf.Configuration {

        val configuration = org.apache.hadoop.conf.Configuration().apply {
            set(HConstants.ZOOKEEPER_ZNODE_PARENT, zookeeperParent!!)
            set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum!!)
            setInt("hbase.zookeeper.port", zookeeperPort!!.toInt())
            setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, clientScannerTimeoutPeriodMilliseconds!!.toInt())
            setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, clientOperationTimeoutMilliseconds!!.toInt())
            setInt(HConstants.HBASE_RPC_READ_TIMEOUT_KEY, rpcReadTimeoutMilliseconds!!.toInt())
            setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, retries!!.toInt())
        }

        logger.info("HBase Configuration",
            "ZOOKEEPER_ZNODE_PARENT" to zookeeperParent!!,
            "ZOOKEEPER_QUORUM" to  zookeeperQuorum!!,
            "HBASE_ZOOKEEPER_PORT" to  zookeeperPort!!,
            "HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD" to  clientScannerTimeoutPeriodMilliseconds!!,
            "HBASE_CLIENT_OPERATION_TIMEOUT" to  clientOperationTimeoutMilliseconds!!,
            "HBASE_RPC_READ_TIMEOUT_KEY" to  rpcReadTimeoutMilliseconds!!,
            "HBASE_CLIENT_RETRIES_NUMBER" to  retries!!)
        logger.info("HBase Configuration loaded")

        return configuration
    }

    @Bean
    fun hbaseConnection(): Connection {

        logger.info("Establishing connection with HBase")

        val configuration = hbaseConfiguration()

        logger.info("Hbase connection configuration",
                HConstants.ZOOKEEPER_ZNODE_PARENT to configuration.get(HConstants.ZOOKEEPER_ZNODE_PARENT),
                HConstants.ZOOKEEPER_QUORUM to configuration.get(HConstants.ZOOKEEPER_QUORUM),
                "hbase.zookeeper.port" to configuration.get("hbase.zookeeper.port"))

        val connection = ConnectionFactory.createConnection(HBaseConfiguration.create(configuration))
        addShutdownHook(connection)

        logger.info("Established connection with HBase")

        return connection
    }

    private fun addShutdownHook(connection: Connection) {
        logger.info("Adding HBase shutdown hook")
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                logger.info("HBase shutdown hook running - closing connection")
                connection.close()
            }
        })
        logger.info("Added HBase shutdown hook")
    }

    @Bean
    fun columnFamily() = columnFamily!!

    @Bean
    fun columnQualifier() = columnQualifier!!

    @Bean
    fun regionReplicationCount() = regionReplicationCount!!.toInt()

    @Bean
    fun regionTargetSize() = regionTargetSize!!.toInt()

    @Bean
    fun regionServerCount() = regionServerCount!!.toInt()

    @Bean
    fun creationTimeoutSeconds() = creationTimeoutSeconds

    @Bean
    fun largeTableThreshold() = largeTableThreshold

    @Bean
    fun chunkSize() = chunkSize!!.toInt()

    companion object {
        val logger = DataworksLogger.getLogger(HBaseConfiguration::class.toString())
    }
}
