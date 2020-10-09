import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import uk.gov.dwp.dataworks.logging.DataworksLogger
import kotlin.time.ExperimentalTime
import kotlin.time.minutes
import kotlin.time.seconds

@ExperimentalTime
class HbaseTableProvisionerIntegrationTest : StringSpec() {
    init {
        "Collections are provisioned as tables with expected region splits into HBase" {
            verifyHbase()
        }
    }

    private val expectedTablesToRegions = mapOf(
        "accepted_data:address" to 1,
        "accepted_data:childrenCircumstances" to 1,
        "core:assessmentPeriod" to 5,
        "core:toDo" to 5,
        "crypto:encryptedData" to 88).toSortedMap()

    private fun hbaseConnection(): Connection {
        val host = System.getenv("HBASE_ZOOKEEPER_QUORUM") ?: "localhost"
        val config = Configuration().apply {
            set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase")
            set(HConstants.ZOOKEEPER_QUORUM, host)
            setInt("hbase.zookeeper.port", 2181)
        }
        return ConnectionFactory.createConnection(HBaseConfiguration.create(config))
    }

    private fun testTables(): List<String> {
        val tables = hbaseConnection().admin.listTableNames()
            .map(TableName::getNameAsString)
            .sorted()
        logger.info("...hbase tables", "number" to "${tables.size}", "all_tables" to "$tables")
        return tables
    }

    private suspend fun verifyHbase() {
        var waitSoFarSecs = 0
        val longInterval = 5
        val expectedTablesSorted = expectedTablesToRegions.keys.sorted()
        logger.info("Waiting for ${expectedTablesSorted.size} hbase tables to appear with given regions",
            "expected_tables_sorted" to "$expectedTablesSorted")

        val foundTablesToRegions = mutableMapOf<String, Int>()

        hbaseConnection().use { hbase ->
            withTimeout(10.minutes) {
                do {
                    val foundTablesSorted = testTables()
                    logger.info("Waiting for ${expectedTablesSorted.size} hbase tables to appear",
                        "found_tables_so_far" to "${foundTablesSorted.size}",
                        "total_seconds_elapsed" to "$waitSoFarSecs")
                    delay(longInterval.seconds)
                    waitSoFarSecs += longInterval
                }
                while (expectedTablesSorted.toSet() != foundTablesSorted.toSet())

                testTables().forEach { tableName ->
                    launch(Dispatchers.IO) {
                        val regionsWithReplication = hbase.admin.getTableRegions(TableName.valueOf(tableName)).size
                        logger.info(
                            "Found table",
                            "table_name" to tableName,
                            "regions_with_replication" to "$regionsWithReplication",
                        )
                        foundTablesToRegions[tableName] = regionsWithReplication
                    }
                }
            }
        }
        foundTablesToRegions.toSortedMap() shouldBe expectedTablesToRegions
    }

    companion object {
        val logger = DataworksLogger.getLogger(HbaseTableProvisionerIntegrationTest::class.toString())
    }
}
