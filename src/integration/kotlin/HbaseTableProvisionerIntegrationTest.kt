import io.kotest.core.spec.style.StringSpec
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
import org.assertj.core.api.Assertions.assertThat
import uk.gov.dwp.dataworks.logging.DataworksLogger
import kotlin.time.ExperimentalTime
import kotlin.time.minutes
import kotlin.time.seconds

@ExperimentalTime
class HbaseTableProvisionerIntegrationTest : StringSpec() {
    init {
        "Collections are provisioned as tables into Hbase" {
            verifyHbase()
        }
    }

    private val numberRegionReplicas = 3
    private val expectedTablesAndRegions = mapOf(
        "accepted_data:address" to 3, //1
        "accepted_data:childrenCircumstances" to 3, //1
        "core:assessmentPeriod" to 15, //5
        "core:toDo" to 15, //5
        "crypto:encryptedData" to 264) //88

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
        logger.info("...hbase tables", "tables_found" to "${tables.size}", "all_table_names" to "$tables")
        return tables
    }

    private suspend fun verifyHbase() {
        var waitSoFarSecs = 0
        val longInterval = 10
        val expectedTablesSorted = expectedTablesAndRegions.toSortedMap()
        logger.info("Waiting for ${expectedTablesSorted.size} hbase tables to appear with given regions",
            "expected_tables_sorted" to "$expectedTablesSorted")

        val foundTablesWithRegions = mutableMapOf<String, Int>()

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
                while (expectedTablesSorted.keys.toSet() != foundTablesSorted.toSet())

                testTables().forEach { tableName ->
                    launch(Dispatchers.IO) {
                        val tableRegions = hbaseConnection().admin.getTableRegions(TableName.valueOf(tableName))
                        val numberRegionsWithReplicas = tableRegions.size
                        logger.info("Found table",
                            "table_name" to tableName,
                            "number_regions_with_replicas" to "$numberRegionsWithReplicas"
                        )

                        tableRegions.forEach { regionInfo ->
                            logger.info(
                                "Found table region",
                                "table_name" to tableName,
                                "region_id" to "$regionInfo.regionId",
                            )
                        }

                        //numberRegionReplicas

//                    val numberRegions = tableRegions
//                        .filter {
//                            //only region replica 0, which might have this set to null
//                            it.regionId == 0L
//                        }.size
//
//                    foundTablesWithRegions[tableName] = numberRegions
//                    logger.info("Found table",
//                        "table_name" to tableName,
//                        "number_regions" to "$numberRegions",
//                        "number_regions_with_replicas" to "$numberRegionsWithReplicas",
//                        "found_replicas" to "${numberRegionsWithReplicas/numberRegions}"
//                    )
                    }
                }
            }
        }

        assertThat(foundTablesWithRegions.toSortedMap()).isEqualTo(expectedTablesSorted)
    }

    companion object {
        val logger = DataworksLogger.getLogger(HbaseTableProvisionerIntegrationTest::class.toString())
    }
}
