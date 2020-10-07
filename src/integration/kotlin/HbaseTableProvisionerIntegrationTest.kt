import io.kotest.core.spec.style.StringSpec
import kotlinx.coroutines.delay
import uk.gov.dwp.dataworks.logging.DataworksLogger
import kotlin.time.ExperimentalTime
import kotlin.time.seconds

@ExperimentalTime
class HbaseTableProvisionerIntegrationTest : StringSpec() {
    init {
        "Collections are provisioned as tables into Hbase" {
            logger.info(" WIP: Waiting for records to be reconciled")
            delay(1.seconds)
            logger.info(" WIP: Done")
        }
    }

    companion object {
        val logger = DataworksLogger.getLogger(HbaseTableProvisionerIntegrationTest::class.toString())
    }
}
