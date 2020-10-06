import io.kotest.core.spec.style.StringSpec
import kotlinx.coroutines.delay
import kotlin.time.ExperimentalTime
import kotlin.time.seconds

@ExperimentalTime
class HbaseTableProvisionerIntegrationTest : StringSpec() {
    init {
        "Collections are provisioned as tables into Hbase" {
//            logger.info("Waiting for records to be reconciled")
            delay(1.seconds)
        }
    }
}
