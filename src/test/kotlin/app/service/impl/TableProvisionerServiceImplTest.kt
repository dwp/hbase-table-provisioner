package app.service.impl

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class TableProvisionerServiceImplTest {

    @Test
    fun totalRegionMultiplierTest() {
        val service = TableProvisionerServiceImpl()
        val c = service.provisionHbaseTable(2, 3)
        assertThat(c).isEqualTo(6)
    }
}