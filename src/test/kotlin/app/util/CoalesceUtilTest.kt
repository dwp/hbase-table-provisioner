package app.util

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class CoalesceUtilTest {

    @Test
    fun confirmCoalesceReturnsCorrectionCollectionName() {

        val topicName = "accepted-data.UpdateMongoLock_acceptedDataService"
        val expected = "accepted_data:UpdateMongoLock_acceptedDataService"

        val result = CoalescingUtil().coalescedCollection(topicName)

        assertThat(result).isEqualTo(expected)
    }

    @Test
    fun confirmCoalesceReturnsCorrectionCollectionNameForArchive() {

        val topicName = "agent_core.agentToDoArchive"
        val expected = "agent_core:agentToDo"

        val result = CoalescingUtil().coalescedCollection(topicName)

        assertThat(result).isEqualTo(expected)
    }
}
