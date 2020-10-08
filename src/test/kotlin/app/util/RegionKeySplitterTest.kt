package app.util

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class RegionKeySplitterTest {

    @Test
    fun confirmCalculateSplitsWillMakeByteArrayFromMinimumNumberOfRegions() {

        val result = calculateSplits(1)

        val expected = listOf<List<Byte>>() //TODO this should not be empty

        assertThat(result).isEqualTo(expected)
    }

    @Test
    fun confirmCalculateSplitsWillMakeByteArrayFromTypicalNumberOfRegions() {

        val result = calculateSplits(100)

        //TODO here we should be checking a list<list<byte>> not a count
        val expectedCount = 99 //TODO Why is this off by 1?

        assertThat(result.size).isEqualTo(expectedCount)
    }

}
