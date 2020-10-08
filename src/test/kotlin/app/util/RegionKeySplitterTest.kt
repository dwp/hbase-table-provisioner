package app.util

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class RegionKeySplitterTest {

    @Test
    fun confirmCalculateSplitsWillMakeNoSplitsForOneRegion() {

        val result = calculateSplits(1)

        val expected = listOf<Byte>()
        assertThat(result).isEqualTo(expected)
    }

    @Test
    fun confirmCalculateSplitsWillMakeOneLessNumberOfSplitsThanNumberOfRegions() {

        val result = calculateSplits(5)
        assertThat(result.size).isEqualTo(4)

        val humanReadableResult = result.map { element ->
            (element.joinToString("") { string -> String.format("\\x%02X", string) })
        }

        assertThat(humanReadableResult.toString()).isEqualTo("[\\x33\\x34, \\x66\\x67, \\x99\\x9A, \\xCC\\xCD]")
    }

}
