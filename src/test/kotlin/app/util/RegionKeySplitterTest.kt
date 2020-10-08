package app.util

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
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

    @Test
    fun splitsAreEquallySized() {
        for (regionCount in 1 .. 10_000) {
            val result = calculateSplits(regionCount)
            val expectedSize = (256 * 256) / regionCount
            val remainder = (256 * 256) % regionCount
            gaps(result).run {
                indices.forEach {
                    assertEquals(expectedSize + if (it < remainder) 1 else 0, this[it])
                }
            }
        }
    }

    private fun gaps(result: List<ByteArray>) =
            result.indices.map {
                val position1 = if (it == 0) 0 else position(result[it - 1])
                val position2 = position(result[it])
                position2 - position1
            }

    private fun position(digits: ByteArray): Int = toUnsigned(digits[0]) * 256 + toUnsigned(digits[1])
    private fun toUnsigned(byte: Byte): Int = if (byte < 0) (byte + 256) else byte.toInt()
}
