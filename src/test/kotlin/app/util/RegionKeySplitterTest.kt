package app.util

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class RegionKeySplitterTest {

    @Test
    fun splitsAreEquallySized() {
        val keyspaceSize = 256 * 256
        for (regionCount in 1 .. 10_000) {
            calculateSplits(regionCount).let { splits ->
                assertEquals(regionCount - 1, splits.size)
                val expectedSize = keyspaceSize / regionCount
                val remainder = keyspaceSize % regionCount
                gaps(splits).let { gaps ->
                    gaps.indices.forEach { index ->
                        assertEquals(expectedSize + if (index < remainder) 1 else 0, gaps[index])
                    }
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
