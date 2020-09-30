package app.util

fun calculateSplits(numberRequired: Int) : List<ByteArray> {
    val space = 256 * 256
    val size = space / numberRequired
    var remainder = space % numberRequired
    val positions = mutableListOf<Int>()
    var previous = 0
    for (split in 0 until numberRequired - 1) {
        val next = previous + size + (if (remainder-- > 0) 1 else 0)
        positions.add(next)
        previous = next
    }
    return positions.map {byteArrayOf((it / 256).toByte(), (it % 256).toByte())}
}
