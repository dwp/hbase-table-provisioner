package util

import app.util.getCollectionFromPath
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class TextUtilTest {

    @Test
    fun shouldGetCollectionFromPath() {

        val path = "s3://bucket/path/collection"
        val expected = "collection"

        val result = getCollectionFromPath(path)

        assertThat(result).isEqualTo(expected)
    }
}