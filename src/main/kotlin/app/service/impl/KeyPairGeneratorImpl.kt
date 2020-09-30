package app.service.impl

import app.domain.KeyPair
import app.service.KeyPairGeneratorService
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Service
class KeyPairGeneratorImpl : KeyPairGeneratorService {

    override fun generateKeyPairs(keys: List<String>, fileFormat: Regex, dataFileExtension: Regex): List<KeyPair> {
        val keysMap = keys.groupBy {
            fileFormat.find(it)?.value
        }

        val (unMatched, matched) = keysMap.map { it }.partition { it.key == null }
        val unMatchedFlattened = unMatched.flatMap { it.value }

        if (unMatchedFlattened.isNotEmpty()) {
            logger.warn("Found unmatched keys not matching regex", "unmatched_count", "${unMatchedFlattened.count()}",
                    "file_format", fileFormat.pattern, "unmatched_keys", unMatchedFlattened.joinToString(", "))
        }

        val keyPairs = matched.map { pair ->
            logger.info("Found matched key pair", "pair_key", "${pair.key}", "pair_value", "${pair.value}")
            val noDataKey =
                    pair.value.filterNot { ti -> (ti.contains(dataFileExtension)) }
            val dataKey = pair.value.find { ti -> ti.contains(dataFileExtension) }

            if (noDataKey.isNotEmpty()) {
                logger.warn("Found file(s) that matched format but not data file extension", "bad_files", noDataKey.joinToString(", "))
            }

            KeyPair(dataKey)
        }
        return keyPairs.filter { it.dataKey !== null }
    }

    companion object {
        val logger = DataworksLogger.getLogger(S3ReaderServiceImpl::class.toString())
    }
}
