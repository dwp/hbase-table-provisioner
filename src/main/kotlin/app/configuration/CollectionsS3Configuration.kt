package app.configuration

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import uk.gov.dwp.dataworks.logging.DataworksLogger


@Configuration
@ConfigurationProperties(prefix = "collections")
data class CollectionsS3Configuration(
        var inputBucket: String? = "NOT_SET",
        var inputBasePath: String? = "NOT_SET",
        var collectionPaths: String? = "NOT_SET",
        var filenameFormatRegexPattern: String? = "NOT_SET",
        var filenameFormatDataExtensionPattern: String? = "NOT_SET",
        var nameRegexPattern: String? = "NOT_SET"
) {

    @Bean
    fun bucket() = inputBucket

    @Bean
    fun basePath() = inputBasePath

    @Bean
    fun collectionPaths() = collectionPaths?.split(",")

    @Bean
    fun filenameFormatRegexPattern() = filenameFormatRegexPattern

    @Bean
    fun filenameFormatDataExtensionPattern() = filenameFormatDataExtensionPattern

    @Bean
    fun collectionNameRegexPattern() = nameRegexPattern

    companion object {
        val logger = DataworksLogger.getLogger(CollectionsS3Configuration::class.toString())
    }
}
