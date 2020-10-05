package app.configuration

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties(prefix = "collections")
data class CollectionsS3Configuration(
        var inputBucket: String? = "NOT_SET",
        var inputBasePath: String? = "NOT_SET",
        var prefixPaths: String? = "NOT_SET",
        var filenameFormatRegexPattern: String? = "NOT_SET",
        var filenameFormatDataExtensionPattern: String? = "NOT_SET",
        var nameRegexPattern: String? = "NOT_SET"
) {

    @Bean
    fun inputBucket() = inputBucket

    @Bean
    fun inputBasePath() = inputBasePath

    @Bean
    fun prefixPaths() = prefixPaths?.split(",")

    @Bean
    fun filenameFormatRegexPattern() = filenameFormatRegexPattern

    @Bean
    fun filenameFormatDataExtensionPattern() = filenameFormatDataExtensionPattern

    @Bean
    fun nameRegexPattern() = nameRegexPattern
}
