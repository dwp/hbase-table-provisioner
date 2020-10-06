package app.configuration

import org.springframework.beans.factory.annotation.Qualifier
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
    @Qualifier("inputBucket")
    fun inputBucket() = inputBucket

    @Bean
    @Qualifier("inputBasePath")
    fun inputBasePath() = inputBasePath

    @Bean
    @Qualifier("prefixPaths")
    fun prefixPaths() = prefixPaths?.split(",")

    @Bean
    @Qualifier("filenameFormatRegexPattern")
    fun filenameFormatRegexPattern() = filenameFormatRegexPattern

    @Bean
    @Qualifier("filenameFormatDataExtensionPattern")
    fun filenameFormatDataExtensionPattern() = filenameFormatDataExtensionPattern

    @Bean
    @Qualifier("nameRegexPattern")
    fun nameRegexPattern() = nameRegexPattern
}
