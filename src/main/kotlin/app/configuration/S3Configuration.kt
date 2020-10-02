package app.configuration

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Configuration
@ConfigurationProperties(prefix = "s3")
class S3Configuration(
        var clientRegion: String? = "NOT_SET",
        var maxAttempts: String? = "NOT_SET",
        var initialBackoffMillis: String? = "NOT_SET",
        var backoffMultiplier: String? = "NOT_SET") {


    @Bean
    fun s3Client(): AmazonS3 {
        logger.info("Retrieving AWS S3 Client", "region" to clientRegion!!)

        val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard()
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .withRegion(Regions.valueOf(clientRegion!!))
                .build()

        logger.info("Retrieved AWS S3 Client", "region" to clientRegion!!)

        return s3Client
    }

    @Bean
    fun maxAttempts() = maxAttempts?.toIntOrNull()

    @Bean
    fun initialBackoffMillis() = initialBackoffMillis?.toLongOrNull()

    @Bean
    fun backoffMultiplier() = backoffMultiplier?.toLongOrNull()

    companion object {
        val logger = DataworksLogger.getLogger(S3Configuration::class.toString())
    }
}