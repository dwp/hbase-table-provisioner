package app.configuration

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Configuration
@ConfigurationProperties(prefix = "s3")
data class S3Configuration(
        var clientRegion: String? = "NOT_SET",
        var maxAttempts: String? = "NOT_SET",
        var initialBackoffMillis: String? = "NOT_SET",
        var backoffMultiplier: String? = "NOT_SET") {

    @Bean
    @Profile("AWS_S3")
    fun s3Client(): AmazonS3 {
        logger.info("Connecting to AWS S3",
            "region" to clientRegion!!,
            "max_attempts" to "${maxAttempts()}",
            "initial_backoff_millis" to "${initialBackoffMillis()}",
            "backoff_multiplier" to "${backoffMultiplier()}",
        )
        val region = clientRegion!!.toLowerCase().replace("_", "-")
        val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard()
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .withRegion(Regions.fromName(region))
                .build()

        logger.info("Connected to AWS S3 Client", "region" to region)

        return s3Client
    }

    @Bean
    @Qualifier("maxAttempts")
    fun maxAttempts() = maxAttempts?.toInt()

    @Bean
    @Qualifier("initialBackoffMillis")
    fun initialBackoffMillis() = initialBackoffMillis?.toLong()

    @Bean
    @Qualifier("backoffMultiplier")
    fun backoffMultiplier() = backoffMultiplier?.toLong()

    companion object {
        val logger = DataworksLogger.getLogger(S3Configuration::class.toString())
    }
}
