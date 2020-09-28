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
@ConfigurationProperties(prefix = "collection.s3")
data class CollectionS3Configuration(
    var bucket: String? = "NOT_SET",
    var path: String? = "NOT_SET",
    var clientRegion: String? = "NOT_SENT"
) {

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
    fun bucket() = bucket

    @Bean
    fun path() = path

    companion object {
        val logger = DataworksLogger.getLogger(CollectionS3Configuration::class.toString())
    }

}
