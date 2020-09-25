package app.configuration

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration


@Configuration
@ConfigurationProperties(prefix = "collection.s3")
data class CollectionS3Configuration(
    var bucket: String? = "NOT_SET",
    var path: String? = "NOT_SET"
) {

    @Bean
    fun s3Client() {
        val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard()
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .withRegion(Regions.valueOf(clientRegion))
                .build()
    }
}