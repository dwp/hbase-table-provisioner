package app.configuration

import com.amazonaws.ClientConfiguration
import com.amazonaws.Protocol
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Configuration
@Profile("LOCAL_S3")
class S3DummyConfiguration {

    @Bean
    fun amazonS3(): AmazonS3 {

        logger.info("Connecting to Dummy S3",
                "service_endpoint" to serviceEndpoint,
                "region" to region)

        val s3Client = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration(serviceEndpoint, region))
                .withClientConfiguration(ClientConfiguration().withProtocol(Protocol.HTTP))
                .withCredentials(
                        AWSStaticCredentialsProvider(BasicAWSCredentials(accessKey, secretKey)))
                .withPathStyleAccessEnabled(true)
                .disableChunkedEncoding()
                .build()

        logger.info("Connected to Dummy S3",
                "service_endpoint" to serviceEndpoint,
                "region" to region)

        return s3Client
    }

    @Value("\${s3.client_region}")
    private lateinit var region: String

    @Value("\${s3.service.endpoint:http://aws-s3:4566}")
    private lateinit var serviceEndpoint: String

    @Value("\${aws.access.key.id")
    private lateinit var accessKey: String

    @Value("\$aws.secret.access.key}")
    private lateinit var secretKey: String

    companion object {
        val logger = DataworksLogger.getLogger(S3DummyConfiguration::class.toString())
    }
}
