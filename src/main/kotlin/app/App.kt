package app

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.retry.annotation.EnableRetry
import kotlin.system.exitProcess

@EnableRetry
@SpringBootApplication
@ConfigurationPropertiesScan
class App

fun main(args: Array<String>) {
    exitProcess(SpringApplication.exit(runApplication<App>(*args)))
}
