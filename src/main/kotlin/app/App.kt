package app

import app.service.impl.TableProvisionerServiceImpl
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.retry.annotation.EnableRetry
import kotlin.time.ExperimentalTime

@ConfigurationPropertiesScan
@SpringBootApplication
@EnableRetry
class App(private val service: TableProvisionerServiceImpl) : CommandLineRunner {
    @ExperimentalTime
    override fun run(vararg args: String?) {
        service.provisionHbaseTable()
    }
}

fun main(args: Array<String>) {
    runApplication<App>(*args)
}
