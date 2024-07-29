package org.camunda.community.benchmarks;

import java.time.Duration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableAsync
//@ZeebeDeployment(resources = "classpath:bpmn/typical_process.bpmn")
class BenchmarkApplication  {

    public static void main(String[] args) {
        SpringApplication.run(BenchmarkApplication.class, args);
    }

    @Bean
    public CommandLineRunner scheduleTaskStat(StatisticsCollector statisticsCollector,
        @Qualifier("taskScheduler") TaskScheduler taskScheduler) {

        return args -> {
            Runnable task = statisticsCollector::printStatus;
            taskScheduler.scheduleAtFixedRate(task, Duration.ofMillis(60 * 1000)); // Interval in milliseconds
        };
    }
}
