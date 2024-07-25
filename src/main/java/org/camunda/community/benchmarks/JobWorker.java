package org.camunda.community.benchmarks;

import java.time.Instant;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.camunda.community.benchmarks.config.BenchmarkConfiguration;
import org.camunda.community.benchmarks.refactoring.RefactoredCommandWrapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.command.CompleteJobCommandStep1;
import io.camunda.zeebe.client.api.command.FinalCommandStep;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.client.api.worker.JobHandler;
import io.camunda.zeebe.client.api.worker.JobWorkerBuilderStep1;
import io.camunda.zeebe.spring.client.exception.ZeebeBpmnError;
import io.camunda.zeebe.spring.client.jobhandling.CommandWrapper;

@Component
public class JobWorker implements ApplicationListener<ContextRefreshedEvent> {
    private static final Logger LOG = LogManager.getLogger(JobWorker.class);

    private final BenchmarkConfiguration config;

    private final BenchmarkCompleteJobExceptionHandlingStrategy exceptionHandlingStrategy;

    // TODO: Check if we can/need to check if the scheduler can catch up with all its work (or if it is overwhelmed)
    private final TaskScheduler scheduler;

    private final ZeebeClient client;

    private final StatisticsCollector stats;

    public JobWorker(BenchmarkConfiguration config,
        BenchmarkCompleteJobExceptionHandlingStrategy exceptionHandlingStrategy,
        @Qualifier("taskScheduler") TaskScheduler scheduler, ZeebeClient client, StatisticsCollector stats) {
        this.config = config;
        this.exceptionHandlingStrategy = exceptionHandlingStrategy;
        this.scheduler = scheduler;
        this.client = client;
        this.stats = stats;
    }

    private void registerWorker(String jobType) {

        long fixedBackOffDelay = config.getFixedBackOffDelay();

        JobWorkerBuilderStep1.JobWorkerBuilderStep3 step3 = client.newWorker()
                .jobType(jobType)
                .handler(new SimpleDelayCompletionHandler(true))
                .name(jobType);

        if(fixedBackOffDelay > 0) {
            step3.backoffSupplier(new FixedBackoffSupplier(fixedBackOffDelay));
        }

        step3.open();
    }

    // Don't do @PostConstruct as this is too early in the Spring lifecycle
    //@PostConstruct
    public void startWorkers() {
        String taskType = config.getJobType();

        String[] jobs = null;
        if (taskType.contains(","))
            jobs = taskType.split(",");
        boolean startWorkers = config.isStartWorkers();
        int numberOfJobTypes = config.getMultipleJobTypes();

        if(startWorkers) {
            //if the job types are not listed out then generate the jobtypes automatically based on the multipleJobTypes
            //other wise loop through the list of jobTypes and create
            if(jobs==null) {
                if (numberOfJobTypes <= 0) {
                    registerWorkersForTaskType(taskType);
                } else {
                    for (int i = 0; i < numberOfJobTypes; i++) {
                        registerWorkersForTaskType(taskType + "-" + (i + 1));
                    }
                }
            } else {
                for (int n = 0; jobs.length > n; n++) {
                    registerWorkersForTaskType(jobs[n]);
                }
            }
        }
    }

    private void registerWorkersForTaskType(String taskType) {
        // worker for normal task type
        registerWorker(taskType);

        // worker for normal "task-type-{starterId}"
        registerWorker(taskType + "-" + config.getStarterId());

        // worker marking completion of process instance via "task-type-complete"
        registerWorker(taskType + "-completed");

        // worker marking completion of process instance via "task-type-complete"
        registerWorker(taskType + "-" + config.getStarterId() + "-completed");
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        startWorkers();
    }

    public class SimpleDelayCompletionHandler implements JobHandler {

        private boolean markProcessInstanceCompleted;

        public SimpleDelayCompletionHandler(boolean markProcessInstanceCompleted) {
            this.markProcessInstanceCompleted = markProcessInstanceCompleted;
        }

        @Override
        public void handle(JobClient jobClient, ActivatedJob job) throws Exception {
            // Auto-complete logic from https://github.com/camunda-community-hub/spring-zeebe/blob/ec41c5af1f64e512c8e7a8deea2aeacb35e61a16/client/spring-zeebe/src/main/java/io/camunda/zeebe/spring/client/jobhandling/JobHandlerInvokingSpringBeans.java#L24
            CompleteJobCommandStep1 completeCommand = jobClient.newCompleteCommand(job.getKey());
            CommandWrapper command = new RefactoredCommandWrapper(
                    (FinalCommandStep) completeCommand,
                    job.getDeadline(),
                    job.toString(),
                    exceptionHandlingStrategy);
            long delay = config.getTaskCompletionDelay();
            LOG.debug("Worker {} will complete in {} MS", job.getType(), delay);
            // schedule the completion asynchronously with the configured delay
            scheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    try {
                        command.executeAsync();
                        stats.incCompletedJobs();
                        if (markProcessInstanceCompleted) {
                            Object startEpochMillis = job.getVariablesAsMap().get(StartPiExecutor.BENCHMARK_START_DATE_MILLIS);
                            if (startEpochMillis!=null && startEpochMillis instanceof Long) {
                                stats.incCompletedProcessInstances((Long)startEpochMillis, Instant.now().toEpochMilli());
                            } else {
                                stats.incCompletedProcessInstances();
                            }
                        }
                    }
                    catch (ZeebeBpmnError bpmnError) {
                        CommandWrapper command = new RefactoredCommandWrapper(
                                createThrowErrorCommand(jobClient, job, bpmnError),
                                job.getDeadline(),
                                job.toString(),
                                exceptionHandlingStrategy);
                        command.executeAsync();
                    }
                }
            }, Instant.now().plusMillis(delay));
        }
    }

    private FinalCommandStep<Void> createThrowErrorCommand(JobClient jobClient, ActivatedJob job, ZeebeBpmnError bpmnError) {
        FinalCommandStep<Void> command = jobClient.newThrowErrorCommand(job.getKey()) // TODO: PR for taking a job only in command chain
                .errorCode(bpmnError.getErrorCode())
                .errorMessage(bpmnError.getErrorMessage());
        return command;
    }
}
