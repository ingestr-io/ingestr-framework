package io.ingestr.framework.service.queue;

import io.ingestr.framework.service.db.LoaderDefinitionServices;
import io.ingestr.framework.service.queue.model.IngestPartitionQueueItem;
import io.ingestr.framework.service.queue.model.QueuedResponse;
import io.ingestr.framework.service.workers.tasks.IngestionTask;
import io.micronaut.context.ApplicationContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class QueueExecutor extends Thread {
    private final QueueConsumer queueConsumer;
    private final ExecutorService executorService;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final LoaderDefinitionServices loaderDefinitionServices;

    private final ApplicationContext applicationContext;


    public QueueExecutor(QueueConsumer queueConsumer,
                         LoaderDefinitionServices loaderDefinitionServices,
                         ApplicationContext applicationContext) {

        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("task-executor-thread-%d")
                .daemon(true)
                .priority(Thread.NORM_PRIORITY)
                .build();

        this.queueConsumer = queueConsumer;
        this.executorService = Executors.newFixedThreadPool(loaderDefinitionServices.getLoaderDefinition().getLoaderConfiguration().getConcurrency(), factory);
        this.loaderDefinitionServices = loaderDefinitionServices;
        this.applicationContext = applicationContext;
    }

    public void shutdown() {
        this.shutdown.set(true);
    }

    public void run() {

        while (!shutdown.get()) {

            //fetch more if the queue size is getting low
            if (queueSize() >= loaderDefinitionServices.getLoaderDefinition().getLoaderConfiguration().getConcurrency()) {
                try {
                    Thread.sleep(100);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
                continue;
            }

            List<QueuedResponse> qrs = queueConsumer.receive();

            for (QueuedResponse qr : qrs) {
                log.debug("Executing Task - {}", qr);
                if (qr.getQueueItem() instanceof IngestPartitionQueueItem) {
                    IngestionTask it = applicationContext.createBean(IngestionTask.class);
                    it.setIngestPartitionQueueItem((IngestPartitionQueueItem) qr.getQueueItem());
                    executorService.submit(it);
                }
            }
        }
    }

    int queueSize() {
        return ((ThreadPoolExecutor) executorService).getQueue().size();
    }

}
