package io.ingestr.framework.repositories;

import io.ingestr.framework.IngestrFunctions;
import io.ingestr.framework.entities.Partition;
import io.ingestr.framework.entities.PartitionEntry;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.jupiter.api.Test;

@Slf4j
@MicronautTest
class PartitionRepositoryTest {
    @Inject
    PartitionRepository pr;

    @Test
    void shouldSaveVeryQuicklyLots() throws InterruptedException {
        StopWatch sw = new StopWatch();
        sw.start();
        for (int x = 0; x < 100_000; x++) {
            Partition partition = IngestrFunctions.newPartition()
                    .partitionEntry(PartitionEntry.newEntry("pk", "val" + x))
                    .dataDescriptorIdentifier("desc_id")
                    .build();
            pr.save(partition);
        }
        sw.stop();

        log.info("Took - {}ms", sw.getTime());
        Thread.sleep(100);

        //then load it up
//        pr = new PartitionRepository(
//                kps
//        );
//        kps.load();
//        kps.waitInitialise();
//
//        List<Partition> partitions = pr.findAll();
//        assertEquals(100_000, partitions.size());
    }
}