package io.ingestr.framework.service.db;

import io.ingestr.framework.entities.DataDescriptor;
import io.ingestr.framework.entities.Ingestion;
import io.ingestr.framework.entities.IngestionSchedule;
import io.ingestr.framework.entities.LoaderDefinition;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;

@Slf4j
public class LoaderDefinitionServices {
    private LoaderDefinition loaderDefinition;

    public LoaderDefinitionServices(LoaderDefinition loaderDefinition) {
        this.loaderDefinition = loaderDefinition;
    }

    public LoaderDefinition getLoaderDefinition() {
        return loaderDefinition;
    }

    public void setLoaderDefinition(LoaderDefinition loaderDefinition) {
        this.loaderDefinition = loaderDefinition;
    }


    public void validate() {
        Set<String> ids = new HashSet<>();
        Set<String> scheduleIds = new HashSet<>();

        for (Ingestion ing : this.loaderDefinition.getIngestions()) {
            if (ids.contains(ing.getIdentifier())) {
                throw new IllegalStateException("Ingestion id " + ing.getIdentifier() + " is already taken");
            }
            ids.add(ing.getIdentifier());

            for (IngestionSchedule is : ing.getIngestionSchedules()) {
                if (scheduleIds.contains(is.getIdentifier())) {
                    throw new IllegalStateException("Schedule Id " + is.getIdentifier() + " is already taken");
                }
                scheduleIds.add(is.getIdentifier());
            }
        }

        ids.clear();
        for (DataDescriptor dd : this.loaderDefinition.getDataDescriptors()) {
            if (ids.contains(dd.getIdentifier())) {
                throw new IllegalStateException("Data Descriptor id " + dd.getIdentifier() + " is already taken");
            }
            ids.add(dd.getIdentifier());
        }
    }
}
