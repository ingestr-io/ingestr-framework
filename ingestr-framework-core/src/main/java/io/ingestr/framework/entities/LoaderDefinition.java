package io.ingestr.framework.entities;

import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class LoaderDefinition {
    private String loaderName;
    private String loaderVersion;
    private List<DataDescriptor> dataDescriptors = new ArrayList<>();
    private List<Ingestion> ingestions = new ArrayList<>();
    private List<ConfigurationDescriptor> configurations = new ArrayList<>();

    private LoaderConfiguration loaderConfiguration;

    public LoaderDefinition(String loaderName, String loaderVersion) {
        this.loaderName = loaderName;
        this.loaderVersion = loaderVersion;
    }

    public String getLoaderName() {
        return loaderName;
    }

    public List<DataDescriptor> getDataDescriptors() {
        return dataDescriptors;
    }

    public List<Ingestion> getIngestions() {
        return ingestions;
    }

    public List<ConfigurationDescriptor> getConfigurations() {
        return configurations;
    }


    public LoaderConfiguration getLoaderConfiguration() {
        return loaderConfiguration;
    }


    public void setLoaderConfiguration(LoaderConfiguration loaderConfiguration) {
        this.loaderConfiguration = loaderConfiguration;
    }


    public Optional<Ingestion> findByIngestionId(String ingestionId) {
        return ingestions.stream()
                .filter(i -> StringUtils.equalsIgnoreCase(i.getIdentifier(), ingestionId))
                .findFirst();
    }

    public Optional<DataDescriptor> findByDescriptorId(String identifier) {
        return dataDescriptors.stream()
                .filter(d -> StringUtils.equalsIgnoreCase(d.getIdentifier(), identifier))
                .findFirst();
    }


    public Optional<IngestionSchedule> findByScheduleId(String scheduleId) {
        for (Ingestion i : this.ingestions) {
            for (IngestionSchedule s : i.getIngestionSchedules()) {
                if (StringUtils.equalsIgnoreCase(s.getIdentifier(), scheduleId)) {
                    return Optional.of(s);
                }
            }
        }
        return Optional.empty();
    }

    public Map<String, String> getConfigurationAsMap() {
        Map<String, String> cfgs = new HashMap<>();
        for (ConfigurationDescriptor cfg : configurations) {
            String val = null;
            if (val == null) {
                val = cfg.getDefaultValue();
            }
            cfgs.put(cfg.getIdentifier(), val);
        }
        return cfgs;
    }

}
