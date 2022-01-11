package io.ingestr.framework.model;

import java.util.ArrayList;
import java.util.List;

public class ParameterDefinitions {
    private List<ParameterDefinition> parameterDefinitions = new ArrayList<>();

    public static ParameterDefinitions of() {
        return new ParameterDefinitions();
    }

    public ParameterDefinitions define(
            ParameterDefinition parameterDefinition) {
        this.parameterDefinitions.add(parameterDefinition);
        return this;
    }
}
