package com.lgc.dspdm.repo.delegate.metadata.bosearch;

import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.util.ArrayList;
import java.util.Collection;

public class BOSearchValuesList<Object> extends ArrayList<Object> {
    private ExecutionContext executionContext = null;

    public BOSearchValuesList(Collection<Object> values, ExecutionContext executionContext) {
        super(values);
        this.executionContext = executionContext;
    }

    public ExecutionContext getExecutionContext() {
        return executionContext;
    }
}
