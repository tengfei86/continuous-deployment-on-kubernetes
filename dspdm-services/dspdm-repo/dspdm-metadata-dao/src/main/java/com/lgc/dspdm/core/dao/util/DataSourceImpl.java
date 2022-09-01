package com.lgc.dspdm.core.dao.util;

import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.sql.Connection;

public interface DataSourceImpl {
    public Connection createNewConnection(ExecutionContext executionContext);
}
