package com.lgc.dspdm.core.dao.util.datamodel;

import bitronix.tm.resource.jdbc.PoolingDataSource;
import com.lgc.dspdm.core.common.config.ConnectionProperties;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.AESSecurityUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.dao.util.DataSourceImpl;
import com.microsoft.sqlserver.jdbc.SQLServerXADataSource;
import org.postgresql.xa.PGXADataSource;

import javax.sql.DataSource;
import javax.sql.XADataSource;
import java.sql.Connection;

public class DataModelDataSourceImpl implements DataSourceImpl {
    private static DSPDMLogger logger = new DSPDMLogger(DataModelDataSourceImpl.class);
    private volatile DataSource dataSource = null;
    private boolean readOnly = false;
    private String dbType = null;

    public DataModelDataSourceImpl(String dbType, boolean isReadOnly) {
        this.dbType = dbType;
        this.readOnly = isReadOnly;
    }

    protected boolean isPostgresDialect() {
        return (DSPDMConstants.SQL.SQLDialect.POSTGRES.equalsIgnoreCase(ConnectionProperties.getInstance().data_model_db_sql_dialect.getPropertyValue()));
    }

    protected boolean isMSSQLServerDialect() {
        return (DSPDMConstants.SQL.SQLDialect.MSSQLSERVER.equalsIgnoreCase(ConnectionProperties.getInstance().data_model_db_sql_dialect.getPropertyValue()));
    }

    /**
     * A single interface to create a new connection
     *
     * @param executionContext
     * @return
     */
    @Override
    public Connection createNewConnection(ExecutionContext executionContext) {
        Connection connection = null;
        try {
            // initialize data source if not already initialized
            dataSource = getPooledDataSource(executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        try {
            long startTime = System.currentTimeMillis();
            connection = dataSource.getConnection();
            if (readOnly) {
                connection.setReadOnly(readOnly);
            }
            executionContext.addDBProcessingTime((System.currentTimeMillis() - startTime));
            logger.info("One {} connection took from {} data source successfully. Time taken by connection in millis : {}",
                    (readOnly ? "read only" : "read write"), dbType, (System.currentTimeMillis() - startTime));
        } catch (Exception e) {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception ex) {
            }
            throw new DSPDMException("Unable to get a {} connection from {} data source. Reason : {}",
                    e, executionContext.getExecutorLocale(), (readOnly ? "read only" : "read write"), dbType, e.getMessage());
        }
        return connection;
    }

    private DataSource getPooledDataSource(ExecutionContext executionContext) {
        if (dataSource == null) {
            synchronized (DataModelDataSourceImpl.class) {
                if (dataSource == null) {
                    XADataSource xaDataSource = null;
                    if (isPostgresDialect()) {
                        xaDataSource = getPostgresXADataSource(executionContext);
                    } else if (isMSSQLServerDialect()) {
                        xaDataSource = getMSSQLServerXADataSource(executionContext);
                    } else {
                        throw new DSPDMException("Unknown SQL dialect", executionContext.getExecutorLocale());
                    }
                    try {
                        logger.info("Going to create a {} connection pool for {}", (readOnly ? "read only" : "read write"), dbType);
                        long startTime = System.currentTimeMillis();
                        PoolingDataSource pds = new PoolingDataSource();
                        String uniqueName = "jdbc/dspdm-" + dbType + "-ds-" + ConnectionProperties.getInstance().data_model_db_sql_dialect.getPropertyValue() + "-" + (readOnly ? "readonly" : "readwrite");
                        pds.setUniqueName(uniqueName);
                        if (executionContext.isUnitTestCall()) {
                            pds.setMinPoolSize(2);
                            pds.setMaxPoolSize(10);
                        } else {
                            pds.setMinPoolSize(ConnectionProperties.getInstance().data_model_db_min_connection_pool_size.getIntegerValue());
                            pds.setMaxPoolSize(ConnectionProperties.getInstance().data_model_db_max_connection_pool_size.getIntegerValue());
                        }
                        pds.setAllowLocalTransactions(readOnly);
                        //  pds.setClassName("bitronix.tm.resource.jdbc.lrc.LrcXADataSource");
                        pds.setXaDataSource(xaDataSource);
                        pds.init();
                        dataSource = pds;
                        logger.info("{} connection pool initialized successfully for {} with size : {}. Time taken by connection in millis : {}"
                                , (readOnly ? "Read only" : "Read write"), dbType, pds.getTotalPoolSize(), (System.currentTimeMillis() - startTime));
                    } catch (Exception e) {
                        String url = ConnectionProperties.getInstance().data_model_db_jdbc_url.getPropertyValue();
                        throw new DSPDMException("Unable to create a {} connection pool for {} with URL : '{}'. Reason : {}",
                                e, executionContext.getExecutorLocale(), (readOnly ? "Read only" : "Read write"), dbType, url, e.getMessage());
                    }
                }
            }
        }
        return dataSource;
    }

    private XADataSource getPostgresXADataSource(ExecutionContext executionContext) {
        XADataSource xaDataSource = null;
        try {
            logger.info("Going to create postgres data source for URL : '{}'", ConnectionProperties.getInstance().data_model_db_jdbc_url.getPropertyValue());
            long startTime = System.currentTimeMillis();
            Class.forName(ConnectionProperties.getInstance().data_model_db_jdbc_driver_class.getPropertyValue());
            PGXADataSource pgxaDataSource = new PGXADataSource();
            pgxaDataSource.setUrl(ConnectionProperties.getInstance().data_model_db_jdbc_url.getPropertyValue());
            pgxaDataSource.setUser(AESSecurityUtils.decode(ConnectionProperties.getInstance().data_model_db_jdbc_user.getPropertyValue()));
            pgxaDataSource.setPassword(AESSecurityUtils.decode(ConnectionProperties.getInstance().data_model_db_jdbc_password.getPropertyValue()));

            xaDataSource = pgxaDataSource;
            executionContext.addDBProcessingTime((System.currentTimeMillis() - startTime));
            logger.info("Postgres data source created successfully. Time taken by connection in millis : {}", (System.currentTimeMillis() - startTime));
        } catch (Exception e) {
            throw new DSPDMException("Unable to create Postgres data source for URL '{}'. Reason : {}", e, executionContext.getExecutorLocale(), ConnectionProperties.getInstance().data_model_db_jdbc_url.getPropertyValue(), e.getMessage());
        }
        return xaDataSource;
    }

    private XADataSource getMSSQLServerXADataSource(ExecutionContext executionContext) {
        XADataSource xaDataSource = null;
        try {
            logger.info("Going to create MS SQL Server data source for URL : '{}'", ConnectionProperties.getInstance().data_model_db_jdbc_url.getPropertyValue());
            long startTime = System.currentTimeMillis();
            Class.forName(ConnectionProperties.getInstance().data_model_db_jdbc_driver_class.getPropertyValue());
            // create ms sql server xa data source
            SQLServerXADataSource sqlServerXADataSource = new SQLServerXADataSource();
            sqlServerXADataSource.setURL(ConnectionProperties.getInstance().data_model_db_jdbc_url.getPropertyValue());
            sqlServerXADataSource.setUser(AESSecurityUtils.decode(ConnectionProperties.getInstance().data_model_db_jdbc_user.getPropertyValue()));
            sqlServerXADataSource.setPassword(AESSecurityUtils.decode(ConnectionProperties.getInstance().data_model_db_jdbc_password.getPropertyValue()));
            xaDataSource = sqlServerXADataSource;
            // success
            executionContext.addDBProcessingTime((System.currentTimeMillis() - startTime));
            logger.info("MS SQL Server data source created successfully. Time taken by connection in millis : {}", (System.currentTimeMillis() - startTime));
        } catch (Exception e) {
            throw new DSPDMException("Unable to create MS SQL Server data source for URL '{}'. Reason : {}", e, executionContext.getExecutorLocale(), ConnectionProperties.getInstance().data_model_db_jdbc_url.getPropertyValue(), e.getMessage());
        }
        return xaDataSource;
    }
}
