package com.lgc.dspdm.repo.delegate;

import bitronix.tm.BitronixTransactionManager;
import bitronix.tm.Configuration;
import bitronix.tm.TransactionManagerServices;
import com.lgc.dspdm.core.common.config.ConnectionProperties;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import javax.transaction.Status;

/**
 * Extend from this class if you want to write any data to the data store
 *
 * @author Muhammad Imran Ansari
 * @since 21-Oct-2019
 */
public class BaseTransactionalDelegate extends BaseDelegate implements AutoCloseable {
    private static DSPDMLogger logger = new DSPDMLogger(BaseTransactionalDelegate.class);
    private static Configuration configuration = null;
    private static BitronixTransactionManager transactionManager = null;

    protected BaseTransactionalDelegate(ExecutionContext executionContext) {
        super(executionContext);
        // going to call a synchronized method
        initializeTransactionManager(executionContext);
    }

    @Override
    public void close() throws Exception {
        if (transactionManager != null) {
            transactionManager.shutdown();
        }
        if (configuration != null) {
            configuration.shutdown();
        }
    }

    private synchronized void initializeTransactionManager(ExecutionContext executionContext) {
        if (configuration == null) {
            configuration = TransactionManagerServices.getConfiguration();
            configuration.setAllowMultipleLrc(true);
            configuration.setAsynchronous2Pc(true);
            configuration.setDefaultTransactionTimeout(ConnectionProperties.getInstance().data_model_db_transaction_timeout_seconds.getIntegerValue());
            transactionManager = TransactionManagerServices.getTransactionManager();
        }
    }

    protected void beginTransaction(ExecutionContext executionContext) {
        if (transactionManager != null) {
            try {
                // start a new transaction and attach to the current running thread
                transactionManager.begin();
                executionContext.setTransactionStarted(true);
            } catch (Exception e) {
                DSPDMException.throwException(e, executionContext);
            }
        } else {
            throw new DSPDMException("Unable to start a transaction. Transaction manager is not initialized.", executionContext.getExecutorLocale());
        }
    }

    protected void rollbackTransaction(Throwable cause, ExecutionContext executionContext) {
        if (transactionManager != null) {
            try {
                // roll back the transaction associated with the current thread
                transactionManager.rollback();
                executionContext.setTransactionStarted(false);
                executionContext.setTransactionRollbacked(true);
                logger.info("Transaction roll backed successfully.");
            } catch (Exception e) {
                if (cause != null) {
                    e.addSuppressed(cause);
                }
                DSPDMException.throwException(e, executionContext);
            }
        } else {
            DSPDMException exception = new DSPDMException("Unable to roll back a transaction. Transaction manager is not initialized.", executionContext.getExecutorLocale());
            exception.addSuppressed(cause);
            throw exception;
        }
    }

    protected void commitTransaction(ExecutionContext executionContext) {
        if (transactionManager != null) {
            try {
                // commit the transaction associated with the current thread
                transactionManager.commit();
                executionContext.setTransactionStarted(false);
                executionContext.setTransactionCommitted(true);
                logger.info("Transaction committed successfully.");
            } catch (Exception e) {
                DSPDMException.throwException(e, executionContext);
            }
        } else {
            throw new DSPDMException("Unable to commit a transaction. Transaction manager is not initialized.", executionContext.getExecutorLocale());
        }
    }

    protected String getTransactionStatus(ExecutionContext executionContext) {
        String statusString = "STATUS_NO_TRANSACTION";
        if (transactionManager != null) {
            try {
                int status = transactionManager.getStatus();
                switch (status) {
                    case Status.STATUS_ACTIVE:
                        statusString = "STATUS_ACTIVE";
                        break;
                    case Status.STATUS_MARKED_ROLLBACK:
                        statusString = "STATUS_MARKED_ROLLBACK";
                        break;
                    case Status.STATUS_PREPARED:
                        statusString = "STATUS_PREPARED";
                        break;
                    case Status.STATUS_COMMITTED:
                        statusString = "STATUS_COMMITTED";
                        break;
                    case Status.STATUS_ROLLEDBACK:
                        statusString = "STATUS_ROLLEDBACK";
                        break;
                    case Status.STATUS_UNKNOWN:
                        statusString = "STATUS_UNKNOWN";
                        break;
                    case Status.STATUS_NO_TRANSACTION:
                        statusString = "STATUS_NO_TRANSACTION";
                        break;
                    case Status.STATUS_PREPARING:
                        statusString = "STATUS_PREPARING";
                        break;
                    case Status.STATUS_COMMITTING:
                        statusString = "STATUS_COMMITTING";
                        break;
                    case Status.STATUS_ROLLING_BACK:
                        statusString = "STATUS_ROLLING_BACK";
                        break;
                }
            } catch (Exception e) {
                DSPDMException.throwException(e, executionContext);
            }
        } else {
            throw new DSPDMException("Unable to get transaction status. Transaction manager is not initialized.", executionContext.getExecutorLocale());
        }
        return statusString;
    }
}
