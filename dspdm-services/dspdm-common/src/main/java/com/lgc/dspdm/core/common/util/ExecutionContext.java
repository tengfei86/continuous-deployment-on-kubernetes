package com.lgc.dspdm.core.common.util;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.*;

public class ExecutionContext implements Serializable {
    private static DSPDMLogger logger = new DSPDMLogger(ExecutionContext.class);
    private String executorName = null;
    private Locale executorLocale = null;
    private ZoneId executorTimeZone = null;
    private boolean transactionStarted = false;
    private boolean transactionCommitted = false;
    private boolean transactionRollbacked = false;
    private boolean unitTestCall = false;
    private long processingStartTime = 0;
    private long totalTimeTakenByDB = 0;
    private boolean readBack = false;
    private boolean collectSQLStats = false;
    private CollectSQLScriptOptions collectSQLScriptOptions = CollectSQLScriptOptions.NO_SCRIPT;
    private boolean fullDrop = false;
    private boolean deleteCascade = false;
    private Map<String, Integer[]> sqlStats = null;
    private List<String> sqlScripts = null;
    private boolean writeNullValues=false;
    private boolean indexAll = false;
    private DynamicDTO userOperationDTO = null;
    private List<DynamicDTO> busObjAttrChangeHistoryDTOList = null;
    private Integer currentOperationId = null;
    private Integer operationSequenceNumber = null;
    private Integer ddlOperationsCount = null;
    private Boolean generateMetadataForAll = false;
    private Boolean deleteMetadataForAll = false;
    private List<DynamicDTO> reportingEntityDTOList = null;
    private List<DynamicDTO> rReportingEntityKindDTOList = null;


    private ExecutionContext(boolean unitTestCall, String executorName, Locale executorLocale, ZoneId executorTimeZone) {
        this.unitTestCall = unitTestCall;
        this.executorName = executorName;
        this.executorLocale = executorLocale;
        this.executorTimeZone = executorTimeZone;
        this.processingStartTime = DateTimeUtils.getCurrentTimestampUTC().getTime();
        this.totalTimeTakenByDB = 0;
        if (!unitTestCall) {
            Thread.currentThread().setName(this.executorName + "_" + StringUtils.getNextRandomNumber(5));
        }
    }

    public static ExecutionContext getEmptyExecutionContext() {
        return new ExecutionContext(false, null, null, null);
    }

    public static ExecutionContext getTestUserExecutionContext() {
        return new ExecutionContext(true, DSPDMConstants.DEFAULT_USERS.TEST_USER, Locale.getDefault(), ZoneId.systemDefault());
    }

    public static ExecutionContext getSystemUserExecutionContext() {
        return new ExecutionContext(false, DSPDMConstants.DEFAULT_USERS.SYSTEM_USER, Locale.getDefault(), ZoneId.systemDefault());
    }

    public static ExecutionContext getUnknownUserExecutionContext() {
        return new ExecutionContext(false, DSPDMConstants.DEFAULT_USERS.UNKNOWN_USER, Locale.getDefault(), ZoneId.systemDefault());
    }

    public String getExecutorName() {
        return executorName;
    }

    public ExecutionContext setExecutorName(String executorName) {
        this.executorName = executorName;
        return this;
    }

    public Locale getExecutorLocale() {
        return executorLocale;
    }

    public ExecutionContext setExecutorLocale(Locale executorLocale) {
        this.executorLocale = executorLocale;
        return this;
    }

    public ZoneId getExecutorTimeZone() {
        return executorTimeZone;
    }

    public ExecutionContext setExecutorTimeZone(ZoneId executorTimeZone) {
        this.executorTimeZone = executorTimeZone;
        return this;
    }

    public boolean isReadBack() {
        return readBack;
    }

    public ExecutionContext setReadBack(boolean readBack) {
        this.readBack = readBack;
        return this;
    }

    public boolean isCollectSQLStats() {
        return collectSQLStats;
    }

    public ExecutionContext setCollectSQLStats(boolean collectSQLStats) {
        this.collectSQLStats = collectSQLStats;
        return this;
    }

    public CollectSQLScriptOptions getCollectSQLScriptOptions() {
        return collectSQLScriptOptions;
    }

    public ExecutionContext setCollectSQLScriptOptions(CollectSQLScriptOptions collectSQLScriptOptions) {
        this.collectSQLScriptOptions = collectSQLScriptOptions;
        return this;
    }

    public boolean isFullDrop() {
        return fullDrop;
    }

    public ExecutionContext setFullDrop(boolean fullDrop) {
        this.fullDrop = fullDrop;
        return this;
    }

    public boolean isDeleteCascade() {
        return deleteCascade;
    }

    public ExecutionContext setDeleteCascade(boolean deleteCascade) {
        this.deleteCascade = deleteCascade;
        return this;
    }

    public Map<String, Integer[]> getSqlStats() {
        return sqlStats;
    }

    public ExecutionContext addSQLStats(String sql, Integer[] timeTakenInMillis) {
        if (sqlStats == null) {
            sqlStats = new LinkedHashMap<>(1);
        }
        Integer[] oldStats = sqlStats.get(sql);
        if (oldStats == null) {
            sqlStats.put(sql, timeTakenInMillis);
        } else {
            oldStats[0] = oldStats[0] + timeTakenInMillis[0];
            oldStats[1] = oldStats[1] + timeTakenInMillis[1];
        }
        return this;
    }

    public List<String> getSqlScripts() {
        return sqlScripts;
    }


    public ExecutionContext addSqlScript(String sql) {
        if (sqlScripts == null) {
            sqlScripts = new LinkedList<>();
        }
        sqlScripts.add(sql);
        return this;
    }

    public boolean isTransactionStarted() {
        return transactionStarted;
    }

    public ExecutionContext setTransactionStarted(boolean transactionStarted) {
        this.transactionStarted = transactionStarted;
        return this;
    }

    public boolean isTransactionCommitted() {
        return transactionCommitted;
    }

    public ExecutionContext setTransactionCommitted(boolean transactionCommitted) {
        this.transactionCommitted = transactionCommitted;
        return this;
    }

    public boolean isTransactionRollbacked() {
        return transactionRollbacked;
    }

    public ExecutionContext setTransactionRollbacked(boolean transactionRollbacked) {
        this.transactionRollbacked = transactionRollbacked;
        return this;
    }

    public long getProcessingStartTime() {
        return processingStartTime;
    }

    public void setProcessingStartTime(long processingStartTime) {
        this.processingStartTime = processingStartTime;
    }

    public long getTotalTimeTakenByDB() {
        return totalTimeTakenByDB;
    }

    public void setTotalTimeTakenByDB(long totalTimeTakenByDB) {
        this.totalTimeTakenByDB = totalTimeTakenByDB;
    }

    public ExecutionContext addDBProcessingTime(long timeTakenByDB) {
        this.totalTimeTakenByDB = this.totalTimeTakenByDB + timeTakenByDB;
        return this;
    }

    private void logTotalDBOperationsTime() {

        logger.info("Total time taken by database processing : {} millis", getTotalTimeTakenByDB());
    }

    private void logTotalRequestTime() {
        logger.info("Total time taken by user request : {} millis", (DateTimeUtils.getCurrentTimestampUTC().getTime() - getProcessingStartTime()));
    }

    public boolean isUnitTestCall() {
        return unitTestCall;
    }

    public ExecutionContext setUnitTestCall(boolean unitTestCall) {
        this.unitTestCall = unitTestCall;
        return this;
    }

    public void logTotalProcessingTime() {
        logTotalDBOperationsTime();
        logTotalRequestTime();
    }

    public boolean isWriteNullValues() {
        return writeNullValues;
    }

    public ExecutionContext setWriteNullValues(boolean writeNullValues) {
        this.writeNullValues = writeNullValues;
        return this;
    }

    public boolean isIndexAll() {
        return indexAll;
    }

    public ExecutionContext setIndexAll(boolean indexAll) {
        this.indexAll = indexAll;
        return this;
    }

    public DynamicDTO getUserOperationDTO() {
        return userOperationDTO;
    }

    public List<DynamicDTO> getBusObjAttrChangeHistoryDTOList() {
        return busObjAttrChangeHistoryDTOList;
    }


    public void addBusObjAttrChangeHistoryDTOList (DynamicDTO dynamicDTO){
        if(this.busObjAttrChangeHistoryDTOList == null){
            busObjAttrChangeHistoryDTOList = new ArrayList<>();
        }
        this.busObjAttrChangeHistoryDTOList.add(dynamicDTO);
    }

    public List<DynamicDTO> getReportingEntityDTOList() { return reportingEntityDTOList; }

    public void addDTOToReportingEntityDTOList (DynamicDTO dynamicDTO){
        if(this.reportingEntityDTOList == null){
            reportingEntityDTOList = new ArrayList<>();
        }
        this.reportingEntityDTOList.add(dynamicDTO);
    }

    public List<DynamicDTO> getrReportingEntityKindDTOList() { return rReportingEntityKindDTOList; }

    public void setrReportingEntityKindDTOList(List<DynamicDTO> rReportingEntityKindDTOList) { this.rReportingEntityKindDTOList = rReportingEntityKindDTOList; }

    public void  addUserOperationDTO (DynamicDTO dynamicDTO){
        this.userOperationDTO = dynamicDTO;
    }

    public Integer getCurrentOperationId() {
        return currentOperationId;
    }

    public void setCurrentOperationId(Integer currentOperationId) {
        this.currentOperationId = currentOperationId;
    }

    public Integer getOperationSequenceNumber() {
        if(operationSequenceNumber == null){
            operationSequenceNumber = 0;
        }
        return operationSequenceNumber;
    }

    public void incrementOperationSequenceNumber() {
        if(operationSequenceNumber == null){
            operationSequenceNumber = 0;
        }
        this.operationSequenceNumber++;
    }

    public Integer getDdlOperationsCount() {
        if(ddlOperationsCount == null){
            ddlOperationsCount = 0;
        }
        return ddlOperationsCount;
    }

    public void incrementDdlOperationCount() {
        if(ddlOperationsCount == null){
            ddlOperationsCount = 0;
        }
        this.ddlOperationsCount++;
    }

    public Boolean getGenerateMetadataForAll() {
        return generateMetadataForAll;
    }

    public void setGenerateMetadataForAll(Boolean generateMetadataForAll) {
        this.generateMetadataForAll = generateMetadataForAll;
    }

    public Boolean getDeleteMetadataForAll() { return deleteMetadataForAll; }

    public void setDeleteMetadataForAll(Boolean deleteMetadataForAll) {
        this.deleteMetadataForAll = deleteMetadataForAll;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExecutionContext{");
        sb.append("executorName='").append(executorName).append('\'');
        sb.append(", executorLocale=").append(executorLocale);
        sb.append(", executorTimeZone=").append(executorTimeZone);
        sb.append(", transactionStarted=").append(transactionStarted);
        sb.append(", transactionCommitted=").append(transactionCommitted);
        sb.append(", transactionRollbacked=").append(transactionRollbacked);
        sb.append(", unitTestCall=").append(unitTestCall);
        sb.append(", processingStartTime=").append(processingStartTime);
        sb.append(", totalTimeTakenByDB=").append(totalTimeTakenByDB);
        sb.append(", readBack=").append(readBack);
        sb.append(", collectSQLStats=").append(collectSQLStats);
        sb.append(", collectSQLScriptOptions=").append(collectSQLScriptOptions);
        sb.append(", fullDrop=").append(fullDrop);
        sb.append(", deleteCascade=").append(deleteCascade);
        sb.append(", sqlStats=").append(sqlStats);
        sb.append(", sqlScripts=").append(sqlScripts);
        sb.append(", writeNullValues=").append(writeNullValues);
        sb.append(", indexAll=").append(indexAll);
        sb.append(", userOperationDTO=").append(userOperationDTO);
        sb.append(", busObjAttrChangeHistoryDTOList=").append(busObjAttrChangeHistoryDTOList);
        sb.append(", currentOperationId=").append(currentOperationId);
        sb.append(", operationSequenceNumber=").append(operationSequenceNumber);
        sb.append(", generateMetadataForAll=").append(generateMetadataForAll);
        sb.append(", deleteMetadataForAll=").append(deleteMetadataForAll);
        sb.append('}');
        return sb.toString();
    }
}
