package com.lgc.dspdm.core.common.data.common;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.CollectionUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Muhammad Imran Ansari
 */
public class SaveResultDTO {
    private Map<String, Integer> insertedRecordsCountByBoName = null;
    private Map<String, Integer> updatedRecordsCountByBoName = null;
    private Map<String, Integer> deletedRecordsCountByBoName = null;
    private Map<String, Integer> ignoredRecordsCountByBoName = null;
    private int addedDBColumnsCount = 0;
    private int updatedDBColumnsCount = 0;
    private int droppedDBColumnsCount = 0;
    private int createdDBTablesCount = 0;
    private int droppedDBTablesCount = 0;
    private int createdDBSequencesCount = 0;
    private int droppedDBSequencesCount = 0;
    private int createdDBRelationshipsCount = 0;
    private int droppedDBRelationshipsCount = 0;
    private int createdDBUniqueConstraintsCount = 0;
    private int createdDBSearchIndexesCount = 0;
    private int droppedDBUniqueConstraintsCount = 0;
    private int droppedDBSearchIndexesCount = 0;
    private boolean insertExecuted = false;
    private boolean updateExecuted = false;
    private boolean deleteExecuted = false;
    private Map<String, List<DynamicDTO>> dataFromReadBack = null;

    public int getInsertedRecordsCount() {
        int count = 0;
        if (CollectionUtils.hasValue(insertedRecordsCountByBoName)) {
            for (Integer boCount : insertedRecordsCountByBoName.values()) {
                count = count + boCount;
            }
        }
        return count;
    }

    public int getUpdatedRecordsCount() {
        int count = 0;
        if (CollectionUtils.hasValue(updatedRecordsCountByBoName)) {
            for (Integer boCount : updatedRecordsCountByBoName.values()) {
                count = count + boCount;
            }
        }
        return count;
    }

    public int getDeletedRecordsCount() {
        int count = 0;
        if (CollectionUtils.hasValue(deletedRecordsCountByBoName)) {
            for (Integer boCount : deletedRecordsCountByBoName.values()) {
                count = count + boCount;
            }
        }
        return count;
    }

    public int getIgnoredRecordsCount() {
        int count = 0;
        if (CollectionUtils.hasValue(ignoredRecordsCountByBoName)) {
            for (Integer boCount : ignoredRecordsCountByBoName.values()) {
                count = count + boCount;
            }
        }
        return count;
    }

    public Map<String, Integer> getInsertedRecordsCountByBoName() {
        return insertedRecordsCountByBoName;
    }

    public Map<String, Integer> getUpdatedRecordsCountByBoName() {
        return updatedRecordsCountByBoName;
    }

    public Map<String, Integer> getDeletedRecordsCountByBoName() {
        return deletedRecordsCountByBoName;
    }

    public Map<String, Integer> getIgnoredRecordsCountByBoName() {
        return ignoredRecordsCountByBoName;
    }

    public boolean isAnyRecordInserted() {
        return CollectionUtils.hasValue(insertedRecordsCountByBoName);
    }

    public boolean isAnyRecordUpdated() {
        return CollectionUtils.hasValue(updatedRecordsCountByBoName);
    }

    public boolean isAnyRecordDeleted() {
        return CollectionUtils.hasValue(deletedRecordsCountByBoName);
    }

    public boolean isAnyRecordIgnored() {
        return CollectionUtils.hasValue(ignoredRecordsCountByBoName);
    }

    public int getAddedDBColumnsCount() {
        return addedDBColumnsCount;
    }

    public int getUpdatedDBColumnsCount() { return updatedDBColumnsCount; }

    public int getDroppedDBColumnsCount() {
        return droppedDBColumnsCount;
    }

    public int getCreatedDBTablesCount() {
        return createdDBTablesCount;
    }

    public int getDroppedDBTablesCount() {
        return droppedDBTablesCount;
    }

    public int getCreatedDBSequencesCount() {
        return createdDBSequencesCount;
    }

    public int getDroppedDBSequencesCount() {
        return droppedDBSequencesCount;
    }

    public int getCreatedDBRelationshipsCount() {
        return createdDBRelationshipsCount;
    }

    public int getDroppedDBRelationshipsCount() {
        return droppedDBRelationshipsCount;
    }

    public int getCreatedDBUniqueConstraintsCount() { return createdDBUniqueConstraintsCount; }

    public int getCreatedDBSearchIndexesCount() { return createdDBSearchIndexesCount; }

    public int getDroppedDBUniqueConstraintsCount() {
        return droppedDBUniqueConstraintsCount;
    }

    public int getDroppedDBSearchIndexesCount() { return droppedDBSearchIndexesCount; }

    public boolean isInsertExecuted() {
        return insertExecuted;
    }

    public void setInsertExecuted(boolean insertExecuted) {
        this.insertExecuted = insertExecuted;
    }

    public boolean isUpdateExecuted() {
        return updateExecuted;
    }

    public void setUpdateExecuted(boolean updateExecuted) {
        this.updateExecuted = updateExecuted;
    }

    public boolean isDeleteExecuted() {
        return deleteExecuted;
    }

    public void setDeleteExecuted(boolean deleteExecuted) {
        this.deleteExecuted = deleteExecuted;
    }

    public void addInsertedRecordsCount(String boName, int count) {
        if (this.insertedRecordsCountByBoName == null) {
            this.insertedRecordsCountByBoName = new LinkedHashMap<>();
        } else {
            Integer oldCount = this.insertedRecordsCountByBoName.get(boName);
            if (oldCount != null) {
                count = oldCount + count;
            }
        }
        this.insertedRecordsCountByBoName.put(boName, count);
    }

    private void addInsertedRecordsCount(Map<String, Integer> insertedRecordsCountByBoName) {
        if (CollectionUtils.hasValue(insertedRecordsCountByBoName)) {
            for (Map.Entry<String, Integer> entry : insertedRecordsCountByBoName.entrySet()) {
                addInsertedRecordsCount(entry.getKey(), entry.getValue());
            }
        }
    }

    public void addUpdatedRecordsCount(String boName, int count) {
        if (this.updatedRecordsCountByBoName == null) {
            this.updatedRecordsCountByBoName = new LinkedHashMap<>();
        } else {
            Integer oldCount = this.updatedRecordsCountByBoName.get(boName);
            if (oldCount != null) {
                count = oldCount + count;
            }
        }
        this.updatedRecordsCountByBoName.put(boName, count);
    }

    private void addUpdatedRecordsCount(Map<String, Integer> updatedRecordsCountByBoName) {
        if (CollectionUtils.hasValue(updatedRecordsCountByBoName)) {
            for (Map.Entry<String, Integer> entry : updatedRecordsCountByBoName.entrySet()) {
                addUpdatedRecordsCount(entry.getKey(), entry.getValue());
            }
        }
    }

    public void addDeletedRecordsCount(String boName, int count) {
        if (this.deletedRecordsCountByBoName == null) {
            this.deletedRecordsCountByBoName = new LinkedHashMap<>();
        } else {
            Integer oldCount = this.deletedRecordsCountByBoName.get(boName);
            if (oldCount != null) {
                count = oldCount + count;
            }
        }
        this.deletedRecordsCountByBoName.put(boName, count);
    }

    private void addDeletedRecordsCount(Map<String, Integer> deletedRecordsCountByBoName) {
        if (CollectionUtils.hasValue(deletedRecordsCountByBoName)) {
            for (Map.Entry<String, Integer> entry : deletedRecordsCountByBoName.entrySet()) {
                addDeletedRecordsCount(entry.getKey(), entry.getValue());
            }
        }
    }

    public void addIgnoredRecordsCount(String boName, int count) {
        if (this.ignoredRecordsCountByBoName == null) {
            this.ignoredRecordsCountByBoName = new LinkedHashMap<>();
        } else {
            Integer oldCount = this.ignoredRecordsCountByBoName.get(boName);
            if (oldCount != null) {
                count = oldCount + count;
            }
        }
        this.ignoredRecordsCountByBoName.put(boName, count);
    }

    private void addIgnoredRecordsCount(Map<String, Integer> ignoredRecordsCountByBoName) {
        if (CollectionUtils.hasValue(ignoredRecordsCountByBoName)) {
            for (Map.Entry<String, Integer> entry : ignoredRecordsCountByBoName.entrySet()) {
                addIgnoredRecordsCount(entry.getKey(), entry.getValue());
            }
        }
    }

    public void addAddedDBColumnsCount(int count) { addedDBColumnsCount = addedDBColumnsCount + count; }

    public void addUpdatedDBColumnsCount(int count) { updatedDBColumnsCount = updatedDBColumnsCount + count; }

    public void addDroppedDBColumnsCount(int count) {
        droppedDBColumnsCount = droppedDBColumnsCount + count;
    }

    public void addCreatedDBTablesCount(int count) {
        createdDBTablesCount = createdDBTablesCount + count;
    }

    public void addDroppedDBTablesCount(int count) {
        droppedDBTablesCount = droppedDBTablesCount + count;
    }

    public void addCreatedDBSequencesCount(int count) {
        createdDBSequencesCount = createdDBSequencesCount + count;
    }

    public void addDroppedDBSequencesCount(int count) {
        droppedDBSequencesCount = droppedDBSequencesCount + count;
    }

    public void addCreatedDBRelationshipsCount(int count) {
        createdDBRelationshipsCount = createdDBRelationshipsCount + count;
    }

    public void addDroppedDBRelationshipsCount(int count) {
        droppedDBRelationshipsCount = droppedDBRelationshipsCount + count;
    }

    public void addCreatedDBUniqueConstraintsCount(int count) {
        createdDBUniqueConstraintsCount = createdDBUniqueConstraintsCount + count;
    }

    public void addCreatedDBSearchIndexesCount(int count) {
        createdDBSearchIndexesCount = createdDBSearchIndexesCount + count;
    }

    public void addDroppedDBUniqueConstraintsCount(int count) {
        droppedDBUniqueConstraintsCount = droppedDBUniqueConstraintsCount + count;
    }

    public void addDroppedDBSearchIndexesCount(int count) {
        droppedDBSearchIndexesCount = droppedDBSearchIndexesCount + count;
    }

    public boolean hasResult() {
        return (isAnyRecordInserted() || isAnyRecordUpdated() || isAnyRecordDeleted());
    }

    public void addResult(SaveResultDTO saveResultDTO) {
        this.addInsertedRecordsCount(saveResultDTO.insertedRecordsCountByBoName);
        this.addUpdatedRecordsCount(saveResultDTO.updatedRecordsCountByBoName);
        this.addDeletedRecordsCount(saveResultDTO.deletedRecordsCountByBoName);
        this.addIgnoredRecordsCount(saveResultDTO.ignoredRecordsCountByBoName);
        this.addedDBColumnsCount = this.addedDBColumnsCount + saveResultDTO.addedDBColumnsCount;
        this.updatedDBColumnsCount = this.updatedDBColumnsCount + saveResultDTO.updatedDBColumnsCount;
        this.droppedDBColumnsCount = this.droppedDBColumnsCount + saveResultDTO.droppedDBColumnsCount;
        this.createdDBTablesCount = this.createdDBTablesCount + saveResultDTO.createdDBTablesCount;
        this.droppedDBTablesCount = this.droppedDBTablesCount + saveResultDTO.droppedDBTablesCount;
        this.createdDBSequencesCount = this.createdDBSequencesCount + saveResultDTO.createdDBSequencesCount;
        this.droppedDBSequencesCount = this.droppedDBSequencesCount + saveResultDTO.droppedDBSequencesCount;
        this.createdDBRelationshipsCount = this.createdDBRelationshipsCount + saveResultDTO.createdDBRelationshipsCount;
        this.droppedDBRelationshipsCount = this.droppedDBRelationshipsCount + saveResultDTO.droppedDBRelationshipsCount;
        this.createdDBUniqueConstraintsCount = this.createdDBUniqueConstraintsCount + saveResultDTO.createdDBUniqueConstraintsCount;
        this.createdDBSearchIndexesCount = this.createdDBSearchIndexesCount + saveResultDTO.createdDBSearchIndexesCount;
        this.droppedDBUniqueConstraintsCount = this.droppedDBUniqueConstraintsCount + saveResultDTO.droppedDBUniqueConstraintsCount;
        this.droppedDBSearchIndexesCount = this.droppedDBSearchIndexesCount + saveResultDTO.droppedDBSearchIndexesCount;
        // do not change the value of the flags mentioned below if they are already true
        this.insertExecuted = (this.insertExecuted) ? true : saveResultDTO.isInsertExecuted();
        this.updateExecuted = (this.updateExecuted) ? true : saveResultDTO.isUpdateExecuted();
        this.deleteExecuted = (this.deleteExecuted) ? true : saveResultDTO.isDeleteExecuted();
    }

    public void addSearchResult(SaveResultDTO saveResultDTO) {
        // do nothing
    }

    public void addOperationResult(SaveResultDTO saveResultDTO) {
        // do nothing
    }

    public Map<String, List<DynamicDTO>> getDataFromReadBack() {
        return dataFromReadBack;
    }

    public void addDataFromReadBack(String boName, List<DynamicDTO> boListFromReadBack) {
        if (this.dataFromReadBack == null) {
            this.dataFromReadBack = new LinkedHashMap<>();
        }
        this.dataFromReadBack.put(boName, boListFromReadBack);
    }
}
