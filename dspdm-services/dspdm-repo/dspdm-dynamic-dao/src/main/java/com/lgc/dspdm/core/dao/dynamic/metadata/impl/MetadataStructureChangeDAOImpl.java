package com.lgc.dspdm.core.dao.dynamic.metadata.impl;

import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.core.dao.BaseDAO;
import com.lgc.dspdm.core.dao.dynamic.IMetadataStructureChangeDAOImpl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

public class MetadataStructureChangeDAOImpl extends AbstractDBStructureChangeDAO implements IMetadataStructureChangeDAOImpl {
    private static DSPDMLogger logger = new DSPDMLogger(MetadataStructureChangeDAOImpl.class);

    private static IMetadataStructureChangeDAOImpl singleton = null;

    private MetadataStructureChangeDAOImpl() {
    }

    public static IMetadataStructureChangeDAOImpl getInstance() {
        if (singleton == null) {
            singleton = new MetadataStructureChangeDAOImpl();
        }
        return singleton;
    }

    @Override
    protected String getType() {
        return null;
    }

    @Override
    public SaveResultDTO addPhysicalColumnInDatabaseTable(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // add column
        saveResultDTO.addResult(addColumn(dynamicDTO, connection, executionContext));
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO updatePhysicalColumnInDatabaseTable(DynamicDTO dynamicDTO, String columnPropertyNameToUpdate, Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // update column
        saveResultDTO.addResult(updateColumn(dynamicDTO, columnPropertyNameToUpdate, connection, executionContext));
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO deletePhysicalColumnFromDatabaseTable(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // drop column
        saveResultDTO.addResult(dropColumn(dynamicDTO, connection, executionContext));
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO addPhysicalTableInDatabase(DynamicDTO businessObjectDynamicDTO,
                                                    List<DynamicDTO> businessObjectAttributes,
                                                    List<DynamicDTO> businessObjectRelationships_FK,
                                                    List<DynamicDTO> uniqueConstraints,
                                                    List<DynamicDTO> searchIndexes,
                                                    Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // create sequence
        saveResultDTO.addResult(createSequence(businessObjectDynamicDTO, connection, executionContext));
        // create table
        saveResultDTO.addResult(createTable(businessObjectDynamicDTO, businessObjectAttributes, businessObjectRelationships_FK, uniqueConstraints,connection, executionContext));
        // adding search index
        if(CollectionUtils.hasValue(searchIndexes)){
            addSearchIndex(searchIndexes,connection, executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO deletePhysicalTableFromDatabase(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // drop table
        saveResultDTO.addResult(dropTable(dynamicDTO, connection, executionContext));
        // drop sequence
        saveResultDTO.addResult(dropSequence(dynamicDTO, connection, executionContext));
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO deletePhysicalRelationshipFromDatabase(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // drop constraint
        saveResultDTO.addResult(dropRelationshipConstraint(dynamicDTO, connection, executionContext));
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO addPhysicalRelationshipToDatabase(List<DynamicDTO> relationships, Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // add constraint
        saveResultDTO.addResult(addForeignKeyConstraint(relationships, connection, executionContext));
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO addPhysicalUniqueConstraintToDatabase(List<DynamicDTO> uniqueConstraints, Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // add unique constraint
        saveResultDTO.addResult(addUniqueConstraint(uniqueConstraints, connection, executionContext));
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO addPhysicalSearchIndexToDatabase(List<DynamicDTO> searchIndexes, Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // add search index
        saveResultDTO.addResult(addSearchIndex(searchIndexes, connection, executionContext));
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO deletePhysicalUniqueConstraintFromDatabase(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // drop constraint
        saveResultDTO.addResult(dropUniqueConstraint(dynamicDTO, connection, executionContext));
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO deletePhysicalSearchIndexFromDatabase(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // drop search index
        saveResultDTO.addResult(dropSearchIndex(dynamicDTO, connection, executionContext));
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO dropDefaultConstraintForMSSQLServerOnly(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = new SaveResultDTO();
        // drop constraint
        saveResultDTO.addResult(dropDefaultConstraint(dynamicDTO, connection, executionContext));
        return saveResultDTO;
    }

    @Override
    public List<String[]> getAttributesVsUnitMapping(String boName, Connection connection, ExecutionContext executionContext) {
        return getAttributesVsUnitMappings(boName, connection, executionContext);
    }

    @Override
    public List<DynamicDTO> restoreBOGroupFromHistory(List<DynamicDTO> boList, Connection connection, ExecutionContext executionContext) {
        return restoreBusinessObjectGroupFromHistoryTables(boList, connection, executionContext);
    }
}
