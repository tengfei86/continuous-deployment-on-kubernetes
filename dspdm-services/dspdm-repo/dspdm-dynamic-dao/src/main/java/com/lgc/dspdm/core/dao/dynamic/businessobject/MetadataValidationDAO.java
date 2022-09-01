package com.lgc.dspdm.core.dao.dynamic.businessobject;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.*;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.*;
import com.lgc.dspdm.core.dao.BaseDAO;
import com.lgc.dspdm.core.dao.dynamic.IDynamicDAO;
import com.lgc.dspdm.core.dao.fixed.GenericDAO;

import java.util.*;

public class MetadataValidationDAO extends BaseDAO {
    private static DSPDMLogger logger = new DSPDMLogger(MetadataValidationDAO.class);
    private static MetadataValidationDAO singleton = null;

    private MetadataValidationDAO() {
        logger.info("Inside Metadata Validation DAO Constructor");
    }

    public static synchronized MetadataValidationDAO getInstance() {
        if (singleton == null) {
            singleton = new MetadataValidationDAO();
        }
        return singleton;
    }

    @Override
    protected String getType() {
        return DSPDMConstants.BoName.BUSINESS_OBJECT_ATTR;
    }

    public Map<String, List<String>> validateMetadata(DynamicDTO businessObject, ExecutionContext executionContext) {
        Map<String, List<String>> sortedMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (businessObject == null) {
            List<TableDTO> tableDTOListFromDB = getTableDTOListFromDB(null, executionContext);
            List<TableDTO> tableDTOLIstFromMetadata = getTableDTOListFromMetadata(executionContext);
            processMatchedNotMatchedTableDTO(tableDTOListFromDB, tableDTOLIstFromMetadata, sortedMap, executionContext);
        } else {
            List<TableDTO> tableDTOListFromDB = getTableDTOListFromDB(businessObject, executionContext);
            List<TableDTO> tableDTOLIstFromMetadata = getTableDTOListFromMetadata(businessObject, executionContext);
            processMatchedNotMatchedTableDTO(tableDTOListFromDB, tableDTOLIstFromMetadata, sortedMap, executionContext);
        }
        return sortedMap;
    }

    private List<TableDTO> getTableDTOListFromDB( DynamicDTO businessObject, ExecutionContext executionContext) {
        logger.info("Inside Dynamic DAO Factory Get All Tables");
        List<TableDTO> list = null;
        try {
            if (businessObject == null) {
                list = GenericDAO.getInstance().findTableDTO(executionContext);
            } else {
                String tableName = (String) businessObject.get(DSPDMConstants.BoAttrName.ENTITY);
                String boName = (String) businessObject.get(DSPDMConstants.BoAttrName.BO_NAME);
                list = GenericDAO.getInstance().findTableDTOForTableName(boName, tableName, executionContext);
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return list;
    }

    private List<TableDTO> getTableDTOListFromMetadata(ExecutionContext executionContext) {
        logger.info("Convert Metadata data to Table DTO For All business objects one by one");
        List<TableDTO> list = new ArrayList<>();
        try {
            List<DynamicDTO> dynamicDTOList = DynamicDAOFactory.getInstance(executionContext)
                    .getBusinessObjects(executionContext);
            for (DynamicDTO dynamicDTO : dynamicDTOList) {
                list.addAll(getTableDTOListFromMetadata(dynamicDTO, executionContext));
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return list;
    }

    private List<TableDTO> getTableDTOListFromMetadata(DynamicDTO dynamicDTO, ExecutionContext executionContext) {
        String boName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.BO_NAME);
        String sequenceName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.KEY_SEQ_NAME);
        String schemaName = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.SCHEMA_NAME);
        String entity = (String) dynamicDTO.get(DSPDMConstants.BoAttrName.ENTITY);
        logger.info("Convert Metadata data to Table {}", boName);
        List<TableDTO> list = new ArrayList<>();
        try {
            if (StringUtils.hasValue(sequenceName)) {
                TableDTO sequenceDTO = new TableDTO();
                sequenceDTO.setType(DSPDMConstants.SchemaName.SEQUENCE);
                sequenceDTO.setTableName(sequenceName);
                sequenceDTO.setEntity(entity);
                list.add(sequenceDTO);
            }

            TableDTO tableDTO = new TableDTO();
            tableDTO.setSchema(schemaName);
            tableDTO.setTableName(entity);
            tableDTO.setEntity(entity);
            tableDTO.setType(DSPDMConstants.SchemaName.TABLE);

            IDynamicDAO dynamicDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(boName, executionContext);

            List<BusinessObjectAttributeDTO> businessObjectAttributeDTOList = ((DynamicDAO) dynamicDAO)
                    .getBusinessObjectAttributeDTOS();
            if (CollectionUtils.hasValue(businessObjectAttributeDTOList)) {
                List<ColumnDTO> columnDTOList = new ArrayList<>(businessObjectAttributeDTOList.size());
                int leftParenthesisIndex = 0;
                int rightParenthesisIndex = 0;
                int commaIndex = 0;
                for (BusinessObjectAttributeDTO businessObjectAttributeDTO : businessObjectAttributeDTOList) {
                    ColumnDTO columnDTO = new ColumnDTO();
                    columnDTO.setTableName(businessObjectAttributeDTO.getEntityName());
                    columnDTO.setColumnName(businessObjectAttributeDTO.getAttributeName());
                    columnDTO.setDefaultValue(businessObjectAttributeDTO.getAttributeDefault());
                    leftParenthesisIndex = businessObjectAttributeDTO.getAttributeDatatype().indexOf("(");
                    if (leftParenthesisIndex > 0) {
                        columnDTO.setDataType(businessObjectAttributeDTO.getAttributeDatatype()
                                .substring(0, leftParenthesisIndex));
                        commaIndex = businessObjectAttributeDTO.getAttributeDatatype().indexOf(",");
                        rightParenthesisIndex = businessObjectAttributeDTO.getAttributeDatatype().indexOf(")");
                        if (commaIndex > 0) {
                            columnDTO.setColumnSize(NumberUtils.convertToInteger(businessObjectAttributeDTO.getAttributeDatatype()
                                    .substring(leftParenthesisIndex + 1, commaIndex), executionContext));
                            columnDTO.setDecimalDigits(NumberUtils.convertToInteger(businessObjectAttributeDTO.getAttributeDatatype()
                                    .substring(commaIndex + 1, rightParenthesisIndex), executionContext));
                        } else {
                            columnDTO.setColumnSize(NumberUtils.convertToInteger(businessObjectAttributeDTO.getAttributeDatatype()
                                    .substring(leftParenthesisIndex + 1, rightParenthesisIndex), executionContext));
                            columnDTO.setDecimalDigits(0);
                        }
                    } else {
                        columnDTO.setDataType(businessObjectAttributeDTO.getAttributeDatatype());
                        columnDTO.setColumnSize(0);
                        columnDTO.setDecimalDigits(0);
                    }
                    columnDTO.setPrimarykey(businessObjectAttributeDTO.getPrimaryKey());
                    columnDTO.setColumnIndex(businessObjectAttributeDTO.getSequenceNumber());
                    columnDTO.setSchema(businessObjectAttributeDTO.getSchemaName());
                    columnDTO.setType(DSPDMConstants.SchemaName.COLUMN);
                    columnDTO.setNullAble(!businessObjectAttributeDTO.getMandatory());// mandatory true means nullable false
                    columnDTO.setBoName(businessObjectAttributeDTO.getBoName());
                    columnDTO.setReferenceBoName(businessObjectAttributeDTO.getReferenceBOName());
                    columnDTO.setReferenceBoAttrValue(businessObjectAttributeDTO.getReferenceBOAttrNameForValue());
                    columnDTO.setReferenceBoAttrLabel(businessObjectAttributeDTO.getReferenceBOAttrNameForLabel());
                    columnDTOList.add(columnDTO);
                }
                tableDTO.setColumnDTOList(columnDTOList);
            }

            List<BusinessObjectAttributeDTO> primarykeyDTOList = ((DynamicDAO) dynamicDAO)
                    .getPrimaryKeyColumns();
            if (CollectionUtils.hasValue(primarykeyDTOList)) {
                List<PrimaryKeyDTO> primaryKeyDTOList = new ArrayList<>(primarykeyDTOList.size());
                for (BusinessObjectAttributeDTO primarykeyDTO : primarykeyDTOList) {
                    PrimaryKeyDTO primaryKeyDTO = new PrimaryKeyDTO();
                    primaryKeyDTO.setTableName(primarykeyDTO.getEntityName());
                    primaryKeyDTO.setColumnName(primarykeyDTO.getAttributeName());
                    primaryKeyDTO.setSchema(primarykeyDTO.getSchemaName());
                    primaryKeyDTO.setColumnIndex(primarykeyDTO.getSequenceNumber());
                    primaryKeyDTO.setType(DSPDMConstants.SchemaName.PRIMARYKEY);
                    primaryKeyDTOList.add(primaryKeyDTO);
                }
                tableDTO.setPrimaryKeyDTOList(primaryKeyDTOList);
            }
            // Unique Indexes
            List<BusinessObjectAttributeConstraintsDTO> businessObjectAttributeConstraintsDTOList = ((DynamicDAO) dynamicDAO)
                    .getBusinessObjectAttributeConstraintsDTOS();
            if (CollectionUtils.hasValue(businessObjectAttributeConstraintsDTOList)) {
                List<IndexDTO> uniqueIndexDTOList = new ArrayList<>(businessObjectAttributeConstraintsDTOList.size());
                for (BusinessObjectAttributeConstraintsDTO businessObjectAttributeConstraintsDTO : businessObjectAttributeConstraintsDTOList) {
                    IndexDTO uniqueIndexDTO = new IndexDTO();
                    uniqueIndexDTO.setTableName(businessObjectAttributeConstraintsDTO.getEntityName());
                    uniqueIndexDTO.setColumnName(businessObjectAttributeConstraintsDTO.getAttributeName());
                    uniqueIndexDTO.setIndexName(businessObjectAttributeConstraintsDTO.getConstraintName());
                    uniqueIndexDTO.setIndexType(DSPDMConstants.SchemaName.UNIQUE_INDEX);
                    uniqueIndexDTO.setColumnIndex(businessObjectAttributeConstraintsDTO.getId());
                    uniqueIndexDTO.setType(DSPDMConstants.SchemaName.INDEX);
                    uniqueIndexDTO.setNonUnique(Boolean.FALSE);
                    uniqueIndexDTOList.add(uniqueIndexDTO);
                }
                tableDTO.setUniqueIndexDTOList(uniqueIndexDTOList);
            }

            // Search Indexes
            List<BusinessObjectAttributeSearchIndexesDTO> businessObjectAttributeSearchIndexesDTOList = ((DynamicDAO) dynamicDAO)
                    .getBusinessObjectAttributeSearchIndexesDTOS();
            if (CollectionUtils.hasValue(businessObjectAttributeSearchIndexesDTOList)) {
                List<IndexDTO> searchIndexDTOList = new ArrayList<>(businessObjectAttributeSearchIndexesDTOList.size());
                for (BusinessObjectAttributeSearchIndexesDTO businessObjectAttributeSearchIndexDTO : businessObjectAttributeSearchIndexesDTOList) {
                    IndexDTO searchIndexDTO = new IndexDTO();
                    searchIndexDTO.setTableName(businessObjectAttributeSearchIndexDTO.getEntityName());
                    searchIndexDTO.setColumnName(businessObjectAttributeSearchIndexDTO.getAttributeName());
                    searchIndexDTO.setIndexName(businessObjectAttributeSearchIndexDTO.getIndexName());
                    searchIndexDTO.setIndexType(DSPDMConstants.SchemaName.SEARCH_INDEX);
                    searchIndexDTO.setColumnIndex(businessObjectAttributeSearchIndexDTO.getId());
                    searchIndexDTO.setType(DSPDMConstants.SchemaName.INDEX);
                    searchIndexDTO.setNonUnique(Boolean.TRUE);
                    searchIndexDTOList.add(searchIndexDTO);
                }
                tableDTO.setSearchIndexDTOList(searchIndexDTOList);
            }

            List<BusinessObjectRelationshipDTO> businessObjectRelationshipDTOList = ((DynamicDAO) dynamicDAO)
                    .getBusinessObjectRelationshipDTOS();
            if (CollectionUtils.hasValue(businessObjectRelationshipDTOList)) {
                List<ForeignKeyDTO> foreignKeyDTOList = new ArrayList<>(businessObjectRelationshipDTOList.size());
                for (BusinessObjectRelationshipDTO businessObjectRelationshipDTO : businessObjectRelationshipDTOList) {
                    // making sure that foreign key validation should appear only in child table not in parent table of current tableDTO
                    if(!businessObjectRelationshipDTO.getParentEntityName().equalsIgnoreCase(tableDTO.getEntity())){
                        ForeignKeyDTO foreignKeyDTO = new ForeignKeyDTO();
                        foreignKeyDTO.setTableName(businessObjectRelationshipDTO.getParentEntityName());
                        foreignKeyDTO.setColumnName(businessObjectRelationshipDTO.getParentBoAttrName());
                        foreignKeyDTO.setForeignkeyName(businessObjectRelationshipDTO.getConstraintName());
                        foreignKeyDTO.setForeignkeyTable(businessObjectRelationshipDTO.getChildEntityName());
                        foreignKeyDTO.setForeignKeyBoName(businessObjectRelationshipDTO.getChildBoName());
                        foreignKeyDTO.setForeignkeyColumn(businessObjectRelationshipDTO.getChildBoAttrName());
                        foreignKeyDTO.setColumnIndex(businessObjectRelationshipDTO.getId());
                        foreignKeyDTO.setType(DSPDMConstants.SchemaName.FOREIGNKEY);
                        foreignKeyDTOList.add(foreignKeyDTO);
                    }
                }
                tableDTO.setForeignKeyDTOList(foreignKeyDTOList);
            }
            list.add(tableDTO);
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return list;
    }

    // PROCESS TABLE DTO

    private static void processMatchedNotMatchedTableDTO(List<TableDTO> tableDTOListFromDB, List<TableDTO> tableDTOLIstFromMetadata,
                                                         Map<String, List<String>> sortedMap, ExecutionContext executionContext) {
        Object[] matchedNotMatchedTableDTOMap = getMatchedNotMatchedTableDTOMap(tableDTOListFromDB, tableDTOLIstFromMetadata);
        List<TableDTO> tableDTOFromDBListNotMatched = (List<TableDTO>) matchedNotMatchedTableDTOMap[0];
        List<TableDTO> tableDTOFromMetadataListNotMatched = (List<TableDTO>) matchedNotMatchedTableDTOMap[1];
        Map<TableDTO, TableDTO> matchedTableDTOMap = (Map<TableDTO, TableDTO>) matchedNotMatchedTableDTOMap[2];
        processMatchedTableDTO(matchedTableDTOMap, sortedMap, executionContext);
        processTableDTOFromDBNotMatched(tableDTOFromDBListNotMatched, sortedMap);
        processTableDTOFromMetadataNotMatched(tableDTOFromMetadataListNotMatched, sortedMap);
    }

    private static void processMatchedTableDTO(Map<TableDTO, TableDTO> matchedTableDTOMap, Map<String, List<String>> sortedMap,
                                               ExecutionContext executionContext) {
        // now iterate on the map
        if (CollectionUtils.hasValue(matchedTableDTOMap)) {
            Map<String, IDynamicDAO> iDynamicDAOMap = new HashMap<>();
            String title = "=== %s: %s ===";
            String message = "INFO: --- TYPE: [%s], NAME: [%s] --- exists in both metadata and also in database.";
            for (Map.Entry<TableDTO, TableDTO> entry : matchedTableDTOMap.entrySet()) {
                TableDTO tableDTOFromDB = entry.getKey();
                TableDTO tableDTOFroMetadata = entry.getValue();
                // clone entity name from metadata list to db list just to know the assigned sequence name to an entity
                tableDTOFromDB.setEntity(tableDTOFroMetadata.getEntity());
                // process table name
                List<String> resultObject = new ArrayList<>();
                resultObject.add(String.format(message, tableDTOFromDB.getType(), tableDTOFromDB.getTableName()));

                // process columns info
                processMatchedNotMatchedColumnDTO(tableDTOFromDB, tableDTOFroMetadata, resultObject);

                // process primary key info
                processMatchedNotMatchedPrimaryKeyDTO(tableDTOFromDB, tableDTOFroMetadata, resultObject);

                // process unique indexes
                processMatchedNotMatchedUniqueIndexDTO(tableDTOFromDB, tableDTOFroMetadata, resultObject);

                // process search indexes
                processMatchedNotMatchedSearchIndexDTO(tableDTOFromDB, tableDTOFroMetadata, resultObject);

                // process relationships
                processMatchedNotMatchedForeignKeyDTO(tableDTOFromDB, tableDTOFroMetadata, resultObject, iDynamicDAOMap, executionContext);

                // add title to map along with the internal messages
                saveInSortedMap(sortedMap, resultObject, tableDTOFroMetadata);
            }
        }
    }

    private static void processTableDTOFromDBNotMatched(List<TableDTO> tableDTOFromDBListNotMatched, Map<String, List<String>> sortedMap) {
        if (CollectionUtils.hasValue(tableDTOFromDBListNotMatched)) {
            String title = "=== %s: %s ===";
            String message = "WARNING: --- TYPE: [%s], NAME: [%s] --- exists in database but does not exist in metadata.";
            for (TableDTO tableDTOFromDB : tableDTOFromDBListNotMatched) {
                List<String> resultObject = new ArrayList<>();
                resultObject.add(String.format(message, tableDTOFromDB.getType(), tableDTOFromDB.getTableName()));
                saveInSortedMap(sortedMap, resultObject, tableDTOFromDB);
            }
        }
    }

    private static void processTableDTOFromMetadataNotMatched(List<TableDTO> tableDTOFromMetadataListNotMatched, Map<String, List<String>> sortedMap) {
        if (CollectionUtils.hasValue(tableDTOFromMetadataListNotMatched)) {
            String title = "=== %s: %s ===";
            String message = "ERROR: --- TYPE: [%s], NAME: [%s] --- exists in metadata but does not exist in database.";
            for (TableDTO tableDTOFromMetadata : tableDTOFromMetadataListNotMatched) {
                List<String> resultObject = new ArrayList<>();
                resultObject.add(String.format(message, tableDTOFromMetadata.getType(), tableDTOFromMetadata.getTableName()));
                saveInSortedMap(sortedMap, resultObject, tableDTOFromMetadata);
            }
        }
    }

    private static void saveInSortedMap(Map<String, List<String>> sortedMap,  List<String> resultObject, TableDTO tableDTO){
        String tableName;
        if(tableDTO.getType().equalsIgnoreCase(DSPDMConstants.SEQUENCE)){
            tableName = tableDTO.getEntity();
            // if tableName is null then it means the sequence/table exists in physical table but doesn't exist in metadata.
            // in that case we're assuming that the sequence name is of pattern SEQ_<TABLE_NAME>.
            // In all other cases than above, we've read the link between sequence name and table name from metadata.
            if(tableName == null){
                tableName = StringUtils.replaceAllIgnoreCaseForGenericString(tableDTO.getTableName(), DSPDMConstants.SEQUENCE_PREFIX, "");
            }
            if(!sortedMap.containsKey(tableName)){
                sortedMap.put(tableName, resultObject);
            }
        }else{
            tableName = tableDTO.getTableName();
            if(!sortedMap.containsKey(tableName)){
                sortedMap.put(tableName, resultObject);
            }else{
                sortedMap.get(tableName).addAll(resultObject);
            }
        }
    }
    private static Object[] getMatchedNotMatchedTableDTOMap(List<TableDTO> tableDTOFromDBList, List<TableDTO> tableDTOFromMetadataList) {

        List<TableDTO> tableDTOFromDBListNotMatched = new LinkedList<>();
        List<TableDTO> tableDTOFromMetadataListNotMatched = new LinkedList<>();
        Map<TableDTO, TableDTO> matchedTableDTOMap = new LinkedHashMap<>();
        // add all objects from metadata to not matched
        if (CollectionUtils.hasValue(tableDTOFromMetadataList)) {
            tableDTOFromMetadataListNotMatched.addAll(tableDTOFromMetadataList);
        }
        for (TableDTO dbDTO : tableDTOFromDBList) {
            boolean matched = false;
            for (int index = 0; index < tableDTOFromMetadataListNotMatched.size(); index++) {
                TableDTO metadataDTO = tableDTOFromMetadataListNotMatched.get(index);
                if ((metadataDTO.getType().equalsIgnoreCase(dbDTO.getType()))
                        && (metadataDTO.getTableName().equalsIgnoreCase(dbDTO.getTableName()))) {
                    // put matched objects in map
                    matchedTableDTOMap.put(dbDTO, metadataDTO);
                    // remove by index is faster than remove by object
                    tableDTOFromMetadataListNotMatched.remove(index);
                    // reduce list size so that in next iteration list is short in size
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                // tableDTO from db does not exist in metadata therefore add to not matched list
                tableDTOFromDBListNotMatched.add(dbDTO);
            }
        }

        return new Object[]{tableDTOFromDBListNotMatched, tableDTOFromMetadataListNotMatched, matchedTableDTOMap};
    }

    private static Object[] getMatchedNotMatchedPrimaryKeyDTOMap(List<PrimaryKeyDTO> pkDTOListFromDB, List<PrimaryKeyDTO> pkDTOListFromMetadata) {
        List<PrimaryKeyDTO> pkDTOListFromDBNotMatched = new LinkedList<>();
        List<PrimaryKeyDTO> pkDTOListFromMetadataNotMatched = new LinkedList<>();
        Map<PrimaryKeyDTO, PrimaryKeyDTO> matchedPKDTOMap = new LinkedHashMap<>();
        // add all objects from metadata to not matched
        if (CollectionUtils.hasValue(pkDTOListFromMetadata)) {
            pkDTOListFromMetadataNotMatched.addAll(pkDTOListFromMetadata);
        }
        if (CollectionUtils.hasValue(pkDTOListFromDB)) {
            for (PrimaryKeyDTO dbDTO : pkDTOListFromDB) {
                boolean matched = false;
                for (int index = 0; index < pkDTOListFromMetadataNotMatched.size(); index++) {
                    PrimaryKeyDTO metadataDTO = pkDTOListFromMetadataNotMatched.get(index);
                    if ((metadataDTO.getTableName().equalsIgnoreCase(dbDTO.getTableName()))
                            && (metadataDTO.getColumnName().equalsIgnoreCase(dbDTO.getColumnName()))) {
                        // put matched objects in map
                        matchedPKDTOMap.put(dbDTO, metadataDTO);
                        // remove by index is faster than remove by object
                        pkDTOListFromMetadataNotMatched.remove(index);
                        // reduce list size so that in next iteration list is short in size
                        matched = true;
                        break;
                    }
                }
                if (!matched) {
                    // tableDTO from db does not exist in metadata therefore add to not matched list
                    pkDTOListFromDBNotMatched.add(dbDTO);
                }
            }
        }
        return new Object[]{pkDTOListFromDBNotMatched, pkDTOListFromMetadataNotMatched, matchedPKDTOMap};
    }

    private static Object[] getMatchedNotMatchedColumnDTOMap(List<ColumnDTO> columnDTOListFromDB, List<ColumnDTO> columnDTOListFromMetadata) {
        List<ColumnDTO> columnDTOListFromDBNotMatched = new LinkedList<>();
        List<ColumnDTO> columnDTOListFromMetadataNotMatched = new LinkedList<>();
        Map<ColumnDTO, ColumnDTO> matchedColumnDTOMap = new LinkedHashMap<>();
        // add all objects from metadata to not matched
        if (CollectionUtils.hasValue(columnDTOListFromMetadata)) {
            columnDTOListFromMetadataNotMatched.addAll(columnDTOListFromMetadata);
        }
        if (CollectionUtils.hasValue(columnDTOListFromDB)) {
            for (ColumnDTO dbDTO : columnDTOListFromDB) {
                boolean matched = false;
                for (int index = 0; index < columnDTOListFromMetadataNotMatched.size(); index++) {
                    ColumnDTO metadataDTO = columnDTOListFromMetadataNotMatched.get(index);
                    if ((metadataDTO.getTableName().equalsIgnoreCase(dbDTO.getTableName()))
                            && (metadataDTO.getColumnName().equalsIgnoreCase(dbDTO.getColumnName()))) {
                        // put matched objects in map
                        matchedColumnDTOMap.put(dbDTO, metadataDTO);
                        // remove by index is faster than remove by object
                        columnDTOListFromMetadataNotMatched.remove(index);
                        // reduce list size so that in next iteration list is short in size
                        matched = true;
                        break;
                    }
                }
                if (!matched) {
                    // tableDTO from db does not exist in metadata therefore add to not matched list
                    columnDTOListFromDBNotMatched.add(dbDTO);
                }
            }
        }
        return new Object[]{columnDTOListFromDBNotMatched, columnDTOListFromMetadataNotMatched, matchedColumnDTOMap};
    }

    private static Object[] getMatchedNotMatchedUniqueIndexDTOMap(List<IndexDTO> indexDTOFromDBList, List<IndexDTO> indexDTOFromMetadataList) {
        List<IndexDTO> indexDTOListFromDBNotMatched = new LinkedList<>();
        List<IndexDTO> indexDTOListFromMetadataNotMatched = new LinkedList<>();
        Map<IndexDTO, IndexDTO> matchedIndexDTOMap = new LinkedHashMap<>();
        // add all objects from metadata to not matched
        if (CollectionUtils.hasValue(indexDTOFromMetadataList)) {
            indexDTOListFromMetadataNotMatched.addAll(indexDTOFromMetadataList);
        }
        if (CollectionUtils.hasValue(indexDTOFromDBList)) {
            for (IndexDTO indexDTOFromDB : indexDTOFromDBList) {
                if (!(indexDTOFromDB.getIndexType().equalsIgnoreCase(DSPDMConstants.SchemaName.PRIMARYKEY))) {
                    boolean matched = false;
                    for (int index = 0; index < indexDTOListFromMetadataNotMatched.size(); index++) {
                        IndexDTO indexDTOFromMetadata = indexDTOListFromMetadataNotMatched.get(index);
                        if (!(indexDTOFromMetadata.getIndexType().equalsIgnoreCase(DSPDMConstants.SchemaName.PRIMARYKEY))) {
                            if ((indexDTOFromMetadata.getTableName().equalsIgnoreCase(indexDTOFromDB.getTableName()))
                                    && (indexDTOFromMetadata.getIndexName().equalsIgnoreCase(indexDTOFromDB.getIndexName()))
                                    && (indexDTOFromMetadata.getColumnName().equalsIgnoreCase(indexDTOFromDB.getColumnName()))) {
                                // put matched objects in map
                                matchedIndexDTOMap.put(indexDTOFromDB, indexDTOFromMetadata);
                                // remove by index is faster than remove by object
                                indexDTOListFromMetadataNotMatched.remove(index);
                                // reduce list size so that in next iteration list is short in size
                                matched = true;
                                break;
                            }
                        } else {
                            // remove by index is faster than remove by object
                            indexDTOListFromMetadataNotMatched.remove(index);
                            // reduce list size so that in next iteration list is short in size
                        }
                    }
                    if (!matched) {
                        // tableDTO from db does not exist in metadata therefore add to not matched list
                        indexDTOListFromDBNotMatched.add(indexDTOFromDB);
                    }
                }
            }
        }
        return new Object[]{indexDTOListFromDBNotMatched, indexDTOListFromMetadataNotMatched, matchedIndexDTOMap};
    }

    private static Object[] getMatchedNotMatchedSearchIndexDTOMap(List<IndexDTO> searchIndexDTOFromDBList, List<IndexDTO> indexDTOFromMetadataList) {
        List<IndexDTO> searchIndexDTOListFromDBNotMatched = new LinkedList<>();
        List<IndexDTO> searchIndexDTOListFromMetadataNotMatched = new LinkedList<>();
        Map<IndexDTO, IndexDTO> matchedSearchIndexDTOMap = new LinkedHashMap<>();
        // add all objects from metadata to not matched
        if (CollectionUtils.hasValue(indexDTOFromMetadataList)) {
            searchIndexDTOListFromMetadataNotMatched.addAll(indexDTOFromMetadataList);
        }
        if (CollectionUtils.hasValue(searchIndexDTOFromDBList)) {
            for (IndexDTO searchIndexDTOFromDB : searchIndexDTOFromDBList) {
                if (!(searchIndexDTOFromDB.getIndexType().equalsIgnoreCase(DSPDMConstants.SchemaName.PRIMARYKEY))) {
                    boolean matched = false;
                    for (int index = 0; index < searchIndexDTOListFromMetadataNotMatched.size(); index++) {
                        IndexDTO indexDTOFromMetadata = searchIndexDTOListFromMetadataNotMatched.get(index);
                        if (!(indexDTOFromMetadata.getIndexType().equalsIgnoreCase(DSPDMConstants.SchemaName.PRIMARYKEY))) {
                            if ((indexDTOFromMetadata.getTableName().equalsIgnoreCase(searchIndexDTOFromDB.getTableName()))
                                    && (indexDTOFromMetadata.getIndexName().equalsIgnoreCase(searchIndexDTOFromDB.getIndexName()))
                                    && (indexDTOFromMetadata.getColumnName().equalsIgnoreCase(searchIndexDTOFromDB.getColumnName()))) {
                                // put matched objects in map
                                matchedSearchIndexDTOMap.put(searchIndexDTOFromDB, indexDTOFromMetadata);
                                // remove by index is faster than remove by object
                                searchIndexDTOListFromMetadataNotMatched.remove(index);
                                // reduce list size so that in next iteration list is short in size
                                matched = true;
                                break;
                            }
                        } else {
                            // remove by index is faster than remove by object
                            searchIndexDTOListFromMetadataNotMatched.remove(index);
                            // reduce list size so that in next iteration list is short in size
                        }
                    }
                    if (!matched) {
                        // tableDTO from db does not exist in metadata therefore add to not matched list
                        searchIndexDTOListFromDBNotMatched.add(searchIndexDTOFromDB);
                    }
                }
            }
        }
        return new Object[]{searchIndexDTOListFromDBNotMatched, searchIndexDTOListFromMetadataNotMatched, matchedSearchIndexDTOMap};
    }

    private static Object[] getMatchedNotMatchedForeignKeyDTOMap(List<ForeignKeyDTO> foreignKeyDTOFromDBList,
                                                                 List<ForeignKeyDTO> foreignKeyDTOFromMetadataList,
                                                                 List<ColumnDTO> columnDTOFromMetadataList,
                                                                 Map<String, IDynamicDAO> iDynamicDAOMap,
                                                                 ExecutionContext executionContext) {
        List<ForeignKeyDTO> foreignKeyDTOFromDBListNotMatched = new LinkedList<>();
        List<ForeignKeyDTO> foreignKeyDTOFromMetadataListNotMatched = new LinkedList<>();
        Map<ForeignKeyDTO, ForeignKeyDTO> matchedForeignKeyDTOMap = new LinkedHashMap<>();
        Map<ForeignKeyDTO, ColumnDTO> invalidReferenceBoName = new LinkedHashMap<>();
        Map<ForeignKeyDTO, ColumnDTO> invalidReferenceBoAttrValue = new LinkedHashMap<>();
        Map<ForeignKeyDTO, ColumnDTO> invalidReferenceBoAttrLabel = new LinkedHashMap<>();

        // add all objects from metadata to not matched
        if (CollectionUtils.hasValue(foreignKeyDTOFromMetadataList)) {
            foreignKeyDTOFromMetadataListNotMatched.addAll(foreignKeyDTOFromMetadataList);
        }
        if (CollectionUtils.hasValue(foreignKeyDTOFromDBList)) {
            for (ForeignKeyDTO foreignKeyDTOFromDB : foreignKeyDTOFromDBList) {
                boolean matched = false;
                for (int index = 0; index < foreignKeyDTOFromMetadataListNotMatched.size(); index++) {
                    ForeignKeyDTO foreignKeyDTOFromMetadata = foreignKeyDTOFromMetadataListNotMatched.get(index);
                    if ((foreignKeyDTOFromMetadata.getTableName().equalsIgnoreCase(foreignKeyDTOFromDB.getTableName()))
                            && (foreignKeyDTOFromMetadata.getForeignkeyTable().equalsIgnoreCase(foreignKeyDTOFromDB.getForeignkeyTable()))
                            && (foreignKeyDTOFromMetadata.getColumnName().equalsIgnoreCase(foreignKeyDTOFromDB.getColumnName()))
                            && (foreignKeyDTOFromMetadata.getForeignkeyColumn().equalsIgnoreCase(foreignKeyDTOFromDB.getForeignkeyColumn()))
                            && (foreignKeyDTOFromMetadata.getForeignkeyName().equalsIgnoreCase(foreignKeyDTOFromDB.getForeignkeyName()))) {
                        // verifying that attribute have reference fields in bus_object_attr table for current relationship
                        for(ColumnDTO columnDTO: columnDTOFromMetadataList){
                            if((columnDTO.getColumnName().equalsIgnoreCase(foreignKeyDTOFromMetadata.getForeignkeyColumn()))
                                    && columnDTO.getBoName().equalsIgnoreCase(foreignKeyDTOFromMetadata.getForeignKeyBoName())){
                                // means attribute DTO is found on which relationship is applied
                                // now check if reference fields are available for the found DTO
                                if(!StringUtils.hasValue(columnDTO.getReferenceBoName())){
                                    invalidReferenceBoName.put(foreignKeyDTOFromMetadata, columnDTO);
                                }
                                if(!StringUtils.hasValue(columnDTO.getReferenceBoAttrValue())){
                                    invalidReferenceBoAttrValue.put(foreignKeyDTOFromMetadata, columnDTO);
                                }
                                if(!StringUtils.hasValue(columnDTO.getReferenceBoAttrLabel())){
                                    invalidReferenceBoAttrLabel.put(foreignKeyDTOFromMetadata, columnDTO);
                                }
                                // if all reference fields are not null or empty, then go for validation of reference fields
                                if ((StringUtils.hasValue(columnDTO.getReferenceBoName()))
                                        && (StringUtils.hasValue(columnDTO.getReferenceBoAttrValue()))
                                        && (StringUtils.hasValue(columnDTO.getReferenceBoAttrLabel()))) {
                                    // means reference fields are not null, now we need to validate if reference fields are correct or no
                                    String referenceBoName = columnDTO.getReferenceBoName();
                                    String referenceBoAttrLabel = columnDTO.getReferenceBoAttrLabel();
                                    String referenceBOAttrValue = columnDTO.getReferenceBoAttrValue();
                                    try {
                                        IDynamicDAO iDynamicDAO;
                                        // used iDynamicDAOMap to reduce getDynamicDaoCalls for a particular BO
                                        if(iDynamicDAOMap.containsKey(referenceBoName)){
                                            iDynamicDAO = iDynamicDAOMap.get(referenceBoName);
                                        }else{
                                            iDynamicDAO = DynamicDAOFactory.getInstance(executionContext).getDynamicDAO(referenceBoName, executionContext);
                                            iDynamicDAOMap.put(referenceBoName, iDynamicDAO);
                                        }
                                        List<String> boAttrNames = iDynamicDAO.readMetadataBOAttrNames(executionContext);
                                        if (!CollectionUtils.containsIgnoreCase(boAttrNames, referenceBOAttrValue)) {
                                            invalidReferenceBoAttrValue.put(foreignKeyDTOFromMetadata, columnDTO);
                                        }
                                        if(referenceBoAttrLabel.contains(",")){
                                            // means we've a composite key, need to split it and then validate it
                                            String[] splitReferenceBoAttrLabel = referenceBoAttrLabel.split(",");
                                            for(String boAttrName: splitReferenceBoAttrLabel){
                                                if (!CollectionUtils.containsIgnoreCase(boAttrNames, boAttrName.trim())) {
                                                    invalidReferenceBoAttrLabel.put(foreignKeyDTOFromMetadata, columnDTO);
                                                    // break out of loop since once attribute is incorrect, simply add in invalidReferenceBoAttrLabel list
                                                    break;
                                                }
                                            }
                                        }else if (!CollectionUtils.containsIgnoreCase(boAttrNames, referenceBoAttrLabel)) {
                                            invalidReferenceBoAttrLabel.put(foreignKeyDTOFromMetadata, columnDTO);
                                        }
                                    } catch (Exception e) {
                                        // means referenceBoName not found, eat exception and add to invalid reference fields
                                        invalidReferenceBoName.put(foreignKeyDTOFromMetadata, columnDTO);
                                    }
                                }
                                break;
                            }
                        }
                        // put matched objects in map
                        matchedForeignKeyDTOMap.put(foreignKeyDTOFromDB, foreignKeyDTOFromMetadata);
                        // remove by index is faster than remove by object
                        foreignKeyDTOFromMetadataListNotMatched.remove(index);
                        // reduce list size so that in next iteration list is short in size
                        matched = true;
                        break;
                    }
                }
                if (!matched) {
                    // tableDTO from db does not exist in metadata therefore add to not matched list
                    foreignKeyDTOFromDBListNotMatched.add(foreignKeyDTOFromDB);
                }
            }
        }
        return new Object[]{foreignKeyDTOFromDBListNotMatched, foreignKeyDTOFromMetadataListNotMatched,
                invalidReferenceBoName, invalidReferenceBoAttrValue, invalidReferenceBoAttrLabel, matchedForeignKeyDTOMap};
    }

    private static void processMatchedNotMatchedPrimaryKeyDTO(TableDTO tableDTOFromDB, TableDTO tableDTOFroMetadata, List<String> resultObject) {
        Object[] matchedNotMatchedPrimaryKeyDTOMap = getMatchedNotMatchedPrimaryKeyDTOMap(
                tableDTOFromDB.getPrimaryKeyDTOList(), tableDTOFroMetadata.getPrimaryKeyDTOList());
        List<PrimaryKeyDTO> columnDTOFromDBListNotMatched = (List<PrimaryKeyDTO>) matchedNotMatchedPrimaryKeyDTOMap[0];
        List<PrimaryKeyDTO> columnDTOFromMetadataListNotMatched = (List<PrimaryKeyDTO>) matchedNotMatchedPrimaryKeyDTOMap[1];
        Map<PrimaryKeyDTO, PrimaryKeyDTO> matchedColumnDTOMap = (Map<PrimaryKeyDTO, PrimaryKeyDTO>) matchedNotMatchedPrimaryKeyDTOMap[2];
        processMatchedPrimaryKeyDTO(matchedColumnDTOMap, resultObject);
        processPrimaryKeyDTOFromDBNotMatched(columnDTOFromDBListNotMatched, resultObject);
        processPrimaryKeyDTOFromMetadataNotMatched(columnDTOFromMetadataListNotMatched, resultObject);
    }

    private static void processMatchedPrimaryKeyDTO(Map<PrimaryKeyDTO, PrimaryKeyDTO> matchedPKDTOMap, List<String> resultObject) {
        if (CollectionUtils.hasValue(matchedPKDTOMap)) {
            String infoMessage = "INFO: --- TYPE: [%s], PRIMARY KEY: [%s] --- exists in both metadata and also in database.";
            for (PrimaryKeyDTO primaryKeyDTO : matchedPKDTOMap.keySet()) {
                resultObject.add(String.format(infoMessage, primaryKeyDTO.getType(), primaryKeyDTO.getColumnName()));
            }
        }
    }

    private static void processPrimaryKeyDTOFromDBNotMatched(List<PrimaryKeyDTO> pkDTOListFromDBNotMatched, List<String> resultObject) {
        if (CollectionUtils.hasValue(pkDTOListFromDBNotMatched)) {
            String message = "WARNING: --- TYPE: [%s], PRIMARY KEY: [%s] --- exists in database but does not exist in metadata.";
            for (PrimaryKeyDTO pkDTO : pkDTOListFromDBNotMatched) {
                resultObject.add(String.format(message, pkDTO.getType(), pkDTO.getColumnName()));
            }
        }
    }

    private static void processPrimaryKeyDTOFromMetadataNotMatched(List<PrimaryKeyDTO> pkDTOListFromMetadataNotMatched, List<String> resultObject) {
        if (CollectionUtils.hasValue(pkDTOListFromMetadataNotMatched)) {
            String message = "ERROR: --- TYPE: [%s], PRIMARY KEY: [%s] --- exists in metadata but does not exist in database.";
            for (PrimaryKeyDTO pkDTO : pkDTOListFromMetadataNotMatched) {
                resultObject.add(String.format(message, pkDTO.getType(), pkDTO.getColumnName()));
            }
        }
    }

    private static void processMatchedNotMatchedColumnDTO(TableDTO tableDTOFromDB, TableDTO tableDTOFroMetadata, List<String> resultObject) {
        Object[] matchedNotMatchedColumnDTOMap = getMatchedNotMatchedColumnDTOMap(
                tableDTOFromDB.getColumnDTOList(), tableDTOFroMetadata.getColumnDTOList());
        List<ColumnDTO> columnDTOFromDBListNotMatched = (List<ColumnDTO>) matchedNotMatchedColumnDTOMap[0];
        List<ColumnDTO> columnDTOFromMetadataListNotMatched = (List<ColumnDTO>) matchedNotMatchedColumnDTOMap[1];
        Map<ColumnDTO, ColumnDTO> matchedColumnDTOMap = (Map<ColumnDTO, ColumnDTO>) matchedNotMatchedColumnDTOMap[2];
        processMatchedColumnDTO(matchedColumnDTOMap, resultObject);
        processColumnDTOFromDBNotMatched(columnDTOFromDBListNotMatched, resultObject);
        processColumnDTOFromMetadataNotMatched(columnDTOFromMetadataListNotMatched, resultObject);
    }

    private static void processMatchedColumnDTO(Map<ColumnDTO, ColumnDTO> matchedColumnDTOMap, List<String> resultObject) {
        if (CollectionUtils.hasValue(matchedColumnDTOMap)) {
            String infoMessage = "INFO: --- TYPE: [%s], COLUMN NAME: [%s] --- exists in both metadata and also in database.";
            String primaryKeyErrorMessage = "ERROR: --- TYPE: [%s], COLUMN NAME: [%s] --- primary key error: database is %s, metadata is %s.";
            String defaultValueErrorMessage = "ERROR: --- TYPE: [%s], COLUMN NAME: [%s] --- default value error: database is %s, metadata is %s.";
            String notNullErrorMessage = "ERROR: --- TYPE: [%s], COLUMN NAME: [%s] --- nullable error: database is %s, metadata is %s.";
            String dataTypeErrorMessage = "ERROR: --- TYPE: [%s], COLUMN NAME: [%s] --- data type error: database is [datatype: %s, size: %s, DecimalDigits: %s], metadata is [datatype: %s, size: %s, DecimalDigits: %s].";
            for (Map.Entry<ColumnDTO, ColumnDTO> entry : matchedColumnDTOMap.entrySet()) {
                ColumnDTO columnDTOFromDB = entry.getKey();
                ColumnDTO columnDTOFromMetadata = entry.getValue();
                resultObject.add(String.format(infoMessage, columnDTOFromDB.getType(), columnDTOFromDB.getColumnName()));

                // primary key
                if (columnDTOFromDB.isPrimarykey() != columnDTOFromMetadata.isPrimarykey()) {
                    resultObject.add(String.format(primaryKeyErrorMessage, columnDTOFromDB.getType(), columnDTOFromDB.getColumnName(),
                            columnDTOFromDB.isPrimarykey(), columnDTOFromMetadata.isPrimarykey()));
                }

                // default value
                if ((StringUtils.hasValue(columnDTOFromDB.getDefaultValue())) || (StringUtils.hasValue(columnDTOFromMetadata.getDefaultValue()))) {
                    boolean isInvalidDefaultValue = false;
                    if (((StringUtils.hasValue(columnDTOFromDB.getDefaultValue())) && (StringUtils.isNullOrEmpty(columnDTOFromMetadata.getDefaultValue())))
                            || ((StringUtils.isNullOrEmpty(columnDTOFromDB.getDefaultValue())) && (StringUtils.hasValue(columnDTOFromMetadata.getDefaultValue())))) {
                        isInvalidDefaultValue = true;
                    } else if ((StringUtils.hasValue(columnDTOFromDB.getDefaultValue())) && StringUtils.hasValue(columnDTOFromMetadata.getDefaultValue())
                            && (!columnDTOFromDB.getDefaultValue().equalsIgnoreCase(columnDTOFromMetadata.getDefaultValue()))) {
                        // tyring matching one more time with removing quotes from db column dto
                        // case: default value error: database is nextval('seq_alarm_detail'::regclass), metadata is nextval(seq_alarm_detail::regclass).
                        if (!columnDTOFromDB.getDefaultValue().replaceAll("'", "").equalsIgnoreCase(columnDTOFromMetadata.getDefaultValue())) {
                            // trying matching one more time with removing round brackets
                            // case:  default value error: database is ((1)), metadata is 1."
                            if (!columnDTOFromDB.getDefaultValue().replaceAll("[()]", "").equalsIgnoreCase(columnDTOFromMetadata.getDefaultValue())) {
                                isInvalidDefaultValue = true;
                            }
                        }// now applying check if one DTO (columnDTOFromDB or columnDTOFromMetadata), have a default type
                        // and other don't (i.e., any of them is as null), then add it to resultObject as  an error
                    }
                    if (isInvalidDefaultValue) {
                        resultObject.add(String.format(defaultValueErrorMessage, columnDTOFromDB.getType(), columnDTOFromDB.getColumnName(),
                                columnDTOFromDB.getDefaultValue(), columnDTOFromMetadata.getDefaultValue()));
                    }
                }
                // nullable
                if (columnDTOFromDB.isNullAble() != columnDTOFromMetadata.isNullAble()) {
                    resultObject.add(String.format(notNullErrorMessage, columnDTOFromDB.getType(), columnDTOFromDB.getColumnName(),
                            columnDTOFromDB.isNullAble(), columnDTOFromMetadata.isNullAble()));
                }

                // data type name
                boolean invalidColumnDataType = true;
                Boolean isMSSQLServerDialect = getInstance().isMSSQLServerDialect(columnDTOFromMetadata.getBoName());
                if (((columnDTOFromDB.getDataType().equalsIgnoreCase(columnDTOFromMetadata.getDataType())))) {
                    // now checking columnSize and decimal digits in case data type is other than string or numeric, otherwise data type is matched
                    if (DSPDMConstants.DataTypes.fromAttributeDataType(columnDTOFromDB.getDataType(), isMSSQLServerDialect).isStringDataType()
                            || DSPDMConstants.DataTypes.fromAttributeDataType(columnDTOFromDB.getDataType(), isMSSQLServerDialect).isNumericDataType()) {
                        if (((columnDTOFromDB.getColumnSize().intValue() == columnDTOFromMetadata.getColumnSize().intValue())
                                && (columnDTOFromDB.getDecimalDigits().intValue() == columnDTOFromMetadata.getDecimalDigits().intValue()))) {
                            invalidColumnDataType = false;
                        }
                    } else {
                        // data type matched
                        invalidColumnDataType = false;
                    }
                } else if ((!isMSSQLServerDialect)
                        && DSPDMConstants.DataTypes.fromAttributeDataType(columnDTOFromDB.getDataType(), false).isCharacterVaryingDataType()
                        && DSPDMConstants.DataTypes.fromAttributeDataType(columnDTOFromMetadata.getDataType(), false).isCharacterVaryingDataType()) {
                    // This trick is applied because postgres can have both varchar and character varying data types. So if we get data type from db
                    // as varchar and from metadata we get character varying (also vice versa), this is not an error.
                    // data type matched if length is same
                    if (((columnDTOFromDB.getColumnSize().intValue() == columnDTOFromMetadata.getColumnSize().intValue())
                            && (columnDTOFromDB.getDecimalDigits().intValue() == columnDTOFromMetadata.getDecimalDigits().intValue()))) {
                        invalidColumnDataType = false;
                    }
                } else{
                    // checking correctness of data by comparing with sql data types
                    int[] sqlDataTypesFromDB = DSPDMConstants.DataTypes.
                            fromAttributeDataType(columnDTOFromDB.getDataType(), isMSSQLServerDialect).getSqlDataTypes();
                    int[] sqlDataTypesFromMetadata = DSPDMConstants.DataTypes.
                            fromAttributeDataType(columnDTOFromMetadata.getDataType(), isMSSQLServerDialect).getSqlDataTypes();
                    // this will compare both arrays and data inside it as well
                    if (Arrays.equals(sqlDataTypesFromDB, sqlDataTypesFromMetadata)) {
                        invalidColumnDataType = false;
                    }
                }

                if (invalidColumnDataType) {
                    resultObject.add(String.format(dataTypeErrorMessage, columnDTOFromDB.getType(), columnDTOFromDB.getColumnName(),
                            columnDTOFromDB.getDataType(), columnDTOFromDB.getColumnSize(), columnDTOFromDB.getDecimalDigits(),
                            columnDTOFromMetadata.getDataType(), columnDTOFromMetadata.getColumnSize(), columnDTOFromMetadata.getDecimalDigits()));
                }
            }
        }
    }

    private static void processColumnDTOFromDBNotMatched(List<ColumnDTO> columnDTOListFromDBNotMatched, List<String> resultObject) {
        if (CollectionUtils.hasValue(columnDTOListFromDBNotMatched)) {
            String message = "WARNING: --- TYPE: [%s], COLUMN NAME: [%s] --- exists in database but does not exist in metadata.";
            for (ColumnDTO columnDTO : columnDTOListFromDBNotMatched) {
                resultObject.add(String.format(message, columnDTO.getType(), columnDTO.getColumnName()));
            }
        }
    }

    private static void processColumnDTOFromMetadataNotMatched(List<ColumnDTO> columnDTOListFromMetadataNotMatched, List<String> resultObject) {
        if (CollectionUtils.hasValue(columnDTOListFromMetadataNotMatched)) {
            String message = "ERROR: --- TYPE: [%s], COLUMN NAME: [%s] --- exists in metadata but does not exist in database.";
            for (ColumnDTO columnDTO : columnDTOListFromMetadataNotMatched) {
                resultObject.add(String.format(message, columnDTO.getType(), columnDTO.getColumnName()));
            }
        }
    }

    // PROCESS UNIQUE INDEX DTO
    private static void processMatchedNotMatchedUniqueIndexDTO(TableDTO tableDTOFromDB, TableDTO tableDTOFromMetadata, List<String> resultObject) {
        List<PrimaryKeyDTO> primaryKeyDTOList = tableDTOFromDB.getPrimaryKeyDTOList();
        List<IndexDTO> uniqueIndexDTOListFromDB = tableDTOFromDB.getUniqueIndexDTOList();
        // remove primary key from unique index from db
        if ((CollectionUtils.hasValue(primaryKeyDTOList)) && (CollectionUtils.hasValue(uniqueIndexDTOListFromDB))) {
            for (int i = 0; i < uniqueIndexDTOListFromDB.size(); i++) {
                for (PrimaryKeyDTO primaryKeyDTO : primaryKeyDTOList) {
                    if (uniqueIndexDTOListFromDB.get(i).getColumnName().equalsIgnoreCase(primaryKeyDTO.getColumnName())) {
                        uniqueIndexDTOListFromDB.remove(i--);
                        break;
                    }
                }
            }
        }
        Object[] matchedNotMatchedUniqueIndexDTOMap = getMatchedNotMatchedUniqueIndexDTOMap(uniqueIndexDTOListFromDB, tableDTOFromMetadata.getUniqueIndexDTOList());
        List<IndexDTO> uniqueIndexDTOFromDBListNotMatched = (List<IndexDTO>) matchedNotMatchedUniqueIndexDTOMap[0];
        List<IndexDTO> uniqueIndexDTOFromMetadataListNotMatched = (List<IndexDTO>) matchedNotMatchedUniqueIndexDTOMap[1];
        Map<IndexDTO, IndexDTO> matchedUniqueIndexDTOMap = (Map<IndexDTO, IndexDTO>) matchedNotMatchedUniqueIndexDTOMap[2];
        processMatchedUniqueIndexDTO(matchedUniqueIndexDTOMap, resultObject);
        processUniqueIndexDTOFromDBNotMatched(uniqueIndexDTOFromDBListNotMatched, resultObject);
        processUniqueIndexDTOFromMetadataNotMatched(uniqueIndexDTOFromMetadataListNotMatched, resultObject);
    }

    private static void processMatchedUniqueIndexDTO(Map<IndexDTO, IndexDTO> matchedUniqueIndexDTOMap, List<String> resultObject) {
        if (CollectionUtils.hasValue(matchedUniqueIndexDTOMap)) {
            String message = "INFO: --- TYPE: [%s], UNIQUE INDEX NAME: [%s], COLUMN NAME: [%s] --- exists in both metadata and also in database.";
            for (IndexDTO uniqueIndexDTOFromDB : matchedUniqueIndexDTOMap.keySet()) {
                resultObject.add(String.format(message, uniqueIndexDTOFromDB.getType(), uniqueIndexDTOFromDB.getIndexName(), uniqueIndexDTOFromDB.getColumnName()));
            }
        }
    }

    private static void processUniqueIndexDTOFromDBNotMatched(List<IndexDTO> uniqueIndexDTOFromDBNotMatched, List<String> resultObject) {
        if (CollectionUtils.hasValue(uniqueIndexDTOFromDBNotMatched)) {
            String message = "WARNING: --- TYPE: [%s], UNIQUE INDEX NAME: [%s], COLUMN NAME: [%s] --- exists in database but does not exist in metadata.";
            for (IndexDTO uniqueIndexDTO : uniqueIndexDTOFromDBNotMatched) {
                resultObject.add(String.format(message, uniqueIndexDTO.getType(), uniqueIndexDTO.getIndexName(), uniqueIndexDTO.getColumnName()));
            }
        }
    }

    private static void processUniqueIndexDTOFromMetadataNotMatched(List<IndexDTO> uniqueIndexDTOFromMetadataNotMatched, List<String> resultObject) {
        if (CollectionUtils.hasValue(uniqueIndexDTOFromMetadataNotMatched)) {
            String message = "ERROR: --- TYPE: [%s], UNIQUE INDEX NAME: [%s], COLUMN NAME: [%s] --- exists in metadata but does not exist in database.";
            for (IndexDTO uniqueIndexDTO : uniqueIndexDTOFromMetadataNotMatched) {
                resultObject.add(String.format(message, uniqueIndexDTO.getType(), uniqueIndexDTO.getIndexName(), uniqueIndexDTO.getColumnName()));
            }
        }
    }

    // PROCESS SEARCH INDEX DTO
    private static void processMatchedNotMatchedSearchIndexDTO(TableDTO tableDTOFromDB, TableDTO tableDTOFromMetadata, List<String> resultObject) {
        List<IndexDTO> searchIndexDTOListFromDB = tableDTOFromDB.getSearchIndexDTOList();
        Object[] matchedNotMatchedSearchIndexDTOMap = getMatchedNotMatchedSearchIndexDTOMap(searchIndexDTOListFromDB, tableDTOFromMetadata.getSearchIndexDTOList());
        List<IndexDTO> searchIndexDTOFromDBListNotMatched = (List<IndexDTO>) matchedNotMatchedSearchIndexDTOMap[0];
        List<IndexDTO> searchIndexDTOFromMetadataListNotMatched = (List<IndexDTO>) matchedNotMatchedSearchIndexDTOMap[1];
        Map<IndexDTO, IndexDTO> matchedSearchIndexDTOMap = (Map<IndexDTO, IndexDTO>) matchedNotMatchedSearchIndexDTOMap[2];
        processMatchedSearchIndexDTO(matchedSearchIndexDTOMap, resultObject);
        processSearchIndexDTOFromDBNotMatched(searchIndexDTOFromDBListNotMatched, resultObject);
        processSearchIndexDTOFromMetadataNotMatched(searchIndexDTOFromMetadataListNotMatched, resultObject);
    }

    private static void processMatchedSearchIndexDTO(Map<IndexDTO, IndexDTO> matchedSearchIndexDTOMap, List<String> resultObject) {
        if (CollectionUtils.hasValue(matchedSearchIndexDTOMap)) {
            String message = "INFO: --- TYPE: [%s], SEARCH INDEX NAME: [%s], COLUMN NAME: [%s] --- exists in both metadata and also in database.";
            for (IndexDTO searchIndexDTOFromDB : matchedSearchIndexDTOMap.keySet()) {
                resultObject.add(String.format(message, searchIndexDTOFromDB.getType(), searchIndexDTOFromDB.getIndexName(), searchIndexDTOFromDB.getColumnName()));
            }
        }
    }

    private static void processSearchIndexDTOFromDBNotMatched(List<IndexDTO> searchIndexDTOFromDBNotMatched, List<String> resultObject) {
        if (CollectionUtils.hasValue(searchIndexDTOFromDBNotMatched)) {
            String message = "WARNING: --- TYPE: [%s], SEARCH INDEX NAME: [%s], COLUMN NAME: [%s] --- exists in database but does not exist in metadata.";
            for (IndexDTO searchIndexDTO : searchIndexDTOFromDBNotMatched) {
                resultObject.add(String.format(message, searchIndexDTO.getType(), searchIndexDTO.getIndexName(), searchIndexDTO.getColumnName()));
            }
        }
    }

    private static void processSearchIndexDTOFromMetadataNotMatched(List<IndexDTO> searchIndexDTOFromMetadataNotMatched, List<String> resultObject) {
        if (CollectionUtils.hasValue(searchIndexDTOFromMetadataNotMatched)) {
            String message = "ERROR: --- TYPE: [%s], SEARCH INDEX NAME: [%s], COLUMN NAME: [%s] --- exists in metadata but does not exist in database.";
            for (IndexDTO searchIndexDTO : searchIndexDTOFromMetadataNotMatched) {
                resultObject.add(String.format(message, searchIndexDTO.getType(), searchIndexDTO.getIndexName(), searchIndexDTO.getColumnName()));
            }
        }
    }

    // PROCESS FOREIGN KEY
    private static void processMatchedNotMatchedForeignKeyDTO(TableDTO tableDTOFromDB, TableDTO tableDTOFroMetadata,
                                                              List<String> resultObject,Map<String, IDynamicDAO> iDynamicDAOMap,
                                                              ExecutionContext executionContext) {
        Object[] matchedNotMatchedForeignKeyDTOMap = getMatchedNotMatchedForeignKeyDTOMap(tableDTOFromDB.getForeignKeyDTOList(),
                tableDTOFroMetadata.getForeignKeyDTOList(), tableDTOFroMetadata.getColumnDTOList(), iDynamicDAOMap, executionContext);
        List<ForeignKeyDTO> foreignKeyDTOFromDBListNotMatched = (List<ForeignKeyDTO>) matchedNotMatchedForeignKeyDTOMap[0];
        List<ForeignKeyDTO> foreignKeyDTOFromMetadataListNotMatched = (List<ForeignKeyDTO>) matchedNotMatchedForeignKeyDTOMap[1];
        Map<ForeignKeyDTO, ColumnDTO> invalidReferenceBoName = (Map<ForeignKeyDTO, ColumnDTO>) matchedNotMatchedForeignKeyDTOMap[2];
        Map<ForeignKeyDTO, ColumnDTO> invalidReferenceBoAttrValue = (Map<ForeignKeyDTO, ColumnDTO>) matchedNotMatchedForeignKeyDTOMap[3];
        Map<ForeignKeyDTO, ColumnDTO> invalidReferenceBoAttrLabel = (Map<ForeignKeyDTO, ColumnDTO>) matchedNotMatchedForeignKeyDTOMap[4];
        Map<ForeignKeyDTO, ForeignKeyDTO> matchedForeignKeyDTOMap = (Map<ForeignKeyDTO, ForeignKeyDTO>) matchedNotMatchedForeignKeyDTOMap[5];
        processMatchedForeignKeyData(matchedForeignKeyDTOMap, resultObject);
        processForeignKeyDTOFromDBNotMatched(foreignKeyDTOFromDBListNotMatched, resultObject);
        processForeignKeyDTOFromMetadataNotMatched(foreignKeyDTOFromMetadataListNotMatched, resultObject);
        processInvalidReferenceFieldsForForeignKeyDTOFromMetadata(invalidReferenceBoName, DSPDMConstants.BoAttrName.REFERENCE_BO_NAME, resultObject);
        processInvalidReferenceFieldsForForeignKeyDTOFromMetadata(invalidReferenceBoAttrValue, DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_VALUE, resultObject);
        processInvalidReferenceFieldsForForeignKeyDTOFromMetadata(invalidReferenceBoAttrLabel, DSPDMConstants.BoAttrName.REFERENCE_BO_ATTR_LABEL, resultObject);
    }

    private static void processMatchedForeignKeyData(Map<ForeignKeyDTO, ForeignKeyDTO> matchedForeignKeyDTOMap, List<String> resultObject) {
        if (CollectionUtils.hasValue(matchedForeignKeyDTOMap)) {
            String message = "INFO: --- TYPE: [%s], FOREIGN KEY NAME: [%s], COLUMN NAME: [%s] --- exists in both metadata and also in database.";
            for (ForeignKeyDTO foreignKeySchem : matchedForeignKeyDTOMap.keySet()) {
                resultObject.add(String.format(message, foreignKeySchem.getType(), foreignKeySchem.getForeignkeyName(), foreignKeySchem.getColumnName()));
            }
        }
    }

    private static void processForeignKeyDTOFromDBNotMatched(List<ForeignKeyDTO> foreignKeyDTOFromDBNotMatched, List<String> resultObject) {
        if (CollectionUtils.hasValue(foreignKeyDTOFromDBNotMatched)) {
            String message = "WARNING: --- TYPE: [%s], FOREIGN KEY NAME: [%s], PARENT TABLE NAME: [%s] , PARENT COLUMN NAME: [%s], CHILD TABLE NAME: [%s] , CHILD COLUMN NAME: [%s] --- exists in database but does not exist in metadata.";
            for (ForeignKeyDTO foreignKeyDTO : foreignKeyDTOFromDBNotMatched) {
                resultObject.add(String.format(message, foreignKeyDTO.getType(), foreignKeyDTO.getForeignkeyName(),
                        foreignKeyDTO.getTableName(), foreignKeyDTO.getColumnName(), foreignKeyDTO.getForeignkeyTable(),
                        foreignKeyDTO.getForeignkeyColumn()));
            }
        }
    }

    private static void processForeignKeyDTOFromMetadataNotMatched(List<ForeignKeyDTO> foreignKeyDTOFromMetadataNotMatched, List<String> resultObject) {
        if (CollectionUtils.hasValue(foreignKeyDTOFromMetadataNotMatched)) {
            String message = "ERROR: --- TYPE: [%s], FOREIGN KEY NAME: [%s], PARENT TABLE NAME: [%s] , PARENT COLUMN NAME: [%s], CHILD TABLE NAME: [%s] , CHILD COLUMN NAME: [%s] --- exists in metadata but does not exist in database.";
            for (ForeignKeyDTO foreignKeyDTO : foreignKeyDTOFromMetadataNotMatched) {
                resultObject.add(String.format(message, foreignKeyDTO.getType(), foreignKeyDTO.getForeignkeyName(),
                        foreignKeyDTO.getTableName(), foreignKeyDTO.getColumnName(), foreignKeyDTO.getForeignkeyTable(),
                        foreignKeyDTO.getForeignkeyColumn()));
            }
        }
    }

    private static void processInvalidReferenceFieldsForForeignKeyDTOFromMetadata(Map<ForeignKeyDTO, ColumnDTO> invalidReferenceField,
                                                                                  String referenceField, List<String> resultObject) {
        if (CollectionUtils.hasValue(invalidReferenceField)) {
            String message = "ERROR: --- TYPE: [%s], FOREIGN KEY NAME: [%s], PARENT TABLE NAME: [%s] , PARENT COLUMN NAME: " +
                    "[%s], CHILD TABLE NAME: [%s] , CHILD COLUMN NAME: [%s], REFERENCE BO NAME: [%s], REFERENCE BO ATTR VALUE: [%s]," +
                    " REFERENCE BO ATTR LABEL: [%s] --- There is an invalid value for " + referenceField +  " in metadata (BUS OBJECT ATTR Table).";
            for (Map.Entry entry: invalidReferenceField.entrySet()) {
                ForeignKeyDTO foreignKeyDTO = (ForeignKeyDTO) entry.getKey();
                ColumnDTO columnDTO = (ColumnDTO) entry.getValue();
                resultObject.add(String.format(message, foreignKeyDTO.getType(), foreignKeyDTO.getForeignkeyName(),
                        foreignKeyDTO.getTableName(), foreignKeyDTO.getColumnName(), foreignKeyDTO.getForeignkeyTable(),
                        foreignKeyDTO.getForeignkeyColumn(), columnDTO.getReferenceBoName(), columnDTO.getReferenceBoAttrValue(),
                        columnDTO.getReferenceBoAttrLabel()));
            }
        }
    }
}
