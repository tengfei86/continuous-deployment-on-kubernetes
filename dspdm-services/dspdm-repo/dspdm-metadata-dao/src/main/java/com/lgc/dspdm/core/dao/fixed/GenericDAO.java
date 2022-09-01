package com.lgc.dspdm.core.dao.fixed;

import com.lgc.dspdm.core.common.data.annotation.AnnotationProcessor;
import com.lgc.dspdm.core.common.data.dto.IBaseDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.*;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.core.dao.IGenericDAO;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class GenericDAO extends BaseFixedDAO implements IGenericDAO {
    private static DSPDMLogger logger = new DSPDMLogger(GenericDAO.class);
    private static GenericDAO singleton = null;

    private GenericDAO() {

    }

    public static GenericDAO getInstance() {
        if (singleton == null) {
            singleton = new GenericDAO();
        }
        return singleton;
    }

    @Override
    protected String getType() {
        return null;
    }

    @Override
    public List<TableDTO> findTableDTO(ExecutionContext executionContext) {
        List<TableDTO> list = new ArrayList<>();
        Connection dataModelDBConnection = null;
        Connection serviceDBConnection = null;
        try{
            // read metadata from data model db
            dataModelDBConnection = getDataModelDBReadOnlyConnection(executionContext);
            list.addAll(findTableDTOsFromDB(dataModelDBConnection, executionContext));
            // read metadata from service db
            serviceDBConnection = getServiceDBReadOnlyConnection(executionContext);
            list.addAll(findTableDTOsFromDB(serviceDBConnection, executionContext));
        }catch(Exception e){
            DSPDMException.throwException(e, executionContext);
        }finally {
            closeDataModelDBConnection(dataModelDBConnection, executionContext);
            closeServiceDBConnection(serviceDBConnection, executionContext);
        }
        return list;
    }

    private List<TableDTO> findTableDTOsFromDB(Connection connection, ExecutionContext executionContext) {
        List<TableDTO> tableDTOsListFromDB = new ArrayList<>();
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            if (!StringUtils.containsIgnoreCase(metaData.getDriverName(), DSPDMConstants.SQL.SQLDialect.POSTGRES)) {
                // means data model db is of sqlserver type
                pstmt = prepareStatement(String.format("select sequence_name as TABLE_NAME, 'SEQUENCE' as TABLE_TYPE, '%s' as TABLE_SCHEM, null as REMARKS " +
                                "from information_schema.sequences where sequence_schema = '%s';"
                        , connection.getSchema(), connection.getSchema()), 0, getType(), executionContext, connection);
                resultSet = pstmt.executeQuery();
                while (resultSet.next()) {
                    TableDTO dto = extractFixedTableDTOFromResultSet(resultSet, executionContext);
                    tableDTOsListFromDB.add(dto);
                }
            }
            resultSet = metaData.getTables(null, connection.getSchema(), null
                    , new String[]{"TABLE", "VIEW", "SEQUENCE"});
            while (resultSet.next()) {
                TableDTO dto = extractFixedTableDTOFromResultSet(resultSet, executionContext);
                fillTableDTODetailsFromMetadata(dto, metaData, isMSSQLServerDialect(dto.getTableName()), executionContext);
                tableDTOsListFromDB.add(dto);
            }
            logger.info("{} tables metadata has been read from db successfully.", tableDTOsListFromDB.size());
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeResultSet(resultSet, executionContext);
            closeStatement(pstmt, executionContext);
        }
        return tableDTOsListFromDB;
    }

    public List<TableDTO> findTableDTOForTableName(String boName, String tableName, ExecutionContext executionContext) {
        List<TableDTO> list = new ArrayList<>();
        Connection dataModelDBConnection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        long startTime = 0;
        try {
            startTime = System.currentTimeMillis();
            dataModelDBConnection = getDataModelDBReadOnlyConnection(executionContext);

            DatabaseMetaData metaData = dataModelDBConnection.getMetaData();
            // first get sequence and add if it exists
            TableDTO sequenceTableDTO = getSequenceTableDTO(boName, tableName, dataModelDBConnection.getSchema(), metaData, dataModelDBConnection, executionContext);
            if (sequenceTableDTO != null) {
                list.add(sequenceTableDTO);
            }
            // now start reading table metadata
            boolean tableMetadataFound = false;
            // first check table name with lower case
            resultSet = metaData.getTables(null, dataModelDBConnection.getSchema(), tableName.trim().toLowerCase()
                    , new String[]{"TABLE", "VIEW","SEQUENCE"});
            if (resultSet.next()) {
                tableMetadataFound = true;
            } else {
                // table name with lower case not found. now try with upper case
                // close old result set before opening a new one
                closeResultSet(resultSet, executionContext);
                // read again
                resultSet = metaData.getTables(null, dataModelDBConnection.getSchema(), tableName.trim().toUpperCase()
                        , new String[]{"TABLE", "VIEW", "SEQUENCE"});
                if (resultSet.next()) {
                    tableMetadataFound = true;
                }
            }
            if (tableMetadataFound) {
                // result set cursor is at current row and already incremented
                do {
                    TableDTO dto = extractFixedTableDTOFromResultSet(resultSet, executionContext);
                    fillTableDTODetailsFromMetadata(dto, metaData, isMSSQLServerDialect(boName), executionContext);
                    list.add(dto);
                } while (resultSet.next());
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeResultSet(resultSet, executionContext);
            closeDataModelDBConnection(dataModelDBConnection, executionContext);
        }
        return list;
    }

    private TableDTO getSequenceTableDTO(String boName, String tableName, String schemaName, DatabaseMetaData metaData,
                                         Connection connection, ExecutionContext executionContext) throws SQLException {
        TableDTO tableDTO = null;
        if (isMSSQLServerDialect(boName)) {
            tableDTO = getSequenceTableDTOForMSSQLServer(tableName, schemaName, connection, executionContext);
        } else {
            tableDTO = getSequenceTableDTOForPostgres(tableName, schemaName, metaData, executionContext);
        }
        return tableDTO;
    }

    private TableDTO getSequenceTableDTOForMSSQLServer(String tableName, String schemaName, Connection connection,
                                                       ExecutionContext executionContext) throws SQLException {
        TableDTO tableDTO = null;
        String sequenceName = "SEQ_" + tableName.trim().toUpperCase();
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            String sql = String.format("select sequence_name as TABLE_NAME, 'SEQUENCE' as TABLE_TYPE, '%s' as TABLE_SCHEM, null as REMARKS " +
                            "from information_schema.sequences where UPPER(sequence_name) = '%s'"
                    , schemaName, sequenceName);
            if(StringUtils.hasValue(schemaName)) {
                sql = sql + String.format(" and sequence_schema = '%s'", schemaName);
            }
            sql+=";";
            logSQL(sql, executionContext);
            pstmt = prepareStatement(sql, 0, tableName,executionContext, connection);
            resultSet = pstmt.executeQuery();
            if (resultSet.next()) {
                tableDTO = extractFixedTableDTOFromResultSet(resultSet, executionContext);
            }
        } finally {
            closeResultSet(resultSet, executionContext);
            closeStatement(pstmt, executionContext);
        }
        return tableDTO;
    }

    private TableDTO getSequenceTableDTOForPostgres(String tableName, String schemaName, DatabaseMetaData metaData,
                                                    ExecutionContext executionContext) throws SQLException {
        TableDTO tableDTO = null;
        // try with upper case first
        String sequenceName = "SEQ_" + tableName.trim().toUpperCase();
        ResultSet resultSet = null;
        try {
            resultSet = metaData.getTables(null, schemaName, sequenceName, new String[]{"SEQUENCE"});
            if (resultSet.next()) {
                tableDTO = extractFixedTableDTOFromResultSet(resultSet, executionContext);
            } else {
                closeResultSet(resultSet, executionContext);
                // try with lower case now
                sequenceName = "seq_" + tableName.trim().toLowerCase();
                resultSet = metaData.getTables(null, schemaName, sequenceName, new String[]{"SEQUENCE"});
                if (resultSet.next()) {
                    tableDTO = extractFixedTableDTOFromResultSet(resultSet, executionContext);
                }
            }
        } finally {
            closeResultSet(resultSet, executionContext);
        }
        return tableDTO;
    }

    private static void fillTableDTODetailsFromMetadata(TableDTO tableDTO, DatabaseMetaData metaData, Boolean isMSSQLServerDialect,
                                                        ExecutionContext executionContext) throws SQLException {
        // Column
        tableDTO.setColumnDTOList(extractFixedColumnDTOFromResultSet(metaData.getColumns(null
                , tableDTO.getSchema(), tableDTO.getTableName(), null)
                , isMSSQLServerDialect, executionContext));

        if (tableDTO.getColumnDTOList().size() > 0) {
            // Primary Key
            tableDTO.setPrimaryKeyDTOList(extractFixedPrimaryKeyDTOFromResultSet(metaData.getPrimaryKeys(null
                    , tableDTO.getSchema(), tableDTO.getTableName())
                    , executionContext));
            if (tableDTO.getPrimaryKeyDTOList().size() > 0) {
                for (PrimaryKeyDTO pkDTO : tableDTO.getPrimaryKeyDTOList()) {
                    tableDTO.getColumnDTOList().stream().filter(c -> c.getColumnName()
                            .equals(pkDTO.getColumnName()))
                            .collect(Collectors.toList())
                            .forEach((ColumnDTO c) -> c.setPrimarykey(true));
                }
            }

            // Foreign Key
            // Add foreign key from imported keys only.
            tableDTO.setForeignKeyDTOList(extractFixedForeignKeyDTOFromResultSet(metaData.getImportedKeys(null
                    , tableDTO.getSchema(), tableDTO.getTableName())
                    , executionContext));

            // Unique Index
            tableDTO.setUniqueIndexDTOList(extractFixedIndexDTOFromResultSet(metaData.getIndexInfo(null
                    , tableDTO.getSchema(), tableDTO.getTableName(), true, true)
                    , executionContext));
            if (tableDTO.getUniqueIndexDTOList().size() > 0 && tableDTO.getPrimaryKeyDTOList().size() > 0) {
                for (PrimaryKeyDTO pkDTO : tableDTO.getPrimaryKeyDTOList()) {
                    tableDTO.getUniqueIndexDTOList().stream().filter(c -> c.getIndexName()
                            .equals(pkDTO.getPrimarykeyName()))
                            .collect(Collectors.toList())
                            .forEach((IndexDTO c) -> c.setIndexType(DSPDMConstants.SchemaName.PRIMARYKEY));
                }
            }

            // Search Index
            List<IndexDTO> searchIndexDTOs = extractFixedIndexDTOFromResultSet(metaData.getIndexInfo(null
                    , tableDTO.getSchema(), tableDTO.getTableName(), false, true)
                    , executionContext);
            if (CollectionUtils.hasValue(searchIndexDTOs)) {
                // removing all non_unique = false from dto list because when we've provided unique = false in
                // metadata.getIndexInfo() method, metadata has given us both unique and non-unique indexes
                // so if we remove non_unique = false, we'll get search indexes
                tableDTO.setSearchIndexDTOList(searchIndexDTOs.stream().filter(c -> c.getNonUnique()
                        .equals(true))
                        .collect(Collectors.toList()));
            }
        }
    }

    @Override
    public <T extends IBaseDTO> List<T> findAll(Class<T> c, List<String> orderBy, ExecutionContext executionContext) {
        List<T> list = new ArrayList<>();
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        String sql = null;
        String finalSQL = null;
        long startTime = 0;
        String boName = AnnotationProcessor.getBOName(c, executionContext);
        try {
            sql = "SELECT * FROM " + AnnotationProcessor.getDatabaseTableName(c, executionContext);

            if (CollectionUtils.hasValue(orderBy)) {
                sql = sql + " ORDER BY ";
                for (int i = 0; i < orderBy.size(); i++) {
                    sql = sql + orderBy.get(i);
                    if (i < orderBy.size() - 1) {
                        sql = sql + ",";
                    }
                }
            }
            connection = getReadOnlyConnection(boName, executionContext);
            pstmt = prepareStatement(sql, 0, getType(), executionContext, connection);
            // collect sql script only if required
            if (executionContext.getCollectSQLScriptOptions().isReadEnabled()) {
                executionContext.addSqlScript(sql);
            }
            finalSQL = logSQL(sql, executionContext);
            startTime = System.currentTimeMillis();
            resultSet = pstmt.executeQuery();
            logSQLTimeTaken(finalSQL, 1, System.currentTimeMillis() - startTime, executionContext);
            while (resultSet.next()) {
                T dto = extractFixedDTOFromResultSet(resultSet, c.getDeclaredConstructor().newInstance(), executionContext);
                list.add(dto);
            }
        } catch (Exception e) {
            if (finalSQL != null) {
                logSQLTimeTaken(finalSQL, 1, (System.currentTimeMillis() - startTime), executionContext);
            } else {
                // add to sql stats
                logSQLTimeTaken(sql, 1, 0, executionContext);
                // log in error logs console
                logSQL(sql, executionContext);
            }
            DSPDMException.throwException(e, executionContext);
        } finally {
            closeResultSet(resultSet, executionContext);
            closeStatement(pstmt, executionContext);
            closeConnection(boName, connection, executionContext);
        }
        return list;
    }
}
