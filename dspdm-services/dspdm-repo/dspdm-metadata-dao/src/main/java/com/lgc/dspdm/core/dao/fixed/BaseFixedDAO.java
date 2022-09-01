package com.lgc.dspdm.core.dao.fixed;

import com.lgc.dspdm.core.common.data.annotation.AnnotationProcessor;
import com.lgc.dspdm.core.common.data.dto.IBaseDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.*;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.ReflectionUtils;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.core.dao.BaseDAO;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Fixed DAO means fix/static structure based classes DAO
 *
 * @author Muhammad Imran Ansari
 */
public abstract class BaseFixedDAO extends BaseDAO {
    private static DSPDMLogger logger = new DSPDMLogger(BaseFixedDAO.class);

    public static <T extends IBaseDTO> T extractFixedDTOFromResultSet(ResultSet rs, T dto, ExecutionContext executionContext) {
        Field[] fields = dto.getClass().getDeclaredFields();
        String columnName = null;
        Class columnType = null;
        Object value = null;
        Method setter = null;
        for (Field field : fields) {
            if ((Modifier.isTransient(field.getModifiers())) || (Modifier.isStatic(field.getModifiers()))) {
                continue;
            }
            columnName = AnnotationProcessor.getDatabaseColumnName(field);
            columnType = AnnotationProcessor.getDatabaseColumnType(field);
            if (columnName != null) {
                try {
                    value = rs.getObject(columnName, columnType);
                    if (value != null) {
                        if ((field.getType() == Boolean.class) && (columnType == Character.class)) {
                            Character character = (Character) value;
                            if ((character.charValue() == 'Y') || (character.charValue() == 'y')) {
                                value = Boolean.TRUE;
                            } else {
                                value = Boolean.FALSE;
                            }
                        }
                    }
                    setter = ReflectionUtils.getSetterForField(field, executionContext);
                    setter.invoke(dto, value);
                } catch (Exception e) {
                    DSPDMException.throwException(e, executionContext);
                }
            }
        }
        return dto;
    }

    public static TableDTO extractFixedTableDTOFromResultSet(ResultSet rs, ExecutionContext executionContext) {
        TableDTO tableDTO = new TableDTO();
        try {
            if (StringUtils.hasValue(rs.getString(DSPDMConstants.SchemaName.TABLE_NAME))) {
                tableDTO.setType(rs.getString(DSPDMConstants.SchemaName.TABLE_TYPE));
                tableDTO.setSchema(rs.getString(DSPDMConstants.SchemaName.TABLE_SCHEMA));
                tableDTO.setTableName(rs.getString(DSPDMConstants.SchemaName.TABLE_NAME));
                tableDTO.setRemarks(rs.getString(DSPDMConstants.SchemaName.REMARKS));
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return tableDTO;
    }

    public static List<ColumnDTO> extractFixedColumnDTOFromResultSet(ResultSet rs, Boolean isMSSQLServerDialect, ExecutionContext executionContext) {
        List<ColumnDTO> columnDTOList = new ArrayList<>();
        try {
            while (rs.next()) {
                if (StringUtils.hasValue(rs.getString(DSPDMConstants.SchemaName.TABLE_NAME))
                        && StringUtils.hasValue(rs.getString(DSPDMConstants.SchemaName.COLUMN_NAME))) {
                    ColumnDTO columnDTO = new ColumnDTO();
                    columnDTO.setSchema(rs.getString(DSPDMConstants.SchemaName.TABLE_SCHEMA));
                    columnDTO.setTableName(rs.getString(DSPDMConstants.SchemaName.TABLE_NAME).toUpperCase());
                    columnDTO.setColumnName(rs.getString(DSPDMConstants.SchemaName.COLUMN_NAME).toUpperCase());
                    columnDTO.setDataType(
                            DSPDMConstants.DataTypes.getDataTypeFromSQLDataType(rs.getInt(DSPDMConstants.SchemaName.SQL_DATA_TYPE))
                            .getAttributeDataType(isMSSQLServerDialect).toUpperCase());
                    columnDTO.setColumnSize(rs.getInt(DSPDMConstants.SchemaName.COLUMN_SIZE));
                    columnDTO.setDecimalDigits(rs.getInt(DSPDMConstants.SchemaName.DECIMAL_DIGITS));
                    columnDTO.setColumnIndex(rs.getInt(DSPDMConstants.SchemaName.ORDINAL_POSITION));
                    columnDTO.setDefaultValue(rs.getString(DSPDMConstants.SchemaName.DEFAULT_VALUE));
                    if(StringUtils.hasValue(rs.getString(DSPDMConstants.SchemaName.IS_NULLABLE)) &&
                            (rs.getString(DSPDMConstants.SchemaName.IS_NULLABLE).equalsIgnoreCase("YES")
                            || rs.getString(DSPDMConstants.SchemaName.IS_NULLABLE).equalsIgnoreCase("true")
                            || rs.getString(DSPDMConstants.SchemaName.IS_NULLABLE).equalsIgnoreCase("1"))){
                        columnDTO.setNullAble(true);
                    }else{
                        columnDTO.setNullAble(false);;
                    }
                    columnDTO.setType(DSPDMConstants.SchemaName.COLUMN);
                    columnDTOList.add(columnDTO);
                }
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return columnDTOList;
    }

    public static List<PrimaryKeyDTO> extractFixedPrimaryKeyDTOFromResultSet(ResultSet rs, ExecutionContext executionContext) {
        List<PrimaryKeyDTO> primaryKeyDTOList = new ArrayList<>();
        try {
            while (rs.next()) {
                if (StringUtils.hasValue(rs.getString(DSPDMConstants.SchemaName.TABLE_NAME))
                        && StringUtils.hasValue(rs.getString(DSPDMConstants.SchemaName.COLUMN_NAME))) {
                    PrimaryKeyDTO primaryKeyDTO = new PrimaryKeyDTO();
                    primaryKeyDTO.setSchema(rs.getString(DSPDMConstants.SchemaName.TABLE_SCHEMA));
                    primaryKeyDTO.setTableName(rs.getString(DSPDMConstants.SchemaName.TABLE_NAME).toUpperCase());
                    primaryKeyDTO.setColumnName(rs.getString(DSPDMConstants.SchemaName.COLUMN_NAME).toUpperCase());
                    primaryKeyDTO.setPrimarykeyName(rs.getString(DSPDMConstants.SchemaName.PRIMARYKEY_NAME));
                    primaryKeyDTO.setColumnIndex(rs.getInt(DSPDMConstants.SchemaName.KEY_SEQ));
                    primaryKeyDTO.setType(DSPDMConstants.SchemaName.PRIMARYKEY);
                    primaryKeyDTOList.add(primaryKeyDTO);
                }
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return primaryKeyDTOList;
    }

    public static List<IndexDTO> extractFixedIndexDTOFromResultSet(ResultSet rs, ExecutionContext executionContext) {
        List<IndexDTO> indexDTOList = new ArrayList<>();
        try {
            while (rs.next()) {
                if (StringUtils.hasValue(rs.getString(DSPDMConstants.SchemaName.TABLE_NAME))
                        && StringUtils.hasValue(rs.getString(DSPDMConstants.SchemaName.COLUMN_NAME))) {
                    IndexDTO indexDTO = new IndexDTO();
                    indexDTO.setSchema(rs.getString(DSPDMConstants.SchemaName.TABLE_SCHEMA));
                    indexDTO.setTableName(rs.getString(DSPDMConstants.SchemaName.TABLE_NAME).toUpperCase());
                    indexDTO.setColumnName(rs.getString(DSPDMConstants.SchemaName.COLUMN_NAME).toUpperCase());
                    indexDTO.setIndexName(rs.getString(DSPDMConstants.SchemaName.INDEX_NAME).toUpperCase());
                    indexDTO.setColumnIndex(rs.getInt(DSPDMConstants.SchemaName.ORDINAL_POSITION));
                    indexDTO.setIndexType(rs.getString(DSPDMConstants.SchemaName.INDEX_TYPE));
                    indexDTO.setNonUnique(rs.getBoolean(DSPDMConstants.SchemaName.NON_UNIQUE));
                    indexDTO.setType(DSPDMConstants.SchemaName.INDEX);
                    indexDTOList.add(indexDTO);
                }
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return indexDTOList;
    }

    public static List<ForeignKeyDTO> extractFixedForeignKeyDTOFromResultSet(ResultSet rs, ExecutionContext executionContext) {
        List<ForeignKeyDTO> foreignKeyDTOList = new ArrayList<>();
        try {
            while (rs.next()) {
                if (StringUtils.hasValue(rs.getString(DSPDMConstants.SchemaName.PKTABLE_NAME))
                        && StringUtils.hasValue(rs.getString(DSPDMConstants.SchemaName.PKCOLUMN_NAME))
                        && StringUtils.hasValue(rs.getString(DSPDMConstants.SchemaName.FOREIGNKEY_NAME))
                        && StringUtils.hasValue(rs.getString(DSPDMConstants.SchemaName.FOREIGNKEY_TABLE))
                        && StringUtils.hasValue(rs.getString(DSPDMConstants.SchemaName.FOREIGNKEY_COLUMN))) {
                    ForeignKeyDTO foreignKeyDTO = new ForeignKeyDTO();
                    foreignKeyDTO.setSchema(rs.getString(DSPDMConstants.SchemaName.PKTABLE_SCHEMA));
                    foreignKeyDTO.setTableName(rs.getString(DSPDMConstants.SchemaName.PKTABLE_NAME).toUpperCase());
                    foreignKeyDTO.setColumnName(rs.getString(DSPDMConstants.SchemaName.PKCOLUMN_NAME).toUpperCase());
                    foreignKeyDTO.setColumnIndex(rs.getInt(DSPDMConstants.SchemaName.KEY_SEQ));
                    foreignKeyDTO.setForeignkeyName(rs.getString(DSPDMConstants.SchemaName.FOREIGNKEY_NAME).toUpperCase());
                    foreignKeyDTO.setForeignkeyTable(rs.getString(DSPDMConstants.SchemaName.FOREIGNKEY_TABLE).toUpperCase());
                    foreignKeyDTO.setForeignkeyColumn(rs.getString(DSPDMConstants.SchemaName.FOREIGNKEY_COLUMN).toUpperCase());
                    foreignKeyDTO.setType(DSPDMConstants.SchemaName.FOREIGNKEY);
                    foreignKeyDTOList.add(foreignKeyDTO);
                }
            }
        } catch (Exception e) {
            DSPDMException.throwException(e, executionContext);
        }
        return foreignKeyDTOList;
    }
}
