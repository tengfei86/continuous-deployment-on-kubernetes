package com.lgc.dspdm.core.dao.dynamic;

import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectDTO;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

public interface IMetadataStructureChangeDAOImpl {
    public SaveResultDTO addPhysicalColumnInDatabaseTable(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext);
    public SaveResultDTO updatePhysicalColumnInDatabaseTable(DynamicDTO dynamicDTO,String columnPropertyNameToUpdate, Connection connection, ExecutionContext executionContext);
    public SaveResultDTO deletePhysicalColumnFromDatabaseTable(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext);
    public SaveResultDTO addPhysicalTableInDatabase(DynamicDTO businessObjectDynamicDTO, List<DynamicDTO> businessObjectAttributes, List<DynamicDTO> businessObjectRelationships_FK,List<DynamicDTO> uniqueConstraints, List<DynamicDTO> searchIndexes, Connection connection, ExecutionContext executionContext);
    public SaveResultDTO deletePhysicalTableFromDatabase(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext);
    public SaveResultDTO deletePhysicalRelationshipFromDatabase(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext);
    public SaveResultDTO addPhysicalRelationshipToDatabase(List<DynamicDTO> relationships, Connection connection, ExecutionContext executionContext);
    public SaveResultDTO addPhysicalUniqueConstraintToDatabase(List<DynamicDTO> uniqueConstraints, Connection connection, ExecutionContext executionContext);
    public SaveResultDTO addPhysicalSearchIndexToDatabase(List<DynamicDTO> uniqueConstraints, Connection connection, ExecutionContext executionContext);
    public SaveResultDTO deletePhysicalUniqueConstraintFromDatabase(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext);
    public SaveResultDTO deletePhysicalSearchIndexFromDatabase(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext);
    public SaveResultDTO dropDefaultConstraintForMSSQLServerOnly(DynamicDTO dynamicDTO, Connection connection, ExecutionContext executionContext);
    public List<String[]> getAttributesVsUnitMapping(String boName, Connection connection, ExecutionContext executionContext);
    public List<DynamicDTO> restoreBOGroupFromHistory(List<DynamicDTO> boList, Connection connection, ExecutionContext executionContext);
}
