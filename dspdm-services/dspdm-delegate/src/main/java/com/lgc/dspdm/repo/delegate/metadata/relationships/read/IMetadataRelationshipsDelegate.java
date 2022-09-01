package com.lgc.dspdm.repo.delegate.metadata.relationships.read;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.util.List;
import java.util.Map;

public interface IMetadataRelationshipsDelegate {
    /**
     * returns all the relationships including parent and child
     *
     * @param boName
     * @param executionContext
     * @return
     */
    public List<DynamicDTO> read(String boName, ExecutionContext executionContext);

    /**
     * returns all the children relationships in a map
     *
     * @param boName
     * @param executionContext
     * @return
     */
    public Map<String, List<DynamicDTO>> readMetadataChildRelationshipsMap(String boName, ExecutionContext executionContext);

    /**
     * returns all the parent relationships in a map
     *
     * @param boName
     * @param executionContext
     * @return
     */
    public Map<String, List<DynamicDTO>> readMetadataParentRelationshipsMap(String boName, ExecutionContext executionContext);

    /**
     * returns bo hierarchy of a given bo
     *
     * @param dynamicDTO
     * @param executionContext
     * @return
     */
    public DynamicDTO readBoHierarchy(DynamicDTO dynamicDTO, ExecutionContext executionContext);
}
