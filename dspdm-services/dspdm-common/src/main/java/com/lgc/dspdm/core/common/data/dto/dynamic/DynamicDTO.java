package com.lgc.dspdm.core.common.data.dto.dynamic;

import com.lgc.dspdm.core.common.data.dto.IBaseDTO;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * Keys stored in this object are case insensitive
 *
 * @author Muhammad Imran Ansari
 * @date 17-Jun-2019
 * <p>
 * Extends with tree map, the order/sequence of the keys is not maintained
 */
public class DynamicDTO extends TreeMap<String, Object> implements IBaseDTO<DynamicPK>, Cloneable{
    /**
     * type of the Business Object etc WELL, AREA
     */
    private String type = null;
    /**
     * To hold primaryt key value
     */
    private DynamicPK id = null;
    /**
     * Primary key column Names from metadata for this business object tupe
     */
    private List<String> primaryKeyColumnNames = null;
    /**
     * Linked Hash set of column names to maintain the order/sequence of columns are per they are being added to the dynamic dto.
     * This is mainly being used after read from database
     */
    private Set<String> columnNamesInOrder = null;
    
    private boolean inserted = false;
    private boolean updated = false;
    private boolean deleted = false;
    private ExecutionContext executionContext = null;
    
    
    public DynamicDTO(String type, List<String> primaryKeyColumnNames, ExecutionContext executionContext) {
        super(String.CASE_INSENSITIVE_ORDER);
        this.type = type;
        this.primaryKeyColumnNames = primaryKeyColumnNames;
        this.executionContext = executionContext;
    }
    
    public DynamicDTO(DynamicDTO dynamicDTO, ExecutionContext executionContext) {
        this(dynamicDTO.type, dynamicDTO.primaryKeyColumnNames, executionContext);
        this.id = dynamicDTO.id;
        this.columnNamesInOrder = dynamicDTO.columnNamesInOrder;
        this.putAll(dynamicDTO);
    }
    
    public String getType() {
        return type;
    }
    
    @Override
    public Object get(Object key) {
        return super.get(key);
    }
    
    @Override
    public Object put(String key, Object value) {
        if (columnNamesInOrder != null) {
            columnNamesInOrder.add(key);
        }
        return super.put(key, value);
    }
    
    public Object putWithOrder(String key, Object value) {
        if (columnNamesInOrder == null) {
            columnNamesInOrder = new LinkedHashSet<>();
            keySet().forEach(columnNamesInOrder::add);
        }
        columnNamesInOrder.add(key);
        return super.put(key, value);
    }
    
    @Override
    public DynamicPK getId() {
        if (id == null) {
            if (CollectionUtils.hasValue(primaryKeyColumnNames)) {
                id = getPrimaryKey(primaryKeyColumnNames);
            }
        }
        return id;
    }
    
    private DynamicPK getPrimaryKey(List<String> primaryKeyColumnNames) {
        
        if (id == null) {
            if (CollectionUtils.hasValue(primaryKeyColumnNames)) {
                List<Object> pkValues = new ArrayList<>(primaryKeyColumnNames.size());
                
                Object value = null;
                for (String primaryKeyColumnName : primaryKeyColumnNames) {
                    value = this.get(primaryKeyColumnName);
                    if (value != null) {
                        pkValues.add(value);
                    } else {
                        // break if any value against pk column is null. No need to process more
                        break;
                    }
                }
                // add to set and return if and only if all the pk values are present in the object
                if (pkValues.size() == primaryKeyColumnNames.size()) {
                    id = new DynamicPK(type, pkValues.toArray());
                }
            }
        }
        return id;
    }
    
    public Set<String> getColumnNamesInOrder() {
        return columnNamesInOrder;
    }
    
    public List<String> getPrimaryKeyColumnNames() {
        return primaryKeyColumnNames;
    }
    
    public void setPrimaryKeyColumnNames(List<String> primaryKeyColumnNames) {
        this.primaryKeyColumnNames = primaryKeyColumnNames;
    }

    public ExecutionContext getExecutionContext() {
        return executionContext;
    }

    public boolean isInserted() {
        return inserted;
    }
    
    public void setInserted(boolean inserted) {
        this.inserted = inserted;
    }
    
    public boolean isUpdated() {
        return updated;
    }
    
    public void setUpdated(boolean updated) {
        this.updated = updated;
    }
    
    public boolean isDeleted() {
        return deleted;
    }
    
    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DynamicDTO{");
        sb.append("type=").append(type);
        sb.append(", ").append("id=").append(id);
        sb.append(", ").append("inserted=").append(inserted);
        sb.append(", ").append("updated=").append(updated);
        sb.append(", ").append("deleted=").append(deleted);
        for (String key : this.keySet()) {
            sb.append(", ").append(key + "=").append(get(key));
        }
        sb.append('}');
        return sb.toString();
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DynamicDTO)) {
            return false;
        }
        DynamicDTO that = (DynamicDTO) o;
        if (!Objects.equals(type, that.type)) {
            return false;
        }
        if (!Objects.equals(id, that.id)) {
            return false;
        }
        return super.equals(o);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(type, id, super.hashCode());
    }
}
