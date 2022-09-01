package com.lgc.dspdm.core.dao;

import com.lgc.dspdm.core.common.data.dto.IBaseDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.TableDTO;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.util.List;

public interface IGenericDAO {
    
    public <T extends IBaseDTO> List<T> findAll(Class<T> c, List<String> orderBy, ExecutionContext executionContext);

    public List<TableDTO> findTableDTO(ExecutionContext executionContext);
    
//    T findOne(final Object id);    

//    T create(final T entity);

//    T update(final T entity);

//    void delete(final T entity);

//    T deleteById(final Long entityId);
}

