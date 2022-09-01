package com.lgc.dspdm.repo.delegate;

import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.repo.delegate.area.AreaDelegateImpl;
import com.lgc.dspdm.repo.delegate.area.IAreaDelegate;
import com.lgc.dspdm.repo.delegate.common.read.BusinessObjectReadDelegate;
import com.lgc.dspdm.repo.delegate.common.read.IBusinessObjectReadDelegate;
import com.lgc.dspdm.repo.delegate.common.write.BusinessObjectWriteDelegate;
import com.lgc.dspdm.repo.delegate.common.write.IBusinessObjectWriteDelegate;
import com.lgc.dspdm.repo.delegate.custom.CustomDelegate;
import com.lgc.dspdm.repo.delegate.custom.ICustomDelegate;
import com.lgc.dspdm.repo.delegate.entitytype.EntityTypeDelegate;
import com.lgc.dspdm.repo.delegate.entitytype.IEntityTypeDelegate;
import com.lgc.dspdm.repo.delegate.metadata.bo.read.IMetadataBODelegate;
import com.lgc.dspdm.repo.delegate.metadata.bo.read.MetadataBODelegate;
import com.lgc.dspdm.repo.delegate.metadata.boattr.read.IMetadataBOAttrReadDelegate;
import com.lgc.dspdm.repo.delegate.metadata.boattr.read.MetadataBOAttrReadDelegate;
import com.lgc.dspdm.repo.delegate.metadata.boattr.write.IMetadataBOAttrWriteDelegate;
import com.lgc.dspdm.repo.delegate.metadata.boattr.write.MetadataBOAttrWriteDelegate;
import com.lgc.dspdm.repo.delegate.metadata.bosearch.BOSearchDelegate;
import com.lgc.dspdm.repo.delegate.metadata.bosearch.IBOSearchDelegate;
import com.lgc.dspdm.repo.delegate.metadata.config.IMetadataConfigDelegate;
import com.lgc.dspdm.repo.delegate.metadata.config.MetadataConfigDelegate;
import com.lgc.dspdm.repo.delegate.metadata.relationships.read.IMetadataRelationshipsDelegate;
import com.lgc.dspdm.repo.delegate.metadata.relationships.read.MetadataRelationshipsDelegate;
import com.lgc.dspdm.repo.delegate.metadata.searchindexes.read.read.IMetadataSearchIndexesDelegate;
import com.lgc.dspdm.repo.delegate.metadata.searchindexes.read.read.MetadataSearchIndexesDelegate;
import com.lgc.dspdm.repo.delegate.metadata.uniqueconstraints.read.IMetadataConstraintsDelegate;
import com.lgc.dspdm.repo.delegate.metadata.uniqueconstraints.read.MetadataConstraintsDelegate;
import com.lgc.dspdm.repo.delegate.referencedata.read.IReferenceDataReadDelegate;
import com.lgc.dspdm.repo.delegate.referencedata.read.ReferenceDataReadDelegate;

public class BusinessDelegateFactory {
    
    public static IMetadataConfigDelegate getMetadataReadDelegate(ExecutionContext executionContext) {
        return MetadataConfigDelegate.getInstance(executionContext);
    }
    
    public static IMetadataBODelegate getMetadataBODelegate(ExecutionContext executionContext) {
        return MetadataBODelegate.getInstance(executionContext);
    }
    
    public static IMetadataBOAttrReadDelegate getMetadataBOAttrReadDelegate(ExecutionContext executionContext) {
        return MetadataBOAttrReadDelegate.getInstance(executionContext);
    }
    
    public static IMetadataBOAttrWriteDelegate getMetadataBOAttrWriteDelegate(ExecutionContext executionContext) {
        return MetadataBOAttrWriteDelegate.getInstance(executionContext);
    }
    
    public static IMetadataConstraintsDelegate getMetadataConstraintsReadDelegate(ExecutionContext executionContext) {
        return MetadataConstraintsDelegate.getInstance(executionContext);
    }

    public static IMetadataSearchIndexesDelegate getMetadataSearchIndexesReadDelegate(ExecutionContext executionContext) {
        return MetadataSearchIndexesDelegate.getInstance(executionContext);
    }
    
    public static IMetadataRelationshipsDelegate getMetadataRelationshipsDelegate(ExecutionContext executionContext) {
        return MetadataRelationshipsDelegate.getInstance(executionContext);
    }
    
    public static IReferenceDataReadDelegate getReferenceDataReadDelegate(ExecutionContext executionContext) {
        return ReferenceDataReadDelegate.getInstance(executionContext);
    }
    
    public static IBusinessObjectReadDelegate getBusinessObjectReadDelegate(ExecutionContext executionContext) {
        return BusinessObjectReadDelegate.getInstance(executionContext);
    }
    
    public static IBusinessObjectWriteDelegate getBusinessObjectWriteDelegate(ExecutionContext executionContext) {
        return BusinessObjectWriteDelegate.getInstance(executionContext);
    }
    
    public static IAreaDelegate getAreaDelegate(ExecutionContext executionContext) {
        // internal calls only no call from service
        return AreaDelegateImpl.getInstance(executionContext);
    }

    public static IBOSearchDelegate getBOSearchDelegate(ExecutionContext executionContext) {
        return BOSearchDelegate.getInstance(executionContext);
    }

    public static ICustomDelegate getCustomDelegate(ExecutionContext executionContext) {
        return CustomDelegate.getInstance(executionContext);
    }

    public static IEntityTypeDelegate getEntityTypeDelegate(ExecutionContext executionContext) {
        return EntityTypeDelegate.getInstance(executionContext);
    }
}
