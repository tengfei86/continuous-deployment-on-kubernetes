package com.lgc.dspdm.repo.delegate.metadata.boattr.write;

import com.lgc.dspdm.core.common.data.common.SaveResultDTO;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.repo.delegate.BaseTransactionalDelegate;
import com.lgc.dspdm.repo.delegate.metadata.config.MetadataConfigDelegateImpl;

import java.util.List;
import java.util.Set;

/**
 * @author Muhammad Imran Ansari
 */
public class MetadataBOAttrWriteDelegate extends BaseTransactionalDelegate implements IMetadataBOAttrWriteDelegate {
    private static IMetadataBOAttrWriteDelegate singleton = null;

    private MetadataBOAttrWriteDelegate(ExecutionContext executionContext) {
        super(executionContext);
    }

    public static IMetadataBOAttrWriteDelegate getInstance(ExecutionContext executionContext) {
        if (singleton == null) {
            singleton = new MetadataBOAttrWriteDelegate(executionContext);
        }
        return singleton;
    }

    /* ************************************************ */
    /* ************** BUSINESS METHODS **************** */
    /* ************************************************ */

    @Override
    public SaveResultDTO addCustomAttributes(List<DynamicDTO> boListToSave, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL SAVE OR UPDATE for all the dynamic dto objects in the map
            saveResultDTO = MetadataBOAttrWriteDelegateImpl.getInstance().addCustomAttributes(boListToSave, executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        // following block should have a separate try and catch block
        // On any exception from the following code should not invoke roll back transaction
        try {
            // refresh metadata after successful add custom attribute
            MetadataConfigDelegateImpl.getInstance().refreshMetadata(executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(new DSPDMException("Custom attribute(s) added successfully but error occurred while refreshing metadata. " + e.getMessage(),
                    e, executionContext.getExecutorLocale()), executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO updateCustomAttributes(List<DynamicDTO> boListToUpdate, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL SAVE OR UPDATE for all the dynamic dto objects in the map
            saveResultDTO = MetadataBOAttrWriteDelegateImpl.getInstance().updateCustomAttributes(boListToUpdate, executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        // following block should have a separate try and catch block
        // On any exception from the following code should not invoke roll back transaction
        try {
            // refresh metadata after successful add custom attribute
            MetadataConfigDelegateImpl.getInstance().refreshMetadata(executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(new DSPDMException("Custom attribute(s) updated successfully but error occurred while refreshing metadata. " + e.getMessage(),
                    e, executionContext.getExecutorLocale()), executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO deleteCustomAttributes(List<DynamicDTO> boListToDelete, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL SAVE OR UPDATE for all the dynamic dto objects in the map
            saveResultDTO = MetadataBOAttrWriteDelegateImpl.getInstance().deleteCustomAttributes(boListToDelete, executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        // following block should have a separate try and catch block
        // On any exception from the following code should not invoke roll back transaction
        try {
            // refresh metadata after successful add custom attribute
            MetadataConfigDelegateImpl.getInstance().refreshMetadata(executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(new DSPDMException("Custom attribute(s) deleted successfully but error occurred while refreshing metadata. " + e.getMessage(),
                    e, executionContext.getExecutorLocale()), executionContext);
        }

        return saveResultDTO;
    }

    @Override
    public SaveResultDTO introduceNewBusinessObjects(List<DynamicDTO> boListToSave, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL SAVE OR UPDATE for all the dynamic dto objects in the map
            saveResultDTO = MetadataBOAttrWriteDelegateImpl.getInstance().introduceNewBusinessObjects(boListToSave, executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        // following block should have a separate try and catch block
        // On any exception from the following code should not invoke roll back transaction
        try {
            // refresh metadata after successful add custom attribute
            MetadataConfigDelegateImpl.getInstance().refreshMetadata(executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(new DSPDMException("Business object(s) introduced successfully but error occurred while refreshing metadata. " + e.getMessage(),
                    e, executionContext.getExecutorLocale()), executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO dropBusinessObjects(List<DynamicDTO> businessObjectsToBeDropped, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL DELETE for all the dynamic dto objects in the map
            saveResultDTO = MetadataBOAttrWriteDelegateImpl.getInstance().dropBusinessObjects(businessObjectsToBeDropped, executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        // following block should have a separate try and catch block
        // On any exception from the following code should not invoke roll back transaction
        try {
            // refresh metadata after successful drop of business object
            MetadataConfigDelegateImpl.getInstance().refreshMetadata(executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(new DSPDMException("Business object(s) dropped successfully but error occurred while refreshing metadata. " + e.getMessage(),
                    e, executionContext.getExecutorLocale()), executionContext);
        }

        return saveResultDTO;
    }

    @Override
    public SaveResultDTO dropBusinessObjectRelationships(List<DynamicDTO> businessObjectsRelationshipsToBeDropped, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL DELETE for all the dynamic dto objects in the map
            saveResultDTO = MetadataBOAttrWriteDelegateImpl.getInstance().dropBusinessObjectRelationships(businessObjectsRelationshipsToBeDropped, executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        // following block should have a separate try and catch block
        // On any exception from the following code should not invoke roll back transaction
        try {
            // refresh metadata after successful drop of business object
            MetadataConfigDelegateImpl.getInstance().refreshMetadata(executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(new DSPDMException("Business object relationship(s) dropped successfully but error occurred while refreshing metadata. " + e.getMessage(),
                    e, executionContext.getExecutorLocale()), executionContext);
        }

        return saveResultDTO;
    }

    @Override
    public SaveResultDTO addBusinessObjectRelationships(List<DynamicDTO> busObjRelationshipsToCreate, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL API
            saveResultDTO = MetadataBOAttrWriteDelegateImpl.getInstance().addBusinessObjectRelationships(busObjRelationshipsToCreate, executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        // following block should have a separate try and catch block
        // On any exception from the following code should not invoke roll back transaction
        try {
            // refresh metadata after successful drop of business object
            MetadataConfigDelegateImpl.getInstance().refreshMetadata(executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(new DSPDMException("Business object relationship(s) added successfully but error occurred while refreshing metadata. " + e.getMessage(),
                    e, executionContext.getExecutorLocale()), executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO addUniqueConstraints(List<DynamicDTO> busObjAttrUniqueConstraintsToBeCreated, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL API
            saveResultDTO = MetadataBOAttrWriteDelegateImpl.getInstance().addUniqueConstraints(busObjAttrUniqueConstraintsToBeCreated, executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        // following block should have a separate try and catch block
        // On any exception from the following code should not invoke roll back transaction
        try {
            // refresh metadata after successful drop of business object
            MetadataConfigDelegateImpl.getInstance().refreshMetadata(executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(new DSPDMException("Unique Constraint(s) added successfully but error occurred while refreshing metadata. " + e.getMessage(),
                    e, executionContext.getExecutorLocale()), executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO addSearchIndexes(List<DynamicDTO> busObjAttrSearchIndexesToBeCreated, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL API
            saveResultDTO = MetadataBOAttrWriteDelegateImpl.getInstance().addSearchIndexes(busObjAttrSearchIndexesToBeCreated, executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        // following block should have a separate try and catch block
        // On any exception from the following code should not invoke roll back transaction
        try {
            // refresh metadata after successful drop of business object
            MetadataConfigDelegateImpl.getInstance().refreshMetadata(executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(new DSPDMException("Search Indexes(s) added successfully but error occurred while refreshing metadata. " + e.getMessage(),
                    e, executionContext.getExecutorLocale()), executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO dropUniqueConstraints(List<DynamicDTO> busObjAttrUniqueConstraintsToBeDropped, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL API
            saveResultDTO = MetadataBOAttrWriteDelegateImpl.getInstance().dropUniqueConstraints(busObjAttrUniqueConstraintsToBeDropped, executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        // following block should have a separate try and catch block
        // On any exception from the following code should not invoke roll back transaction
        try {
            // refresh metadata after successful drop of business object
            MetadataConfigDelegateImpl.getInstance().refreshMetadata(executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(new DSPDMException("Unique Constraint(s) dropped successfully but error occurred while refreshing metadata. " + e.getMessage(),
                    e, executionContext.getExecutorLocale()), executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO dropSearchIndexes(List<DynamicDTO> busObjAttrSearchIndexesToBeDropped, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL API
            saveResultDTO = MetadataBOAttrWriteDelegateImpl.getInstance().dropSearchIndexes(busObjAttrSearchIndexesToBeDropped, executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        // following block should have a separate try and catch block
        // On any exception from the following code should not invoke roll back transaction
        try {
            // refresh metadata after successful drop of business object
            MetadataConfigDelegateImpl.getInstance().refreshMetadata(executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(new DSPDMException("Search Index(es) dropped successfully but error occurred while refreshing metadata. " + e.getMessage(),
                    e, executionContext.getExecutorLocale()), executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO generateMetadataForExistingTable(List<DynamicDTO> boListToGenerateMetadata, Set<String> exclusionList, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL API
            saveResultDTO = MetadataBOAttrWriteDelegateImpl.getInstance().generateMetadataForExistingTable(boListToGenerateMetadata, exclusionList, executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        // following block should have a separate try and catch block
        // On any exception from the following code should not invoke roll back transaction
        try {
            // refresh metadata after successful drop of business object
            MetadataConfigDelegateImpl.getInstance().refreshMetadata(executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(new DSPDMException("Business object(s) metadata generated successfully for existing table or view but error occurred while refreshing metadata. " + e.getMessage(),
                    e, executionContext.getExecutorLocale()), executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO deleteMetadataForExistingTable(List<DynamicDTO> boListToDeleteMetadata, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL API
            saveResultDTO = MetadataBOAttrWriteDelegateImpl.getInstance().deleteMetadataForExistingTable(boListToDeleteMetadata, executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        // following block should have a separate try and catch block
        // On any exception from the following code should not invoke roll back transaction
        try {
            // refresh metadata after successful drop of business object
            MetadataConfigDelegateImpl.getInstance().refreshMetadata(executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(new DSPDMException("Business object(s) metadata deleted successfully for existing table or view but error occurred while refreshing metadata. " + e.getMessage(),
                    e, executionContext.getExecutorLocale()), executionContext);
        }
        return saveResultDTO;
    }

    @Override
    public SaveResultDTO addBusinessObjectGroups(List<DynamicDTO> busObjGroupsToBeCreated, ExecutionContext executionContext) {
        SaveResultDTO saveResultDTO = null;
        try {
            // START TRANSACTION
            beginTransaction(executionContext);
            // CALL API
            saveResultDTO = MetadataBOAttrWriteDelegateImpl.getInstance().addBusinessObjectGroups(busObjGroupsToBeCreated, executionContext);
            // COMMIT TRANSACTION
            commitTransaction(executionContext);
        } catch (Throwable e) {
            // ROLLBACK TRANSACTION
            rollbackTransaction(e, executionContext);
            DSPDMException.throwException(e, executionContext);
        }
        // following block should have a separate try and catch block
        // On any exception from the following code should not invoke roll back transaction
        try {
            // refresh metadata after successful drop of business object
            MetadataConfigDelegateImpl.getInstance().refreshMetadata(executionContext);
        } catch (Exception e) {
            DSPDMException.throwException(new DSPDMException("Business Object Group(s) added successfully but error occurred while refreshing metadata. " + e.getMessage(),
                    e, executionContext.getExecutorLocale()), executionContext);
        }
        return saveResultDTO;
    }
}
