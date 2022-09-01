package com.lgc.dspdm.core.dao.fixed;

import com.lgc.dspdm.config.AppConfigInit;
import com.lgc.dspdm.core.common.config.PropertySource;
import com.lgc.dspdm.core.common.data.annotation.AnnotationProcessor;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectAttributeConstraintsDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectAttributeDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectDTO;
import com.lgc.dspdm.core.common.data.dto.fixed.BusinessObjectRelationshipDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class GenericDAOTest {
    private static DSPDMLogger logger = new DSPDMLogger(GenericDAOTest.class);

    @BeforeAll
    public static void setUp() throws Exception {
        ExecutionContext executionContext = null;
        try {
            executionContext = ExecutionContext.getTestUserExecutionContext();
        } catch (Exception e) {
            logger.error("Unable to get execution context for test user");
            DSPDMException.throwException(e, Locale.getDefault());
        }
        // initialize application configurations
        AppConfigInit.init(PropertySource.MAP_PROVIDED, executionContext);
    }

    @Test
    public void testBusinessObjects() {
        ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
        try {
            GenericDAO.getInstance().findAll(BusinessObjectDTO.class, null, executionContext);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            DSPDMException.throwException(e, executionContext);
        }
    }

    @Test
    public void testBusinessObjectAttributes() {
        ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
        try {
            List<String> orderBy = new ArrayList<>(2);
            String boNamePhysicalColumnName = AnnotationProcessor.getDatabaseColumnName(BusinessObjectAttributeDTO.class.getDeclaredField(BusinessObjectAttributeDTO.properties.boName.name()));
            String sequenceNumberPhysicalColumnName = AnnotationProcessor.getDatabaseColumnName(BusinessObjectAttributeDTO.class.getDeclaredField(BusinessObjectAttributeDTO.properties.sequenceNumber.name()));
            orderBy.add(boNamePhysicalColumnName);
            orderBy.add(sequenceNumberPhysicalColumnName);
            GenericDAO.getInstance().findAll(BusinessObjectAttributeDTO.class, orderBy, executionContext);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            DSPDMException.throwException(e, executionContext);
        }
    }

    @Test
    public void testBusinessObjectAttributeConstraints() {
        ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
        try {
            GenericDAO.getInstance().findAll(BusinessObjectAttributeConstraintsDTO.class, null, executionContext);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            DSPDMException.throwException(e, executionContext);
        }
    }

    @Test
    public void testBusinessObjectRelationships() {
        ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
        try {
            GenericDAO.getInstance().findAll(BusinessObjectRelationshipDTO.class, null, executionContext);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            DSPDMException.throwException(e, executionContext);
        }
    }
}
