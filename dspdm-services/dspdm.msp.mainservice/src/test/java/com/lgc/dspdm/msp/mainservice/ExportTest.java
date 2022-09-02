package com.lgc.dspdm.msp.mainservice;

import com.lgc.dspdm.core.common.data.criteria.join.JoinType;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.msp.mainservice.model.BOQuery;
import com.lgc.dspdm.msp.mainservice.model.DSPDMUnit;
import com.lgc.dspdm.msp.mainservice.model.Pagination;
import com.lgc.dspdm.msp.mainservice.model.join.SimpleJoinClause;
import org.junit.Test;

import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class ExportTest extends BaseServiceTest {
    /*private static DSPDMLogger logger = new DSPDMLogger(ExportTest.class);

    @Test
    public void testExport()  {
        super.exportGet("WELL");
        logger.info("testExport Passed.");
    }

    @Test
    public void testExportPostSystemUnitConversion()  {
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName("WELL");
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        queryEntity.setUnitPolicy(DSPDMConstants.Units.UNITPOLICY_SYSTEM);
        Response response = super.exportPost(queryEntity);
        logger.info("testExportPost Passed.");
    }

    @Test
    public void testExportPostUserUnitConversion()  {
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName("WELL");
        queryEntity.setSheetName("WELL2");
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        queryEntity.setUnitPolicy(DSPDMConstants.Units.UNITPOLICY_USER);
        Response response = super.exportPost(queryEntity);
        logger.info("testExportPost Passed.");
    }
    
    @Test
    public void testExportPostCustomUnitConversion()  {
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName("WELL");
        queryEntity.setSheetName("WELL3");
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        queryEntity.setUnitPolicy(DSPDMConstants.Units.UNITPOLICY_CUSTOM);
        List<DSPDMUnit> dspdmUnits=new ArrayList<DSPDMUnit>();
        DSPDMUnit dspdmUnit=new DSPDMUnit();
        dspdmUnit.setBoAttrName("X_COORDINATE");
        dspdmUnit.setTargetUnit("rad");
        dspdmUnits.add(dspdmUnit);
        queryEntity.setDSPDMUnits(dspdmUnits);
        Response response = super.exportPost(queryEntity);
        logger.info("testExportPost Passed.");
    }
    
    @Test
    public void testSelectList() {
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName("WELL");
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        Pagination pg = new Pagination();
        pg.setRecordsPerPage(1);
        queryEntity.setPagination(pg);
        String[] select = {"ROW_CREATED_DATE", "ROW_CREATED_BY"};
        queryEntity.setSelectList(Arrays.asList(select));
        Response response = super.exportPost(queryEntity);
        logger.info("testSelectList Passed.");
    }

    @Test
    public void testExportJoinedTable() {
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName("WELL");
        queryEntity.setSheetName("WELL_JOINED_WELLBORE");
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        queryEntity.setJoinAlias("a");
        queryEntity.setSimpleJoins(Arrays.asList(new SimpleJoinClause().setBoName("WELLBORE").setJoinAlias("b").setJoinType(JoinType.INNER.name())));
        Pagination pg = new Pagination();
        pg.setRecordsPerPage(1);
        pg.setPages(Arrays.asList(1));
        queryEntity.setPagination(pg);
        Response response = super.exportPost(queryEntity);
        if (response.getStatus() == DSPDMConstants.HTTP_STATUS_CODES.OK) {
            logger.info("testExportJoinedTable Passed.");
        } else {
            throw new DSPDMException("testExportJoinedTable failed", Locale.getDefault());
        }
    }
    
    @Test
    public void testExportBOQueryPost()  {
        BOQuery queryEntity = new BOQuery();
        queryEntity.setBoName("WELL");
        queryEntity.setLanguage("en");
        queryEntity.setTimezone("GMT+08:00");
        Response response = super.exportPost(queryEntity);
        logger.info("testExportPost one entity Passed.");
    }
    
    @Test
    public void testExportBOQuerysPost() {
        BOQuery queryEntity1 = new BOQuery();
        queryEntity1.setBoName("WELL");
        queryEntity1.setLanguage("en");
        queryEntity1.setTimezone("GMT+08:00");
        BOQuery queryEntity2 = new BOQuery();
        queryEntity2.setBoName("WELLBORE");
        queryEntity2.setLanguage("en");
        queryEntity2.setTimezone("GMT+08:00");
        Response response = super.exportPost(queryEntity1, queryEntity2);
        logger.info("testExportPost entitys Passed.");
    }*/
}
