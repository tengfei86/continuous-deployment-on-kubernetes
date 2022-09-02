package com.lgc.dspdm.msp.mainservice;

import com.lgc.dspdm.config.security.SecurityServerTokenUtils;
import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.msp.mainservice.model.DSPDMResponse;
import com.lgc.dspdm.msp.mainservice.utils.security.AuthTokenUtils;
import com.lgc.dspdm.msp.mainservice.utils.security.AuthTokenInfo;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UserPermissionCacheTest extends BaseServiceTest {
    /*private DSPDMLogger logger = new DSPDMLogger(UserPermissionCacheTest.class);

    @Test
    public void test001RefreshAuthorizationCacheForUser() {

        DSPDMResponse dspdmResponse = super.refreshAuthorizationCacheForUser(getUserId());
        logger.info("testRefreshAuthorizationCacheForUser Passed.");
    }

    @Test
    public void test002DeleteUserAuthorizationCache() {
        DSPDMResponse dspdmResponse = super.deleteUserAuthorizationCache(getUserId());
        logger.info("testDeleteUserAuthorizationCache Passed.");
    }

    @Test
    public void test003ClearAuthorizationCache() {
        DSPDMResponse dspdmResponse = super.clearAuthorizationCache();
        logger.info("testClearAuthorizationCache Passed.");
    }

    private String getUserId() {
        String userId = null;
        ExecutionContext executionContext = ExecutionContext.getTestUserExecutionContext();
        if (ConfigProperties.getInstance().is_security_enabled.getBooleanValue()) {
            String token = SecurityServerTokenUtils.getTokenFromSecurityServer(executionContext);
            AuthTokenInfo authTokenInfo = AuthTokenUtils.parseAuthToken(token, executionContext.getExecutorLocale());
            userId = authTokenInfo.getSubject();
        } else {
            userId = executionContext.getExecutorName();
        }
        return userId;
    }*/
}
