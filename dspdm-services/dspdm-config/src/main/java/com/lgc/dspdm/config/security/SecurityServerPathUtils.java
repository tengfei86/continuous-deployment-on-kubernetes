package com.lgc.dspdm.config.security;

import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;

public class SecurityServerPathUtils {

    private static final String path = "/auth";
    private static final String usersPath = "/users";
    private static final String clientsPath = "/clients";
    private static final String policiesPath = "/clients/{}/authz/resource-server/policy/role?max=10000";
//    private static final String userRolePath = "/users/{}/role-mappings/";
    private static final String userRolePath = "/users/{}/role-mappings/clients/{}/composite";

    private final String userListUrl;
    private final String clientListUrl;
    private final String clientPolicyListUrl;
    private final String userRoleListUrl;
    private final String userInfoListUrl;
    private final String tokenUrl;

    private static SecurityServerPathUtils singleton = null;

    private SecurityServerPathUtils(ExecutionContext executionContext) {
        String path = SecurityServerPathUtils.path;
        String tokenURLFormat = SecurityServerConnectionProperties.getInstance(executionContext).SECURITY_SERVER_TOKEN_URL_FORMAT.getPropertyValue();
        if (!(tokenURLFormat.contains(SecurityServerPathUtils.path))) {
            // if format does not include /auth then clear it
            path = "";
        }
        String reaslmPublicUrl = SecurityServerConnectionProperties.getInstance(executionContext).SECURITY_SERVER_ROOT_URL.getPropertyValue()
                + path
                + "/admin"
                + DSPDMConstants.SecurityServerParamNames.realmsPath
                + SecurityServerConnectionProperties.getInstance(executionContext).SECURITY_SERVER_REALM.getPropertyValue();
        String protocolPublicUrl = SecurityServerConnectionProperties.getInstance(executionContext).SECURITY_SERVER_ROOT_URL.getPropertyValue()
                + path
                + DSPDMConstants.SecurityServerParamNames.realmsPath
                + SecurityServerConnectionProperties.getInstance(executionContext).SECURITY_SERVER_REALM.getPropertyValue()
                + DSPDMConstants.SecurityServerParamNames.protocolPath;

        userListUrl = reaslmPublicUrl + usersPath;
        clientListUrl = reaslmPublicUrl + clientsPath;
        clientPolicyListUrl = reaslmPublicUrl + policiesPath;
        userRoleListUrl = reaslmPublicUrl + userRolePath;
        userInfoListUrl = protocolPublicUrl + DSPDMConstants.SecurityServerParamNames.userinfo;
        tokenUrl = protocolPublicUrl + DSPDMConstants.SecurityServerParamNames.token;
    }

    public static SecurityServerPathUtils getInstance(ExecutionContext executionContext) {
        if (singleton == null) {
            synchronized (SecurityServerPathUtils.class) {
                if (singleton == null) {
                    singleton = new SecurityServerPathUtils(executionContext);
                }
            }
        }
        return singleton;
    }

    public synchronized void refreshSecurityServerPathUtils(ExecutionContext executionContext) {
        SecurityServerPathUtils newInstance = new SecurityServerPathUtils(executionContext);
        synchronized (SecurityServerPathUtils.class) {
            SecurityServerPathUtils.singleton = newInstance;
        }
    }

    public String getUserListUrl() {
        return userListUrl;
    }

    public String getClientListUrl() {
        return clientListUrl;
    }

    public String getClientPolicyListUrl() {
        return clientPolicyListUrl;
    }

    public String getUserRoleListUrl() {
        return userRoleListUrl;
    }

    public String getUserInfoListUrl() {
        return userInfoListUrl;
    }

    public String getTokenUrl() {
        return tokenUrl;
    }

}
