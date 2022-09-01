package com.lgc.dspdm.msp.mainservice.utils.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lgc.dspdm.config.security.SecurityServerPathUtils;
import com.lgc.dspdm.config.security.SecurityServerConnectionProperties;
import com.lgc.dspdm.config.security.SecurityServerTokenUtils;
import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.msp.mainservice.utils.HttpUtil;

import java.util.*;

/**
 * @author rao.alikhan
 */
public class ApiPermissionsCache {
    // Logger instance for logging
    private static DSPDMLogger logger = new DSPDMLogger(ApiPermissionsCache.class);
    // Working singleton instance
    private static volatile ApiPermissionsCache singletonInstance = null;

    // Object mapper for json conversion to objects
    private static ObjectMapper mapper = new ObjectMapper();

    // security server information collection cache
    private Map<String, List<String>> userNameToAPIPermissionsMappingCache = null;

    // Constants for map keys to get attributes from response object which will be a Map
    private static final String CLIENT_MAPPINGS = "clientMappings";
    private static final String ID = "id";
    private static final String NAME = "name";
    private static final String MAPPINGS = "mappings";
    private static final String ROLES = "roles";
    private static final String ADMIN = "admin";
    private static final String ADMINISTRATOR = "administrator";
    private static final String CLIENTID = "clientId";

    // the only private constructor
    private ApiPermissionsCache() {
        // case insensitive key based cache
        userNameToAPIPermissionsMappingCache = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    }

    public static ApiPermissionsCache getInstance() {
        if (singletonInstance == null) {
            synchronized (ApiPermissionsCache.class) {
                if (singletonInstance == null) {
                    singletonInstance = new ApiPermissionsCache();
                }
            }
        }
        return singletonInstance;
    }

    /**
     * This will update security server user, roles, policy, permissions maps
     *
     * @author rao.alikhan
     * @since 15-07-2020
     */
    public void refreshUserAuthorizationCache(String userId, ExecutionContext executionContext) {
        try {
            refreshUserPermissionsMap(userId, executionContext);
        } catch (Throwable ex) {
            DSPDMException.throwException(ex, executionContext.getExecutorLocale());
        }
    }

    /**
     * This will clear user api permission map
     *
     * @author rao.alikhan
     * @since 11-05-2021
     */
    public void clearAuthorizationCache() {
        synchronized (userNameToAPIPermissionsMappingCache) {
            userNameToAPIPermissionsMappingCache.clear();
        }
    }

    /**
     * This will remove user permission entry from security server cache
     *
     * @param userId           User id is key of the permission map
     * @param executionContext Execution Context
     * @author rao.alikhan
     * @since 12-05-2021
     */
    public void removeUserFromAuthorizationCache(String userId, ExecutionContext executionContext) {
        if (CollectionUtils.hasValue(userNameToAPIPermissionsMappingCache) && CollectionUtils.hasValue(userNameToAPIPermissionsMappingCache.get(userId))) {
            userNameToAPIPermissionsMappingCache.remove(userId);
        } else {
            throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.NOT_FOUND, "'{}' not found in security server api permission cache", executionContext.getExecutorLocale(), userId);
        }
    }

    public void validateUserHasPermission(String userId, String apiPermissionToValidate, ExecutionContext executionContext) {
        if ((CollectionUtils.isNullOrEmpty(userNameToAPIPermissionsMappingCache))
                || (!(userNameToAPIPermissionsMappingCache.containsKey(userId)))) {
            logger.info("'{}' not defined in security server api permission cache. Going to prepare permissions in security server api permission cache against this user", userId);
            // init security server api permission user cache and put userId in it
            refreshUserPermissionsMap(userId, executionContext);
        }
        List<String> permissionList = userNameToAPIPermissionsMappingCache.get(userId);
        Optional<String> matched = permissionList.stream().filter(permission -> (
                permission.equalsIgnoreCase(DSPDMConstants.ApiPermission.SYSTEM_ADMIN)
                        || permission.equalsIgnoreCase(apiPermissionToValidate))).findFirst();
        if (!matched.isPresent()) {
            logger.info("'{}' not found in security server api permission cache for user '{}'", apiPermissionToValidate, userId);
            throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.FORBIDDEN, "Access denied to this request", executionContext.getExecutorLocale());
        }
    }

    /**
     * This will first fetch user permissions and then put those permissions in map.
     *
     * @param userId           User id is required to fetch permission
     * @param executionContext Execution Context
     * @author rao.alikhan
     * @since 11-05-2021
     */
    private void refreshUserPermissionsMap(String userId, ExecutionContext executionContext) {
        // prepare security server api permission cache for the current user
        // get user permissions
        logger.info("Going to get permissions from security server api permission for user '{}'", userId);
        List<String> permissions = getAPIPermissionsForUser(getHttpHeadersMap(executionContext), userId, executionContext);
        if (CollectionUtils.hasValue(permissions)) {
            userNameToAPIPermissionsMappingCache.put(userId, permissions);
        } else if (ConfigProperties.getInstance().allow_read_record_permission_by_default.getBooleanValue()) {
            logger.info("'{}' does not have any roles. Going to assign default view role", userId);
            userNameToAPIPermissionsMappingCache.put(userId, Arrays.asList(DSPDMConstants.ApiPermission.VIEW));
        } else {
            logger.info("No api permission found in security server api permission cache for user '{}'", userId);
            throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.FORBIDDEN, "Access denied to this request", executionContext.getExecutorLocale());
        }
    }

    /**
     * Get http headers which includes bearer token
     *
     * @param executionContext Execution context
     * @author rao.alikhan
     * @since 15-07-2020
     */
    private Map<String, String> getHttpHeadersMap(ExecutionContext executionContext) {
        String token = SecurityServerTokenUtils.getBearerTokenFromSecurityServer(executionContext);
        // prepare headers for request
        Map<String, String> headers = new HashMap<>(1);
        headers.put("Authorization", token);
        return headers;
    }

    /**
     * This will prepare client policy list along with the assigned roles
     *
     * @param headers          Http headers having bearer token
     * @param executionContext Execution context
     * @author rao.alikhan
     * @since 15-07-2020
     */

    private Map<String, List<String>> getClientPolicyNamesAndTheirRoleIds(Map<String, String> headers, ExecutionContext executionContext) {
        Map<String, List<String>> clientPolicyNamesAndTheirRoleIds = null;
        String nameOfClient = SecurityServerConnectionProperties.getInstance(executionContext).SECURITY_SERVER_PDM_CLIENT_NAME.getPropertyValue();
        String idOfClient = SecurityServerConnectionProperties.getInstance(executionContext).SECURITY_SERVER_PDM_CLIENT_ID.getPropertyValue();
        String clientPolicyListUrl = SecurityServerPathUtils.getInstance(executionContext).getClientPolicyListUrl();
        String clientPoliciesURL = StringUtils.formatMessage(clientPolicyListUrl, idOfClient);
        try {
            logger.info("Going to read client policies for client name '{}' with client id '{}' and URL '{}'.",
                    nameOfClient, idOfClient, clientPoliciesURL);
            String response = HttpUtil.sendHttpGET(clientPoliciesURL, headers, executionContext);
            List<Map<String, Object>> securityServerClientPolicyDTOList = null;
            try {
                securityServerClientPolicyDTOList = mapper.readValue(response, List.class);
                logger.info("Client policies loaded successfully with size '{}'.", (securityServerClientPolicyDTOList == null) ? null : securityServerClientPolicyDTOList.size());
            } catch (Throwable ex) {
                throw new DSPDMException("Unable to parse the response from client policy service '{}'", ex, executionContext.getExecutorLocale(), clientPolicyListUrl);
            }
            if (CollectionUtils.hasValue(securityServerClientPolicyDTOList)) {
                clientPolicyNamesAndTheirRoleIds = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                for (Map<String, Object> clientPolicyServiceResponse : securityServerClientPolicyDTOList) {
                    // get roles ofr current policy being iterated
                    List<Map<String, String>> roles = (List<Map<String, String>>) clientPolicyServiceResponse.get(ROLES);
                    if (CollectionUtils.hasValue(roles)) {
                        String policyName = clientPolicyServiceResponse.get(NAME).toString();
                        List<String> roleIdListForCurrentPolicy = CollectionUtils.getStringValuesFromList(roles, ID);
                        clientPolicyNamesAndTheirRoleIds.put(policyName, roleIdListForCurrentPolicy);
                    }
                }
            } else {
                throw new DSPDMException("No security server client authorization policies found for security server client with name '{}'.", executionContext.getExecutorLocale(), nameOfClient);
            }
        } catch (Throwable ex) {
            logger.info("Unable to read client policies for client name '{}' with client id '{}' and URL '{}'.",
                    nameOfClient, idOfClient, clientPoliciesURL);
            DSPDMException.throwException(ex, executionContext);
        }
        return clientPolicyNamesAndTheirRoleIds;
    }

    /**
     * This will get user permissions
     *
     * @author rao.alikhan
     * @since 15-07-2020
     */
    private List<String> getAPIPermissionsForUser(Map<String, String> httpHeadersMap, String userId, ExecutionContext executionContext) {
        List<String> finalPermissionsForUser = null;
        try {
            Map<String, String> userRolesNameAndIdMap = getAllPDMClientRolesForUserId(userId, httpHeadersMap, executionContext);
            logger.info("Going to prepare api permissions for user '{}' using client roles mapping.", userId);
            if (CollectionUtils.hasValue(userRolesNameAndIdMap)) {
                // if user got admin role then add full access permission
                if (userRolesNameAndIdMap.containsKey(DSPDMConstants.ApiPermission.SYSTEM_ADMIN)) {
                    finalPermissionsForUser = new ArrayList<>(1);
                    // add to list
                    finalPermissionsForUser.add(DSPDMConstants.ApiPermission.SYSTEM_ADMIN);
                } else {
                    // get client policies from security server
                    Map<String, List<String>> clientPolicyNamesAndTheirRoleIds = getClientPolicyNamesAndTheirRoleIds(httpHeadersMap, executionContext);
                    Set<String> permissionSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                    // get permissions list by mappings
                    for (Map.Entry<String, String> userRoleEntry : userRolesNameAndIdMap.entrySet()) {
                        logger.info("Going to get API permissions by role name '{}' and role id '{}'", userRoleEntry.getKey(), userRoleEntry.getValue());
                        Set<String> permissionsByRole = getAPIPermissionsByRoleId(userRoleEntry.getValue(), clientPolicyNamesAndTheirRoleIds);
                        if (CollectionUtils.hasValue(permissionsByRole)) {
                            // add to list
                            permissionSet.addAll(permissionsByRole);
                        }
                    }
                    // covert set to list
                    finalPermissionsForUser = new ArrayList<>(permissionSet);
                }
            }
            logger.info("Permissions prepared successfully for userId '{}' size {}", userId, finalPermissionsForUser == null ? null : finalPermissionsForUser.size());
        } catch (Throwable ex) {
            if (ex instanceof DSPDMException) {
                DSPDMException dspdmException = (DSPDMException) ex;
                if (dspdmException.getHttpStatusCode() == DSPDMConstants.HTTP_STATUS_CODES.NOT_FOUND) {
                    // If user id not found but it exists in permission cache then need to remove from there as well.
                    if (CollectionUtils.hasValue(userNameToAPIPermissionsMappingCache)) {
                        if (CollectionUtils.hasValue(userNameToAPIPermissionsMappingCache.get(userId))) {
                            logger.info("User id not found in security server but exists in security server api permission cache", executionContext);
                            removeUserFromAuthorizationCache(userId, executionContext);
                        }
                    }
                }
                throw dspdmException;
            } else {
                DSPDMException.throwException(ex, executionContext);
            }
        }
        return finalPermissionsForUser;
    }

    /**
     * This will prepare client roles against the given user
     *
     * @param userId           User id required to attach the http request
     * @param headers          Http headers having bearer token
     * @param executionContext Execution context
     * @return Will return security server client roles for configured client's name
     * @author rao.alikhan
     * @since 15-07-2020
     */
    private Map<String, String> getAllPDMClientRolesForUserId(String userId, Map<String, String> headers, ExecutionContext executionContext) {
        Map<String, String> pdmClientRolesForUserId = null;
        if (StringUtils.isNullOrEmpty(userId)) {
            throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST, "User id required", null, executionContext.getExecutorLocale());
        }
        String clientId = SecurityServerConnectionProperties.getInstance(executionContext).SECURITY_SERVER_PDM_CLIENT_ID.getPropertyValue();
        if (StringUtils.isNullOrEmpty(clientId)) {
            throw new DSPDMException(DSPDMConstants.HTTP_STATUS_CODES.BAD_REQUEST, "Client id required", null, executionContext.getExecutorLocale());
        }
        String userRoleListUrl = SecurityServerPathUtils.getInstance(executionContext).getUserRoleListUrl();
        // update user id in url
        String userRoleSecurityServerURL = StringUtils.formatMessage(userRoleListUrl, userId, clientId);
        try {
            logger.info("Going to read all user (effective) roles for user '{}', client '{}' and URL '{}'", userId, clientId, userRoleSecurityServerURL);
            String response = HttpUtil.sendHttpGET(userRoleSecurityServerURL, headers, executionContext);
            List<Map<String, String>> effectiveRolesList;
            try {
                // parse response to get all roles related to this user
                effectiveRolesList = mapper.readValue(response, List.class);
            } catch (Exception e) {
                throw new DSPDMException("Unable to parse the response from user roles by user id service '{}'", e, executionContext.getExecutorLocale(), userRoleSecurityServerURL);
            }
            if (CollectionUtils.isNullOrEmpty(effectiveRolesList)) {
                logger.info("No '{}' found in user roles response for user '{}', client '{}' and URL '{}'", CLIENT_MAPPINGS, userId, clientId, userRoleSecurityServerURL);
            } else {
                pdmClientRolesForUserId = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                for(Map<String, String> userRole: effectiveRolesList){
                    pdmClientRolesForUserId.put(userRole.get(NAME), userRole.get(ID));
                }
            }
        } catch (Throwable ex) {
            DSPDMException.throwException(ex, executionContext);
        }
        return pdmClientRolesForUserId;
    }

    /**
     * This will prepare a list of permissions from client policy list based on the role id given
     *
     * @param roleId                           Role id is required to get permission
     * @param clientPolicyNamesAndTheirRoleIds
     * @return Will return list of permissions given by the role
     * @author rao.alikhan
     * * @since 15-07-2020
     */
    private Set<String> getAPIPermissionsByRoleId(String roleId, Map<String, List<String>> clientPolicyNamesAndTheirRoleIds) {
        Set<String> permissionList = null;
        if (CollectionUtils.hasValue(clientPolicyNamesAndTheirRoleIds)) {
            permissionList = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            for (Map.Entry<String, List<String>> clientPolicyEntry : clientPolicyNamesAndTheirRoleIds.entrySet()) {
                List<String> roleIdListForCurrentPolicy = clientPolicyEntry.getValue();
                if (roleIdListForCurrentPolicy.contains(roleId)) {
                    String policyName = clientPolicyEntry.getKey();
                    for (DSPDMConstants.APIPermissionsMapping mapping : DSPDMConstants.APIPermissionsMapping.values()) {
                        if (policyName.equalsIgnoreCase(mapping.getSecurityServerPolicyName())) {
                            permissionList.add(mapping.getApiPermissionName());
                        }
                    }
                }
            }
        }
        return permissionList;
    }
}
