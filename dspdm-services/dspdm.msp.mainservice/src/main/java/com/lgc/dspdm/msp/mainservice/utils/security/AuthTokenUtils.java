package com.lgc.dspdm.msp.mainservice.utils.security;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lgc.dspdm.config.security.SecurityServerPathUtils;
import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import com.lgc.dspdm.msp.mainservice.utils.HttpUtil;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;


import java.util.Base64;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * utility class to interpret the json web token
 */
public class AuthTokenUtils {
    // object mapper instance
    private static ObjectMapper objectMapper = null;
    // logger instance
    private static DSPDMLogger logger = new DSPDMLogger(AuthTokenUtils.class);

    public static void validateAccessTokenFromHttpHeaders(HttpHeaders httpHeaders, ExecutionContext executionContext) {
        String token = getAuthTokenFromHttpHeaders(httpHeaders);
        if(ConfigProperties.getInstance().is_security_enabled.getBooleanValue()) {
            validateAccessToken(token, executionContext);
        }
    }

    public static void refreshUserAuthorizationCache(String userId, ExecutionContext executionContext) {
        ApiPermissionsCache.getInstance().refreshUserAuthorizationCache(userId, executionContext);
    }

    public static void clearAuthorizationCache() {
        ApiPermissionsCache.getInstance().clearAuthorizationCache();
    }

    public static void removeUserFromAuthorizationCache(String userId, ExecutionContext executionContext) {
        ApiPermissionsCache.getInstance().removeUserFromAuthorizationCache(userId, executionContext);
    }

    public static void validateApiPermissions(String userId, String apiRequestType, ExecutionContext executionContext) {
        if(ConfigProperties.getInstance().is_security_enabled.getBooleanValue()) {
            ApiPermissionsCache.getInstance().validateUserHasPermission(userId, apiRequestType, executionContext);
        }
    }

    public static AuthTokenInfo getTokenInfoFromToken(HttpHeaders httpHeaders, Locale locale) {
        AuthTokenInfo authTokenInfo = null;
        if (ConfigProperties.getInstance().is_security_enabled.getBooleanValue()) {
            authTokenInfo = parseAuthToken(getAuthTokenFromHttpHeaders(httpHeaders), locale);
        } else {
            authTokenInfo = new AuthTokenInfo();
        }
        return authTokenInfo;
    }

    /**
     * Validate access token from security server
     *
     * @param token Token is required to validate
     * @author rao.alikhan
     * @since 07-07-2020
     */
    private static void validateAccessToken(String token, ExecutionContext executionContext) {
        if (StringUtils.hasValue(token)) {
            Map<String, String> headers = new HashMap<>(1);
            headers.put("Authorization", "Bearer " + token);
            String userInfoListUrl = null;
            try {
                userInfoListUrl = SecurityServerPathUtils.getInstance(executionContext).getUserInfoListUrl();
                logger.info("Going to get user info to authenticate auth token");
                HttpUtil.sendHttpGET(userInfoListUrl, headers, executionContext);
            } catch (Throwable ex) {
                logger.info("Security server response returns with the error. So it means the token is expired or invalid for url : '{}'", userInfoListUrl);
                DSPDMException.throwException(ex, executionContext);
            }
        } else {
            logger.info("No token found to authenticate.");
            throw new DSPDMException(Response.Status.UNAUTHORIZED.getStatusCode(), "401 Http UnAuthorized", null, executionContext.getExecutorLocale());
        }
    }

    private static ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }
        return objectMapper;
    }

    private static String getAuthTokenFromHttpHeaders(HttpHeaders httpHeaders) {
        String token = httpHeaders.getHeaderString("Authorization");
        if (StringUtils.hasValue(token)) {
            if (token.startsWith("Bearer ")) {
                token = token.substring("Bearer ".length());
            }
        }
        return token;
    }

    public static AuthTokenInfo parseAuthToken(String authToken, Locale locale) {
        if (StringUtils.isNullOrEmpty(authToken)) {
            throw new DSPDMException("Invalid token : {}", locale, authToken);
        }
        AuthTokenInfo authTokenInfo = null;
        String[] segments = authToken.split("\\.");
        if (segments.length <= 1) {
            throw new DSPDMException("Invalid token segments length: {}", locale, authToken);
        }
        String base64String = segments[1];
        int requiredLength = (int) (4 * Math.ceil(base64String.length() / 4.0));
        int nbrPaddings = requiredLength - base64String.length();

        if (nbrPaddings > 0) {
            base64String = base64String + "====".substring(0, nbrPaddings);
        }

        base64String = base64String.replace("-", "+");
        base64String = base64String.replace("_", "/");

        try {
            byte[] data = Base64.getDecoder().decode(base64String);

            String text = new String(data, "UTF-8");
            authTokenInfo = getObjectMapper().readValue(text, AuthTokenInfo.class);

        } catch (Exception e) {
            DSPDMException.throwException(e, ExecutionContext.getUnknownUserExecutionContext().setExecutorLocale(locale));
        }
        return authTokenInfo;
    }

    private static String getToken() {
        return "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI2eEM3d2ZyLUpRMjNleE41YUxMYWx1WGdsV3UwNG93YzdfeGlkeTF3eW1BIn0.eyJqdGkiOiIzZDgyMmM3OC1jYmIyLTQ5ZjAtYTEyOC1lNWRhYjk2OGY5YzAiLCJleHAiOjE1NzEwMzYxOTQsIm5iZiI6MCwiaWF0IjoxNTY5MjM2MTk0LCJpc3MiOiJodHRwOi8vbG1rci00MjE2LnJkeC5sZ2MuY29tOjg4ODAvYXV0aC9yZWFsbXMvRGVjaXNpb25TcGFjZV9JbnRlZ3JhdGlvbl9TZXJ2ZXIiLCJhdWQiOiJEc3BkbUFwcCIsInN1YiI6ImVmZDljYjVjLWIxYzAtNDZmZC1hYzVlLTU5MWJjNmNkMGYyNSIsInR5cCI6IkJlYXJlciIsImF6cCI6IkRzcGRtQXBwIiwiYXV0aF90aW1lIjoxNTY5MjM2MTk0LCJzZXNzaW9uX3N0YXRlIjoiYTlmYzA5OWItZWY4MC00OTI0LTkxMTEtNzA3MDRmNThjNmU1IiwiYWNyIjoiMSIsImNsaWVudF9zZXNzaW9uIjoiZmQyM2FhNmMtYjA2Yy00ZmExLTg0OWMtNmYxZjU5NGIwMGY4IiwiYWxsb3dlZC1vcmlnaW5zIjpbIioiXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbImRzaXMtdXNlcnMiLCJiaGRtLW1hbmFnZXIiLCJiaGRtLXVzZXIiLCJiaGRtLWFkbWluIiwiZHNpcy1hZG1pbnMiLCJ1bWFfYXV0aG9yaXphdGlvbiJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImRzaXMtYnBtIjp7InJvbGVzIjpbImtpZW1nbXQiLCJkc2JwbS11c2VyLXJvbGUiLCJyZXN0LWFsbCIsImFkbWluIiwidXNlciIsImRzYnBtLWFkbWluLXJvbGUiXX0sImRzaXMtY29uZmlnIjp7InJvbGVzIjpbImRzaXMtY29uZmlnLXVzZXJzIiwiZHNpcy1jb25maWctYWRtaW5zIl19LCJyZWFsbS1tYW5hZ2VtZW50Ijp7InJvbGVzIjpbInZpZXctcmVhbG0iLCJ2aWV3LWlkZW50aXR5LXByb3ZpZGVycyIsIm1hbmFnZS1pZGVudGl0eS1wcm92aWRlcnMiLCJpbXBlcnNvbmF0aW9uIiwicmVhbG0tYWRtaW4iLCJjcmVhdGUtY2xpZW50IiwibWFuYWdlLXVzZXJzIiwidmlldy1hdXRob3JpemF0aW9uIiwibWFuYWdlLWV2ZW50cyIsIm1hbmFnZS1yZWFsbSIsInZpZXctZXZlbnRzIiwidmlldy11c2VycyIsInZpZXctY2xpZW50cyIsIm1hbmFnZS1hdXRob3JpemF0aW9uIiwibWFuYWdlLWNsaWVudHMiXX0sImRzaXMtc2VhcmNoIjp7InJvbGVzIjpbImRzaXMtc2VhcmNoLWFkbWlucyIsImRzaXMtc2VhcmNoLXVzZXJzIl19LCJEc3BkbUFwcCI6eyJyb2xlcyI6WyJkc2lzLWRhdGEtdXNlcnMiLCJkc2lzLXVzZXJzIiwiZHNpcy1hZG1pbnMiLCJkc2lzLWRhdGEtYWRtaW5zIl19LCJkc2lzLXRyYW5zZmVyIjp7InJvbGVzIjpbImRzaXMtdHJhbnNmZXItdXNlcnMiLCJkc2lzLXRyYW5zZmVyLWFkbWlucyJdfSwiZHNpcy1kYXRhIjp7InJvbGVzIjpbImRzaXMtZGF0YS11c2VycyIsImRzaXMtZGF0YS1hZG1pbnMiXX0sImRzaXMtY29uc29sZSI6eyJyb2xlcyI6WyJkc2lzLWNvbnNvbGUtYWRtaW5zIl19LCJkc2lzLWJwbS1zZXJ2ZXIiOnsicm9sZXMiOlsia2llLXNlcnZlciJdfSwiZHNpcy1zY2hlZHVsZXIiOnsicm9sZXMiOlsiZHNpcy1zY2hlZHVsZXItYWRtaW5zIl19LCJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50Iiwidmlldy1wcm9maWxlIl19fSwibmFtZSI6IkphbmUgQWRtaW4iLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJhZG1pbiIsImdpdmVuX25hbWUiOiJKYW5lIiwiZmFtaWx5X25hbWUiOiJBZG1pbiIsImVtYWlsIjoibXVoYW1tYWRpbXJhbi5hbnNhcmlAaGFsbGlidXJ0b24uY29tIn0.f0DssZs9asa0l7XUoZ04OgS2ugEjWIWrqiwIQqUxQsabzewREQGOEfkNobUzFrKlA1jLoy--4CnAjoM89wzD3Rm8v79XZFQSEU0oD4Ia-yUV-ARC26jeEeLAS-4J8W6LVWdVN-MUNeXoN9CPse02heO7ZisO2-N8VZoCIzsKNSbw2okldF1apve2zGnyqqDJs2ENx_FqA4GInRFiiLLAN6J6AjNr5ReqaFJGok2KBnNlS5TubClpEvjZQuJQK6k5IS_eMmcPbQbNOnQXxaUoNqcDLCr5DHTihqDsAhGXJGKFaYwBfZWhw8SrsUt4Q43Ixz25tZBrSlkTLB69ut5xrg";
    }

    public static void main(String[] args) {
        AuthTokenInfo authTokenInfo = parseAuthToken(getToken(), Locale.getDefault());
        System.out.println(authTokenInfo);
    }
}
