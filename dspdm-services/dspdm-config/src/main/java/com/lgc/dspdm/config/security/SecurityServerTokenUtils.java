package com.lgc.dspdm.config.security;

import com.alibaba.fastjson.JSONPath;
import com.lgc.dspdm.config.utils.HttpUtils;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.logging.DSPDMLogger;
import com.lgc.dspdm.core.common.util.AESSecurityUtils;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;
import com.lgc.dspdm.core.common.util.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;

import java.util.ArrayList;
import java.util.List;

public class SecurityServerTokenUtils {
    private static DSPDMLogger logger = new DSPDMLogger(SecurityServerTokenUtils.class);

    public static String getBearerTokenFromSecurityServer(ExecutionContext executionContext) {
        return "Bearer " + getTokenFromSecurityServer(executionContext);
    }

    public static String getTokenFromSecurityServer(ExecutionContext executionContext) {
        String token = null;
        try {
            String tokenUrl = null;
            List<NameValuePair> params = new ArrayList<>();
            NameValuePair grant_type = new BasicNameValuePair(DSPDMConstants.SecurityServerParamNames.grant_type, DSPDMConstants.SecurityServerParamNames.password);
            NameValuePair username = new BasicNameValuePair(DSPDMConstants.SecurityServerParamNames.username, AESSecurityUtils.decode(SecurityServerConnectionProperties.getInstance(executionContext).SECURITY_SERVER_USER.getPropertyValue()));
            NameValuePair password = new BasicNameValuePair(DSPDMConstants.SecurityServerParamNames.password, AESSecurityUtils.decode(SecurityServerConnectionProperties.getInstance(executionContext).SECURITY_SERVER_ENCPASS.getPropertyValue()));
            NameValuePair client_id = new BasicNameValuePair(DSPDMConstants.SecurityServerParamNames.client_id, SecurityServerConnectionProperties.getInstance(executionContext).SECURITY_SERVER_PDM_CLIENT_NAME.getPropertyValue());
            NameValuePair client_secret = new BasicNameValuePair(DSPDMConstants.SecurityServerParamNames.client_secret, SecurityServerConnectionProperties.getInstance(executionContext).SECURITY_SERVER_PDM_CLIENT_SECRET.getPropertyValue());
            params.add(grant_type);
            params.add(username);
            params.add(password);
            params.add(client_id);
            params.add(client_secret);
            tokenUrl = String.format(SecurityServerConnectionProperties.getInstance(executionContext).SECURITY_SERVER_TOKEN_URL_FORMAT.getPropertyValue(),
                    SecurityServerConnectionProperties.getInstance(executionContext).SECURITY_SERVER_ROOT_URL.getPropertyValue(),
                    SecurityServerConnectionProperties.getInstance(executionContext).SECURITY_SERVER_REALM.getPropertyValue());
            logger.info("Going to send a request to security server to get token against client, username and password provided in security server connection settings.");
            String response = HttpUtils.sendHttpPost(tokenUrl, params, null, executionContext);
            if (StringUtils.isNullOrEmpty(response)) {
                throw new DSPDMException("Null or empty response from auth service '{}'", executionContext.getExecutorLocale(), tokenUrl);
            }
            token = (String) JSONPath.read(response, "access_token");
            if (StringUtils.isNullOrEmpty(token)) {
                throw new DSPDMException("No access_token found from auth service '{}'", executionContext.getExecutorLocale(), tokenUrl);
            }
            token = token;
        } catch (Throwable e) {
            DSPDMException.throwException(e, executionContext);
        }
        return token;
    }

    public static void main(String[] args) {
        System.out.println(getBearerTokenFromSecurityServer(ExecutionContext.getSystemUserExecutionContext()));
    }
}
