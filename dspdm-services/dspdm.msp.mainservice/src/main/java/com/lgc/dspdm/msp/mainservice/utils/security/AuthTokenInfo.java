package com.lgc.dspdm.msp.mainservice.utils.security;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * Sample token below
 * <p>
 * "jti": "3d822c78-cbb2-49f0-a128-e5dab968f9c0",
 * "exp": 1571036194,
 * "nbf": 0,
 * "iat": 1569236194,
 * "iss": "http://lmkr-4216.rdx.lgc.com:8880/auth/realms/DecisionSpace_Integration_Server",
 * "aud": "DspdmApp",
 * "sub": "efd9cb5c-b1c0-46fd-ac5e-591bc6cd0f25",
 * "typ": "Bearer",
 * "azp": "DspdmApp",
 * "auth_time": 1569236194,
 * "session_state": "a9fc099b-ef80-4924-9111-70704f58c6e5",
 * "acr": "1",
 * "client_session": "fd23aa6c-b06c-4fa1-849c-6f1f594b00f8",
 * "allowed-origins": [
 * "*"
 * ],
 * "realm_access": {
 * "roles": [
 * ]
 * },
 * "resource_access": {
 * <p>
 * },
 * "name": "Jane Admin",
 * "preferred_username": "admin",
 * "given_name": "Jane",
 * "family_name": "Admin",
 * "email": "muhammadimran.ansari@halliburton.com"
 */
public class AuthTokenInfo {
    /**
     * JWT ID (unique identifier for this token)
     */
    @JsonProperty("jti")
    private String jsonWebTokenId = null;
    /**
     * Expiration time (seconds since Unix epoch)
     */
    @JsonProperty("exp")
    private Integer expirationTime = null;
    /**
     * Not valid before (seconds since Unix epoch)
     */
    @JsonProperty("nbf")
    private Integer notValidBefore = null;
    /**
     * Issued at (seconds since Unix epoch)
     */
    @JsonProperty("iat")
    private Integer issuedAt = null;
    /**
     * Issuer (who created and signed this token)
     */
    @JsonProperty("iss")
    private String issuer = null;
    /**
     * Audience (who or what the token is intended for)
     */
//    @JsonProperty("aud")
//    private List<String> audience = null;
    /**
     * Subject (whom the token refers to)
     */
    @JsonProperty("sub")
    private String subject = null;
    /**
     * Type of token, Bearer etc
     */
    @JsonProperty("typ")
    private String type = null;
    /**
     * Authorized party (the party to which this token was issued)
     */
    @JsonProperty("azp")
    private String authorizedParty = null;

    @JsonProperty("nonce")
    private String nonce = null;

    /**
     * Time when authentication occurred
     */
    @JsonProperty("auth_time")
    private Integer authenticationTime = null;
    /**
     *
     */
    @JsonProperty("session_state")
    private String sessionState = null;
    /**
     * Authentication context class
     */
    @JsonProperty("acr")
    private String authenticationContext = null;

    /**
     * Origins to allow
     */
    @JsonProperty("allowed-origins")
    private List<String> allowedOrigins;

    /**
     * All accessed realm names
     */
    @JsonProperty("realm_access")
    private Map<String, Object> realmAccess;

    /**
     * All accessed resource names
     */
    @JsonProperty("resource_access")
    private Map<String, Object> resourceAccess;

    /**
     * Scope defined
     */
    @JsonProperty("scope")
    private String scope;

    /**
     * User email verified or not
     */
    @JsonProperty("email_verified")
    private Boolean emailVerified;

    /**
     *
     */
    @JsonProperty("client_session")
    private String clientSession = null;
    /**
     * Full name space separated
     */
    @JsonProperty("name")
    private String completeName = null;
    /**
     * user name
     */
    @JsonProperty("preferred_username")
    private String userName = null;
    /**
     * First name
     */
    @JsonProperty("given_name")
    private String givenName = null;
    /**
     * Last name
     */
    @JsonProperty("family_name")
    private String familyName = null;
    /**
     * Email
     */
    @JsonProperty("email")
    private String email = null;


    public String getJsonWebTokenId() {
        return jsonWebTokenId;
    }

    public void setJsonWebTokenId(String jsonWebTokenId) {
        this.jsonWebTokenId = jsonWebTokenId;
    }

    public Integer getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(Integer expirationTime) {
        this.expirationTime = expirationTime;
    }

    public Integer getNotValidBefore() {
        return notValidBefore;
    }

    public void setNotValidBefore(Integer notValidBefore) {
        this.notValidBefore = notValidBefore;
    }

    public Integer getIssuedAt() {
        return issuedAt;
    }

    public void setIssuedAt(Integer issuedAt) {
        this.issuedAt = issuedAt;
    }

    public String getIssuer() {
        return issuer;
    }

    public void setIssuer(String issuer) {
        this.issuer = issuer;
    }

//    public List<String> getAudience() {
//        return audience;
//    }
//
//    public void setAudience(List<String> audience) {
//        this.audience = audience;
//    }

    public String getNonce() {
        return nonce;
    }

    public void setNonce(String nonce) {
        this.nonce = nonce;
    }

    public List<String> getAllowedOrigins() {
        return allowedOrigins;
    }

    public void setAllowedOrigins(List<String> allowedOrigins) {
        this.allowedOrigins = allowedOrigins;
    }

    public Map<String, Object> getRealmAccess() {
        return realmAccess;
    }

    public void setRealmAccess(Map<String, Object> realmAccess) {
        this.realmAccess = realmAccess;
    }

    public Map<String, Object> getResourceAccess() {
        return resourceAccess;
    }

    public void setResourceAccess(Map<String, Object> resourceAccess) {
        this.resourceAccess = resourceAccess;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    public Boolean getEmailVerified() {
        return emailVerified;
    }

    public void setEmailVerified(Boolean emailVerified) {
        this.emailVerified = emailVerified;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getAuthorizedParty() {
        return authorizedParty;
    }

    public void setAuthorizedParty(String authorizedParty) {
        this.authorizedParty = authorizedParty;
    }

    public Integer getAuthenticationTime() {
        return authenticationTime;
    }

    public void setAuthenticationTime(Integer authenticationTime) {
        this.authenticationTime = authenticationTime;
    }

    public String getSessionState() {
        return sessionState;
    }

    public void setSessionState(String sessionState) {
        this.sessionState = sessionState;
    }

    public String getAuthenticationContext() {
        return authenticationContext;
    }

    public void setAuthenticationContext(String authenticationContext) {
        this.authenticationContext = authenticationContext;
    }

    public String getClientSession() {
        return clientSession;
    }

    public void setClientSession(String clientSession) {
        this.clientSession = clientSession;
    }

    public String getCompleteName() {
        return completeName;
    }

    public void setCompleteName(String completeName) {
        this.completeName = completeName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getGivenName() {
        return givenName;
    }

    public void setGivenName(String givenName) {
        this.givenName = givenName;
    }

    public String getFamilyName() {
        return familyName;
    }

    public void setFamilyName(String familyName) {
        this.familyName = familyName;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TokenInfo{");
        sb.append("jsonWebTokenId=").append(jsonWebTokenId);
        sb.append(", expirationTime=").append(expirationTime);
        sb.append(", notValidBefore=").append(notValidBefore);
        sb.append(", issuedAt=").append(issuedAt);
        sb.append(", issuer=").append(issuer);
//        sb.append(", audience=").append(audience);
        sb.append(", subject=").append(subject);
        sb.append(", type=").append(type);
        sb.append(", authorizedParty=").append(authorizedParty);
        sb.append(", authenticationTime=").append(authenticationTime);
        sb.append(", sessionState=").append(sessionState);
        sb.append(", authenticationContext=").append(authenticationContext);
        sb.append(", clientSession=").append(clientSession);
        sb.append(", completeName=").append(completeName);
        sb.append(", userName=").append(userName);
        sb.append(", givenName=").append(givenName);
        sb.append(", familyName=").append(familyName);
        sb.append(", email=").append(email);
        sb.append('}');
        return sb.toString();
    }
}
