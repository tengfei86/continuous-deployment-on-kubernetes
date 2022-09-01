package com.lgc.dspdm.core.common.util;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.*;
import java.util.stream.Collectors;

public interface DSPDMConstants {

    public static final String EMPTY_STRING = "";
    public static final char DOT = '.';
    public static final char SPACE = ' ';
    public static final char COMMA = ',';
    public static final char OPEN_PARENTHESIS = '(';
    public static final char CLOSE_PARENTHESIS = ')';
    public static final char ASTERISK = '*';
    public static final String REFERENCE_DATA = "REFERENCE_DATA";
    public static final String REFERENCE_DATA_FOR_FILTERS = "REFERENCE_DATA_FOR_FILTERS";
    public static final String REFERENCE_DATA_CONSTRAINTS = "REFERENCE_DATA_CONSTRAINTS";
    public static final Charset UTF_8 = StandardCharsets.UTF_8;
    public static final List<String> SEARCH_DB_BO_NAMES = Collections.unmodifiableList(Arrays.asList(BoName.BO_SEARCH));
    public static final List<String> SERVICE_DB_BO_NAMES = Collections.unmodifiableList(Arrays.asList(
            BoName.BO_SEARCH,
            BoName.BUS_OBJ_ATTR_CHANGE_HISTORY,
            BoName.BUS_OBJ_ATTR_SEARCH_INDEXES,
            BoName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS,
            BoName.BUS_OBJ_RELATIONSHIP,
            BoName.BUSINESS_OBJECT,
            BoName.BUSINESS_OBJECT_ATTR,
            BoName.BUSINESS_OBJECT_GROUP,
            BoName.R_BUSINESS_OBJECT_OPR,
            BoName.R_FAVORITE_FUNCTION,
            BoName.R_GROUP_CATEGORY,
            BoName.UNIT_TEMPLATE_DETAILS,
            BoName.UNIT_TEMPLATES,
            BoName.USER_CURRENT_UNIT_TEMPLATES,
            BoName.USER_FAVORITE,
            BoName.USER_PERFORMED_OPR,
            BoName.USER_UNIT_SETTINGS));

    public static final List<String> SERVICE_DB_BO_TABLE_NAMES = Collections.unmodifiableList(Arrays.asList(
            BoTableName.BO_SEARCH,
            BoTableName.BUS_OBJ_ATTR_CHANGE_HISTORY,
            BoTableName.BUS_OBJ_ATTR_SEARCH_INDEXES,
            BoTableName.BUS_OBJ_ATTR_UNIQ_CONSTRAINTS,
            BoTableName.BUS_OBJ_RELATIONSHIP,
            BoTableName.BUSINESS_OBJECT,
            BoTableName.BUSINESS_OBJECT_ATTR,
            BoTableName.BUSINESS_OBJECT_GROUP,
            BoTableName.R_BUSINESS_OBJECT_OPR,
            BoTableName.R_FAVORITE_FUNCTION,
            BoTableName.R_GROUP_CATEGORY,
            BoTableName.UNIT_TEMPLATE_DETAILS,
            BoTableName.UNIT_TEMPLATES,
            BoTableName.USER_CURRENT_UNIT_TEMPLATES,
            BoTableName.USER_FAVORITE,
            BoTableName.USER_PERFORMED_OPR,
            BoTableName.USER_UNIT_SETTINGS));

    public static final List<String> NO_CHANGE_TRACK_BO_NAMES = Collections.unmodifiableList(Arrays.asList(
            BoName.BO_SEARCH,
            BoName.BUS_OBJ_ATTR_CHANGE_HISTORY,
            BoName.R_BUSINESS_OBJECT_OPR,
            BoName.USER_PERFORMED_OPR));
    public static final String AND = "and";
    public static final String UNDERSCORE = "_";
    public static final String DYNAMICALALIAS = "alias";
    public static final String FILTER_GROUP_KEY = "FILTER_GROUP_KEY";
    // GENERATE METADATA FOR ALL
    public static final String OVERWRITE_POLICY = "overwritePolicy";
    public static final String TABLE_TYPE = "tableType";
    public static final String EXCLUSION_LIST = "exclusionList";
    public static final String SKIP = "skip";
    public static final String ABORT = "abort";
    public static final String OVERWRITE = "overwrite";
    public static final String TABLE = "table";
    public static final String VIEW = "view";
    public static final String TABLE_VIEW = "table_view";
    public static final String DATABASE_NAME = "databaseName";
    public static final String SCHEMA_NAME = "schemaName";
    public static final int WRONG_DATATYPE_FROM_DRIVER_FOR_TIMESTAMP_WITH_TIMEZONE = -155;
    public static final int WRONG_DATATYPE_FROM_DRIVER_FOR_JSONB = 1111;
    public static final Double MAX_DOUBLE_VALUE_FOR_REAL_DATA_TYPE = 99999990.0;
    public static final Integer MAX_INTEGER_VALUE_FOR_REAL_DATA_TYPE = 99999990;
    public static final List<String> SPECIAL_CHARACTER_USED_FOR_WAF_RULES_BYPASSING = Collections.unmodifiableList(Arrays.asList(
            "#", "(", ")", "::", ">", "<", "!","@", "%", "&", "*", "^", "~"));
    public static final Boolean RESTRICT_USER_TO_DELETE_DATA_FIRST_TO_DELETE_CUSTOM_ATTR_FLAG = false;
    public static final String SEQUENCE_PREFIX = "SEQ_";
    public static final String SEQUENCE = "SEQUENCE";
    public static final String EXPANDED = "expanded";



    /**
     * For the following attributes we will convert the value to lower case after receiving the request
     */
    public static final Set<String> LOWER_CASE_BO_ATTR_NAMES = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(
            BoAttrName.ATTRIBUTE_DATATYPE
    )));
    /**
     * For the following attributes we will convert the value to upper case after receiving the request
     */
    public static final Set<String> UPPER_CASE_BO_ATTR_NAMES = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(
            BoAttrName.BO_NAME,
            BoAttrName.PARENT_BO_NAME,
            BoAttrName.CHILD_BO_NAME,
            BoAttrName.BO_ATTR_NAME,
            BoAttrName.PARENT_BO_ATTR_NAME,
            BoAttrName.CHILD_BO_ATTR_NAME,
            BoAttrName.BUS_OBJ_RELATIONSHIP_NAME,
            BoAttrName.CONSTRAINT_NAME,
            BoAttrName.INDEX_NAME,
            BoAttrName.REFERENCE_BO_NAME,
            BoAttrName.REFERENCE_BO_ATTR_VALUE,
            BoAttrName.REFERENCE_BO_ATTR_LABEL,
            BoAttrName.KEY_SEQ_NAME,
            BoAttrName.RELATED_BO_ATTR_NAME,
            BoAttrName.ENTITY,
            BoAttrName.CHILD_ENTITY_NAME,
            BoAttrName.PARENT_ENTITY_NAME,
            BoAttrName.ATTRIBUTE,
            BoAttrName.CHILD_ATTRIBUTE_NAME,
            BoAttrName.PARENT_ATTRIBUTE_NAME,
            BoAttrName.DEFAULT_ATTRIBUTE_NAME,
            BoAttrName.BO_ATTR_UNIT_ATTR_NAME
    )));
    /**
     * For following attributes we will replace the space with underscore after receiving the save request
     */
    public static final Set<String> NO_SPACE_BO_ATTR_NAMES = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(
            BoAttrName.BO_ATTR_NAME,
            BoAttrName.PARENT_BO_ATTR_NAME,
            BoAttrName.CHILD_BO_ATTR_NAME,
            BoAttrName.CONSTRAINT_NAME,
            BoAttrName.INDEX_NAME,
            BoAttrName.KEY_SEQ_NAME,
            BoAttrName.RELATED_BO_ATTR_NAME,
            BoAttrName.ENTITY,
            BoAttrName.CHILD_ENTITY_NAME,
            BoAttrName.PARENT_ENTITY_NAME,
            BoAttrName.ATTRIBUTE,
            BoAttrName.CHILD_ATTRIBUTE_NAME,
            BoAttrName.PARENT_ATTRIBUTE_NAME,
            BoAttrName.DEFAULT_ATTRIBUTE_NAME,
            BoAttrName.BO_ATTR_UNIT_ATTR_NAME
    )));

    public static interface SecurityServerParamNames {
        public static final String username = "username";
        public static final String userPass = "userPass";
        public static final String grant_type = "grant_type";
        public static final String password = "password";
        public static final String client_id = "client_id";
        public static final String client_secret = "client_secret";
        public static final String realmsPath = "/realms/";
        public static final String protocolPath = "/protocol/openid-connect/";
        public static final String userinfo = "userinfo";
        public static final String token = "token";
    }

    /**
     * Bo names which are un-secure. Means read/write to these bo names does not need specific permission.
     * Any valid user can read and write data to these business objects
     *
     * @author rao.alikhan
     * @since 27-07-2020
     */
    public static enum UNSECURE_BO_NAMES {
        UNIT_TEMPLATES(Units.UNIT_TEMPLATES),
        USER_CURRENT_UNIT_TEMPLATES(Units.USER_CURRENT_UNIT_TEMPLATES),
        USER_UNIT_SETTINGS(Units.USER_UNIT_SETTINGS),
        UNIT_TEMPLATE_DETAILS(Units.UNIT_TEMPLATE_DETAILS);

        UNSECURE_BO_NAMES(String boName) {
            this.boName = boName;
        }

        private String boName = null;


        public String getBoName() {
            return boName;
        }

        /**
         * returns true of the given bo name is unsecure
         *
         * @param boName
         * @return
         */
        public static boolean isUnSecureBoName(String boName) {
            boolean isUnSecure = false;
            for (UNSECURE_BO_NAMES unSecureBoName : UNSECURE_BO_NAMES.values()) {
                if (unSecureBoName.getBoName().equalsIgnoreCase(boName)) {
                    isUnSecure = true;
                    break;
                }
            }
            return isUnSecure;
        }
    }

    /**
     * API permissions for services access
     *
     * @author rao.alikhan
     * @since 16-07-2020
     */
    public static interface ApiPermission {
        /**
         * This will allow all APIs access to the user
         */
        public static final String SYSTEM_ADMIN = "System Admin";
        /**
         * This permission will allow only adding custom business object
         */
        public static final String CUSTOM_BUSINESS_OBJECT_ADD = "Custom Business Object Add";
        /**
         * This permission will allow only deleting custom business object
         */
        public static final String CUSTOM_BUSINESS_OBJECT_DELETE = "Custom Business Object Delete";
        /**
         * This permission will allow only deleting custom business object
         */
        public static final String CUSTOM_UNIQUE_CONSTRAINT = "Custom Unique Constraint";
        /**
         * This permission will allow only adding/dropping custom search index
         */
        public static final String CUSTOM_SEARCH_INDEX = "Custom Search Index";
        /**
         * This permission will allow only deleting custom business object
         */
        public static final String CUSTOM_RELATIONSHIP = "Custom Relationship";
        /**
         * This permission will allow only adding custom attributes
         */
        public static final String CUSTOM_ATTRIBUTE = "Custom Attribute";
        /**
         * This permission will allow save/update/import/parse APIs
         * For deleting attribute user need to have delete access
         */
        public static final String EDIT = "Edit";
        /**
         * This permission will allow all delete request for business objects and custom attributes
         */
        public static final String DELETE = "Delete";
        /**
         * This permission will allow exporting of data to the user
         */
        public static final String EXPORT = "Export";
        /**
         * This permission will allow reading of data to the user, Right now if the user has the valid token then by default this permission will be given
         */
        public static final String VIEW = "View";
        /**
         * This permission will allow only specific operation which are open to everyone
         */
        public static final String OPEN = "Open";
        /**
         * This permission will allow user to generate metadata for existing table/view
         */
        public static final String GENERATE_METADATA_FOR_EXISTING_TABLE_VIEW = "Generate Metadata For Existing Table_View";
        /**
         * This permission will allow user to delete metadata for existing table/view
         */
        public static final String DELETE_METADATA_FOR_EXISTING_TABLE_VIEW = "Delete Metadata For Existing Table_View";
        /**
         * This permission will allow user to generate metadata for all table/views in given/connected database
         */
        public static final String GENERATE_METADATA_FOR_ALL = "Generate Metadata For All";
        /**
         * This permission will allow user to delete metadata for all table/views in given/connected database
         */
        public static final String DELETE_METADATA_FOR_ALL = "Delete Metadata For All";
    }

    /**
     * Security Server Policies for API permissions
     * In Security Server under the clients-><client_name>->Authorization->Policies Data Operations defined for API Permissions
     *
     * @author rao.alikhan
     * @since 17-07-2020
     */
    public static interface SecurityServerApiPermission {
        /**
         * This will allow all APIs access to the user
         */
        public static final String SYSTEM_ADMIN = "System Admin";
        /**
         * Security Server Policy to add custom unique constraint
         */
        public static final String CUSTOM_UNIQUE_CONSTRAINT = "Data Operations_Custom Unique Constraint";
        /**
         * Security Server Policy to add custom unique constraint
         */
        public static final String CUSTOM_SEARCH_INDEX = "Data Operations_Custom Search Index";
        /**
         * Security Server Policy to add custom business object add only
         */
        public static final String CUSTOM_BUSINESS_OBJECT_ADD = "Data Operations_Custom Business Object Add";
        /**
         * Security Server Policy to add custom business object delete only
         */
        public static final String CUSTOM_BUSINESS_OBJECT_DELETE = "Data Operations_Custom Business Object Delete";
        /**
         * Security Server Policy to add custom attribute
         */
        public static final String CUSTOM_RELATIONSHIP = "Data Operations_Custom Relationship";
        /**
         * Security Server Policy to add custom attribute
         */
        public static final String CUSTOM_ATTRIBUTE = "Data Operations_Custom Attribute";
        /**
         * Security Server policy to use save/update/import/parse
         */
        public static final String EDIT = "Data Operations_Edit";
        /**
         * Security Server Policy to use delete APIs
         */
        public static final String DELETE = "Data Operations_Delete";
        /**
         * Security Server Policy to use export API
         */
        public static final String EXPORT = "Data Operations_Export";
        /**
         * This permission will allow reading of data to the user, Right now if the user has the valid token then by default this permission will be given
         */
        public static final String VIEW = "Data Operations_View";
        /**
         * This permission will allow only specific operation which are open to everyone
         */
        public static final String OPEN = "Open";
        /**
         * This permission will allow user to generate metadata for existing table/view
         */
        public static final String GENERATE_METADATA_FOR_EXISTING_TABLE_VIEW = "Generate Metadata For Existing Table_View";
        /**
         * This permission will allow user to delete metadata for existing table/view
         */
        public static final String DELETE_METADATA_FOR_EXISTING_TABLE_VIEW = "Delete Metadata For Existing Table_View";
        /**
         * This permission will allow user to generate metadata for all table/views in given/connected database
         */
        public static final String GENERATE_METADATA_FOR_ALL = "Generate Metadata For All";
        /**
         * This permission will allow user to delete metadata for all table/views in given/connected database
         */
        public static final String DELETE_METADATA_FOR_ALL = "Delete Metadata For All";
    }

    public static enum APIPermissionsMapping {
        SYSTEM_ADMIN(SecurityServerApiPermission.SYSTEM_ADMIN, ApiPermission.SYSTEM_ADMIN),
        EDIT(SecurityServerApiPermission.EDIT, ApiPermission.EDIT),
        DELETE(SecurityServerApiPermission.DELETE, ApiPermission.DELETE),
        EXPORT(SecurityServerApiPermission.EXPORT, ApiPermission.EXPORT),
        OPEN(SecurityServerApiPermission.OPEN, ApiPermission.OPEN),
        VIEW(SecurityServerApiPermission.VIEW, ApiPermission.VIEW),
        CUSTOM_RELATIONSHIP(SecurityServerApiPermission.CUSTOM_RELATIONSHIP, ApiPermission.CUSTOM_RELATIONSHIP),
        CUSTOM_UNIQUE_CONSTRAINT(SecurityServerApiPermission.CUSTOM_UNIQUE_CONSTRAINT, ApiPermission.CUSTOM_UNIQUE_CONSTRAINT),
        CUSTOM_SEARCH_INDEX(SecurityServerApiPermission.CUSTOM_SEARCH_INDEX, ApiPermission.CUSTOM_SEARCH_INDEX),
        CUSTOM_BUSINESS_OBJECT_ADD(SecurityServerApiPermission.CUSTOM_BUSINESS_OBJECT_ADD, ApiPermission.CUSTOM_BUSINESS_OBJECT_ADD),
        CUSTOM_BUSINESS_OBJECT_DELETE(SecurityServerApiPermission.CUSTOM_BUSINESS_OBJECT_DELETE, ApiPermission.CUSTOM_BUSINESS_OBJECT_DELETE),
        CUSTOM_ATTRIBUTE(SecurityServerApiPermission.CUSTOM_ATTRIBUTE, ApiPermission.CUSTOM_ATTRIBUTE),
        GENERATE_METADATA_FOR_EXISTING_TABLE_VIEW(SecurityServerApiPermission.GENERATE_METADATA_FOR_EXISTING_TABLE_VIEW, ApiPermission.GENERATE_METADATA_FOR_EXISTING_TABLE_VIEW),
        DELETE_METADATA_FOR_EXISTING_TABLE_VIEW(SecurityServerApiPermission.DELETE_METADATA_FOR_EXISTING_TABLE_VIEW, ApiPermission.DELETE_METADATA_FOR_EXISTING_TABLE_VIEW),
        GENERATE_METADATA_FOR_ALL(SecurityServerApiPermission.GENERATE_METADATA_FOR_ALL, ApiPermission.GENERATE_METADATA_FOR_ALL),
        DELETE_METADATA_FOR_ALL(SecurityServerApiPermission.DELETE_METADATA_FOR_ALL, ApiPermission.DELETE_METADATA_FOR_ALL);

        private APIPermissionsMapping(String securityServerPolicyName, String apiPermissionName) {
            this.securityServerPolicyName = securityServerPolicyName;
            this.apiPermissionName = apiPermissionName;
        }

        private String securityServerPolicyName = null;
        private String apiPermissionName = null;

        public String getSecurityServerPolicyName() {
            return securityServerPolicyName;
        }

        public String getApiPermissionName() {
            return apiPermissionName;
        }
    }

    public static interface SystemPropertyNames {
        /**
         * following is the system property name which we can use to lookup
         */
        public String PATH_TO_CONFIG_ROOT_DIR = "pathToConfigRootDir";
    }

    public static interface ConfigFileNames {
        public String SECURITY_SERVER_FILE_NAME = "security-server.properties";
        public String CONFIG_FILE_NAME = "config.properties";
        public String CONNECTION_FILE_NAME = "connection.properties";
        public String WAF_BYPASS_RULES_FILE_NAME = "wafbypassrules.properties";
        public String LOG4J2_FILE_NAME = "log4j2.xml";
    }

    public static enum DBOperation {
        CREATE, READ, UPDATE, DELETE;
    }

    public static enum JsonbOperators {
        OR("|");

        JsonbOperators(String operator) {
            this.operator = operator;
        }

        private String operator;

        public String getOperator() {
            return operator;
        }

        public void setOperator(String operator) {
            this.operator = operator;
        }
    }

    public static interface Separator {
        // This is a separator for bo search table which will separate composite keys and its values
        public static final String BO_SEARCH_ATTR_SEPARATOR = ":::";
    }

    public static interface CRAWLER_OPERATION {
        public static final String CREATE = "create";
        public static final String UPDATE = "update";
        public static final String DELETE = "delete";
        public static final String DEFINE = "define";
        public static final String DROP = "drop";
    }

    public static interface DEFAULT_VALUES {
        public static final boolean IS_ACTIVE_FALSE = false;
        public static final boolean IS_ACTIVE_TRUE = true;
    }

    public static interface SEARCH_INDEX_USE_CASE {
        public static final String UPPER = "UPPER";
        public static final String LOWER = "LOWER";
    }

    public static interface ENV {
        public static final String QA = "qa";
        public static final String DEV = "dev";
        public static final String LOCALHOST = "localhost";
        public static final String OEC = "oec";
        public static final String DEMO = "demo";
        public static final String UNITTEST = "unittest";
    }

    public static interface SQL {
        public static final int MAX_BATCH_INSERT_SIZE = 100;
        public static final int MAX_BATCH_DELETE_SIZE = 100;
        public static final int MAX_SQL_IN_ARGS = 256;
        public static final char SQL_PARAM_PLACEHOLDER = '?';
        public static final String SQL_LIKE_OPERAND = "%";
        public static final List<Integer> NUMERIC_DATA_TYPES = Arrays.asList(Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT, Types.FLOAT, Types.REAL, Types.DOUBLE, Types.NUMERIC, Types.DECIMAL);

        public static interface SQLDialect {
            public static final String POSTGRES = "postgres";
            public static final String MSSQLSERVER = "mssqlserver";
        }
    }

    public static interface SchemaName {
        public static final String TABLE_SCHEMA = "TABLE_SCHEM";
        public static final String SEQUENCE_NAME = "SEQUENCE_NAME";
        public static final String TABLE_NAME = "TABLE_NAME";
        public static final String TABLE_TYPE = "TABLE_TYPE";
        public static final String COLUMN_NAME = "COLUMN_NAME";
        public static final String DATA_TYPE = "TYPE_NAME";
        public static final String COLUMN_SIZE = "COLUMN_SIZE";
        public static final String DECIMAL_DIGITS = "DECIMAL_DIGITS";
        public static final String IS_NULLABLE = "IS_NULLABLE";
        public static final String DEFAULT_VALUE = "COLUMN_DEF";
        public static final String ORDINAL_POSITION = "ORDINAL_POSITION";
        public static final String REMARKS = "REMARKS";
        public static final String PRIMARYKEY_NAME = "PK_NAME";
        public static final String PKTABLE_SCHEMA = "PKTABLE_SCHEM";
        public static final String PKTABLE_NAME = "PKTABLE_NAME";
        public static final String PKCOLUMN_NAME = "PKCOLUMN_NAME";
        public static final String FOREIGNKEY_TABLE = "FKTABLE_NAME";
        public static final String FOREIGNKEY_COLUMN = "FKCOLUMN_NAME";
        public static final String FOREIGNKEY_NAME = "FK_NAME";
        public static final String KEY_SEQ = "KEY_SEQ";
        public static final String INDEX_NAME = "INDEX_NAME";
        public static final String INDEX_TYPE = "TYPE";
        public static final String INDEX = "INDEX";
        public static final String UNIQUE_INDEX = "UNIQUE_INDEX";
        public static final String SEARCH_INDEX = "SEARCH_INDEX";
        public static final String TABLE = "TABLE";
        public static final String SEQUENCE = "SEQUENCE";
        public static final String COLUMN = "COLUMN";
        public static final String FOREIGNKEY = "FOREIGNKEY";
        public static final String PRIMARYKEY = "PRIMARYKEY";
        public static final String SQL_DATA_TYPE = "DATA_TYPE";
        public static final String NON_UNIQUE = "NON_UNIQUE";
    }

    public static interface BoName {
        public static final String R_AREA_LEVEL = "R AREA LEVEL";
        public static final String AREA = "AREA";
        public static final String BUSINESS_OBJECT = "BUSINESS OBJECT";
        public static final String BUSINESS_OBJECT_ATTR = "BUSINESS OBJECT ATTR";
        public static final String BUSINESS_OBJECT_ATTR_UNIT = "BUSINESS OBJECT ATTR UNIT";
        public static final String BUS_OBJ_RELATIONSHIP = "BUS OBJ RELATIONSHIP";
        public static final String BO_SEARCH = "BO SEARCH";
        public static final String WELL_VOL_DAILY = "WELL VOL DAILY";
        public static final String BUSINESS_OBJECT_GROUP = "BUSINESS OBJECT GROUP";
        public static final String BUS_OBJ_ATTR_UNIQ_CONSTRAINTS = "BUS OBJ ATTR UNIQ CONSTRAINTS";
        public static final String BUS_OBJ_ATTR_SEARCH_INDEXES = "BUS OBJ ATTR SEARCH INDEXES";
        public static final String R_ENTITY_TYPE = "R ENTITY TYPE";
        public static final String NODE = "NODE";
        public static final String R_GROUP_CATEGORY = "R GROUP CATEGORY";
        public static final String R_BUSINESS_OBJECT_OPR = "R BUSINESS OBJECT OPR";
        public static final String USER_PERFORMED_OPR = "USER PERFORMED OPR";
        public static final String BUS_OBJ_ATTR_CHANGE_HISTORY = "BUS OBJ ATTR CHANGE HISTORY";

        public static final String R_FAVORITE_FUNCTION = "R FAVORITE FUNCTION";
        public static final String UNIT_TEMPLATE_DETAILS = "UNIT TEMPLATE DETAILS";
        public static final String UNIT_TEMPLATES = "UNIT TEMPLATES";
        public static final String USER_CURRENT_UNIT_TEMPLATES = "USER CURRENT UNIT TEMPLATES";
        public static final String USER_FAVORITE = "USER FAVORITE";
        public static final String USER_UNIT_SETTINGS = "USER UNIT SETTINGS";
        public static final String R_REPORTING_ENTITY_KIND = "R REPORTING ENTITY KIND";
        public static final String REPORTING_ENTITY = "REPORTING ENTITY";
        public static final String PRODUCT_VOLUME_SUMMARY = "PRODUCT VOLUME SUMMARY";

    }

    public static interface BoTableName {
        public static final String R_AREA_LEVEL = "R_AREA_LEVEL";
        public static final String AREA = "AREA";
        public static final String BUSINESS_OBJECT = "BUSINESS_OBJECT";
        public static final String BUSINESS_OBJECT_ATTR = "BUSINESS_OBJECT_ATTR";
        public static final String BUSINESS_OBJECT_ATTR_UNIT = "BUSINESS_OBJECT_ATTR_UNIT";
        public static final String BUS_OBJ_RELATIONSHIP = "BUS_OBJ_RELATIONSHIP";
        public static final String BO_SEARCH = "BO_SEARCH";
        public static final String WELL_VOL_DAILY = "WELL_VOL_DAILY";
        public static final String BUSINESS_OBJECT_GROUP = "BUSINESS_OBJECT_GROUP";
        public static final String BUS_OBJ_ATTR_UNIQ_CONSTRAINTS = "BUS_OBJ_ATTR_UNIQ_CONSTRAINTS";
        public static final String BUS_OBJ_ATTR_SEARCH_INDEXES = "BUS_OBJ_ATTR_SEARCH_INDEXES";
        public static final String R_ENTITY_TYPE = "R_ENTITY_TYPE";
        public static final String NODE = "NODE";
        public static final String R_GROUP_CATEGORY = "R_GROUP_CATEGORY";
        public static final String R_BUSINESS_OBJECT_OPR = "R_BUSINESS_OBJECT_OPR";
        public static final String USER_PERFORMED_OPR = "USER_PERFORMED_OPR";
        public static final String BUS_OBJ_ATTR_CHANGE_HISTORY = "BUS_OBJ_ATTR_CHANGE_HISTORY";


        public static final String R_FAVORITE_FUNCTION = "R_FAVORITE_FUNCTION";
        public static final String UNIT_TEMPLATE_DETAILS = "UNIT_TEMPLATE_DETAILS";
        public static final String UNIT_TEMPLATES = "UNIT_TEMPLATES";
        public static final String USER_CURRENT_UNIT_TEMPLATES = "USER_CURRENT_UNIT_TEMPLATES";
        public static final String USER_FAVORITE = "USER_FAVORITE";
        public static final String USER_UNIT_SETTINGS = "USER_UNIT_SETTINGS";

    }

    public static interface BoAttrName {
        // BUSINESS_OBJECT TABLE STARTco
        public static final String BUSINESS_OBJECT_ID = "BUSINESS_OBJECT_ID";
        public static final String BO_NAME = "BO_NAME";
        public static final String BO_DISPLAY_NAME = "BO_DISPLAY_NAME";
        public static final String ENTITY = "ENTITY";
        public static final String BO_DESC = "BO_DESC";
        public static final String IS_ACTIVE = "IS_ACTIVE";
        public static final String IS_MASTER_DATA = "IS_MASTER_DATA";
        public static final String IS_OPERATIONAL_TABLE = "IS_OPERATIONAL_TABLE";
        public static final String IS_RESULT_TABLE = "IS_RESULT_TABLE";
        public static final String IS_METADATA_TABLE = "IS_METADATA_TABLE";
        public static final String IS_REFERENCE_TABLE = "IS_REFERENCE_TABLE";
        public static final String SCHEMA_NAME = "SCHEMA_NAME";
        public static final String KEY_SEQ_NAME = "KEY_SEQ_NAME";
        public static final String ROW_CREATED_BY = "ROW_CREATED_BY";
        public static final String ROW_CREATED_DATE = "ROW_CREATED_DATE";
        public static final String ROW_CHANGED_BY = "ROW_CHANGED_BY";
        public static final String ROW_CHANGED_DATE = "ROW_CHANGED_DATE";
        public static final String UNIT = "UNIT";
        public static final String SOURCEUNIT = "sourceUnit";
        public static final String TARGETEUNIT = "targetUnit";
        // BUSINESS_OBJECT TABLE END
        // BUSINESS_OBJECT_ATTR TABLE START
        public static final String BUSINESS_OBJECT_ATTR_ID = "BUSINESS_OBJECT_ATTR_ID";
        public static final String BO_ATTR_NAME = "BO_ATTR_NAME";
        public static final String ATTRIBUTE = "ATTRIBUTE";
        public static final String ATTRIBUTE_DATATYPE = "ATTRIBUTE_DATATYPE";
        public static final String ATTRIBUTE_DEFAULT = "ATTRIBUTE_DEFAULT";
        public static final String ATTRIBUTE_DESC = "ATTRIBUTE_DESC";
        public static final String ATTRIBUTE_DISPLAYNAME = "ATTRIBUTE_DISPLAYNAME";
        public static final String CONTROL_TYPE = "CONTROL_TYPE";
        public static final String HISTORY_ATTRIBUTE = "HISTORY_ATTRIBUTE";
        public static final String HISTORY_TABLE = "HISTORY_TABLE";
        public static final String IS_HIDDEN = "IS_HIDDEN";
        public static final String IS_MANDATORY = "IS_MANDATORY";
        public static final String IS_PRIMARY_KEY = "IS_PRIMARY_KEY";
        public static final String IS_REFERENCE_IND = "IS_REFERENCE_IND";
        public static final String IS_SORTABLE = "IS_SORTABLE";
        public static final String IS_UPLOAD_NEEDED = "IS_UPLOAD_NEEDED";
        public static final String IS_INTERNAL = "IS_INTERNAL";
        public static final String IS_READ_ONLY = "IS_READ_ONLY";
        public static final String RELATED_BO_ATTR_NAME = "RELATED_BO_ATTR_NAME";
        public static final String REFERENCE_BO_NAME = "REFERENCE_BO_NAME";
        public static final String REFERENCE_BO_ATTR_VALUE = "REFERENCE_BO_ATTR_VALUE";
        public static final String REFERENCE_BO_ATTR_LABEL = "REFERENCE_BO_ATTR_LABEL";
        public static final String SEQUENCE_NUM = "SEQUENCE_NUM";
        public static final String IS_CUSTOM_ATTRIBUTE = "IS_CUSTOM_ATTRIBUTE";
        public static final String IS_NOT_NULL = "IS_NOT_NULL";
        // BUSINESS_OBJECT ATTR END
        public static final String JAVA_DATA_TYPE = "javaDataType";
        public static final String SQL_DATA_TYPE = "sqlDataType";
        public static final String MAX_ALLOWED_LENGTH = "maxAllowedLength";
        public static final String MAX_ALLOWED_DECIMAL_PLACES = "maxAllowedDecimalPlaces";
        // BUSINESS_OBJECT ATTR Constraints Start
        public static final String BUS_OBJ_ATTR_UNIQ_CONS_ID = "BUS_OBJ_ATTR_UNIQ_CONS_ID";
        public static final String CONSTRAINT_NAME = "CONSTRAINT_NAME";

        public static final String VERIFY = "VERIFY";

        // BUSINESS_OBJECT ATTR SEARCH INDEX Start
        public static final String BUS_OBJ_ATTR_SEARCH_INDEX_ID = "BUS_OBJ_ATTR_SEARCH_INDEX_ID";
        public static final String INDEX_NAME = "INDEX_NAME";
        public static final String USE_CASE = "USE_CASE";
        // BUSINESS_OBJECT ATTR Constraints End
        // BUSINESS_OBJECT Relationships Start
        public static final String BUS_OBJ_RELATIONSHIP_ID = "BUS_OBJ_RELATIONSHIP_ID";
        public static final String BUS_OBJ_RELATIONSHIP_NAME = "BUS_OBJ_RELATIONSHIP_NAME";
        public static final String CHILD_BO_NAME = "CHILD_BO_NAME";
        public static final String CHILD_BO_ATTR_NAME = "CHILD_BO_ATTR_NAME";
        public static final String PARENT_BO_NAME = "PARENT_BO_NAME";
        public static final String PARENT_BO_ATTR_NAME = "PARENT_BO_ATTR_NAME";
        public static final String IS_PRIMARY_KEY_RELATIONSHIP = "IS_PRIMARY_KEY_RELATIONSHIP";
        public static final String CHILD_ENTITY_NAME = "CHILD_ENTITY_NAME";
        public static final String PARENT_ENTITY_NAME = "PARENT_ENTITY_NAME";
        public static final String CHILD_ATTRIBUTE_NAME = "CHILD_ATTRIBUTE_NAME";
        public static final String PARENT_ATTRIBUTE_NAME = "PARENT_ATTRIBUTE_NAME";
        public static final String CHILD_ORDER = "CHILD_ORDER";
        // BUSINESS_OBJECT Relationships End
        // BUSINESS_OBJECT Attribute Unit Start
        public static final String BUSINESS_OBJECT_ATTR_UNIT_ID = "BUSINESS_OBJECT_ATTR_UNIT_ID";
        public static final String BO_ATTR_UNIT_ATTR_NAME = "BO_ATTR_UNIT_ATTR_NAME";
        // BUSINESS_OBJECT Attribute Unit End
        public static final String R_AREA_LEVEL_ID = "R_AREA_LEVEL_ID";
        public static final String PARENT_AREA_ID = "PARENT_AREA_ID";
        public static final String AREA_ID = "AREA_ID";
        // Business Object Search attributes
        public static final String BO_SEARCH_ID = "BO_SEARCH_ID";
        public static final String PK_BO_ATTR_NAMES = "PK_BO_ATTR_NAMES";
        public static final String PK_BO_ATTR_VALUES = "PK_BO_ATTR_VALUES";
        public static final String SEARCH_JSONB = "SEARCH_JSONB";
        public static final String OBJECT_JSONB = "OBJECT_JSONB";
        // WELL VOL DAILY attributes
        public static final String WELL_ID = "WELL_ID";
        public static final String UWI = "UWI";
        public static final String VOLUME_DATE = "VOLUME_DATE";
        public static final String BOE_VOLUME = "BOE_VOLUME";
        // R_ENTITY_TYPE
        public static final String DEFAULT_ATTRIBUTE_NAME = "DEFAULT_ATTRIBUTE_NAME";
        public static final String ENTITY_TYPE = "ENTITY_TYPE";
        public static final String ENTITY_NAME = "ENTITY_NAME";
        // BUSINESS OBJECT GROUP
        public static final String R_GROUP_CATEGORY_ID="R_GROUP_CATEGORY_ID";
        public static final String GROUP_CATEGORY_NAME="GROUP_CATEGORY_NAME";
        public static final String DISPLAY_NAME = "DISPLAY_NAME";
        public static final String BUSINESS_OBJECT_GROUP_ID = "BUSINESS_OBJECT_GROUP_ID";
        // SAVING HISTORY
        public static final String USER_PERFORMED_OPR_ID = "USER_PERFORMED_OPR_ID";
        public static final String R_BUSINESS_OBJECT_OPR_ID = "R_BUSINESS_OBJECT_OPR_ID";
        public static final String OPERATION_SHORT_NAME = "OPERATION_SHORT_NAME";
        public static final String OPERATION_NAME = "OPERATION_NAME";
        public static final String EFFECTED_ROWS_COUNT = "EFFECTED_ROWS_COUNT";
        public static final String PK_ID = "PK_ID";
        public static final String OLD_VALUE = "OLD_VALUE";
        public static final String CHANGED_VALUE = "CHANGED_VALUE";
        public static final String OPR_SEQUENCE_NUMBER = "OPR_SEQUENCE_NUMBER";
        // REPORTING ENTITY KIND
        public static final String REPORTING_ENTITY_KIND = "REPORTING_ENTITY_KIND";
        public static final String R_REPORTING_ENTITY_KIND_ID = "R_REPORTING_ENTITY_KIND_ID";
        public static final String ASSOCIATED_OBJECT_ID = "ASSOCIATED_OBJECT_ID";
        public static final String ASSOCIATED_OBJECT_NAME = "ASSOCIATED_OBJECT_NAME";
        // WELL
        public static final String REMARK = "REMARK";

        public static boolean isAuditField(String boAttrName) {
            if (DSPDMConstants.BoAttrName.ROW_CHANGED_BY.equalsIgnoreCase(boAttrName)
                    || DSPDMConstants.BoAttrName.ROW_CHANGED_DATE.equalsIgnoreCase(boAttrName)
                    || DSPDMConstants.BoAttrName.ROW_CREATED_BY.equalsIgnoreCase(boAttrName)
                    || DSPDMConstants.BoAttrName.ROW_CREATED_DATE.equalsIgnoreCase(boAttrName)) {
                return true;
            }
            return false;
        }
    }


    public static enum Status {
        INFO(0, "Info"),
        SUCCESS(1, "Success"),
        PARTIAL_SUCCESS(2, "Partial Success"),
        WARNING(-1, "Warning"),
        ERROR(-2, "Error"),
        FATAL(-3, "Error"),
        SESSION_TIMEOUT(-4, "Session Timeout");

        private Status(int code, String label) {
            this.code = code;
            this.label = label;
        }

        private int code = 0;
        private String label = null;

        public int getCode() {
            return code;
        }

        public String getLabel() {
            return label;
        }
    }

    public static interface DEFAULT_USERS {
        public static final String TEST_USER = "**test";
        public static final String SYSTEM_USER = "**system";
        public static final String UNKNOWN_USER = "**unknown";
        public static final String OSDU_TEST_USER = "osdutest";
    }

    /**
     * keys to be used in maps for preparing response
     */
    public static interface DSPDM_RESPONSE {
        public static final String STATUS_KEY = "status";
        public static final String STATUS_CODE_KEY = "statusCode";
        public static final String STATUS_LABEL_KEY = "statusLabel";
        public static final String MESSAGES_KEY = "messages";
        public static final String MESSAGE_KEY = "message";
        public static final String EXCEPTION_KEY = "exception";
        public static final String STACK_TRACE_KEY = "stackTrace";
        public static final String DATA_KEY = "data";
        public static final String EXECUTION_CONTEXT_KEY = "executionContext";
        public static final String VERSION_KEY = "version";
        public static final String THREAD_NAME_KEY = "threadName";
        public static final String REQUEST_TIME_KEY = "requestTime";
        public static final String RESPONSE_TIME_KEY = "responseTime";
        public static final String TOTAL_RECORDS_KEY = "totalRecords";
        public static final String LIST_KEY = "list";
        public static final String TYPE_KEY = "type";
        public static final String ID_KEY = "id";
        public static final String PARSED_VALUES_KEY = "parsedValues";
        public static final String OLD_VALUES_KEY = "oldValues";
        public static final String INVALID_VALUES_KEY = "invalidValues";
        public static final String SQL_STATS_KEY = "sqlStats";
        public static final String SQL_SCRIPTS_KEY = "sqlScript";
        public static final String SQL_KEY = "sql";
        public static final String EXECUTION_COUNT_KEY = "executionCount";
        public static final String TOTAL_TIME_IN_MILLIS_KEY = "totalTimeInMillis";
        public static final String EXECUTOR_NAME_KEY = "executorName";
        public static final String EXECUTOR_LOCALE_KEY = "executorLocale";
        public static final String EXECUTOR_TIMEZONE_KEY = "executorTimeZone";
        public static final String TRANSACTION_STARTED_KEY = "transactionStarted";
        public static final String TRANSACTION_COMMITTED_KEY = "transactionCommitted";
        public static final String TRANSACTION_ROLL_BACKED_KEY = "transactionRollbacked";
        public static final String UNIT_TEST_CALL_KEY = "unitTestCall";
        public static final String PROCESSING_START_TIME_KEY = "processingStartTime";
        public static final String TOTAL_TIME_TAKEN_BY_DB_KEY = "totalTimeTakenByDB";
        public static final String READ_BACK_KEY = "readBack";
        public static final String SHOW_SQL_STATS_KEY = "showSQLStats";

        public static final String BO_NAME_KEY = "boName";
        public static final String BO_ATTR_NAME_KEY = "boAttrName";
        public static final String BO_ATTR_ID_KEY = "boAttrId";
        public static final String BO_ATTR_DISPLAY_NAME_KEY = "boAttrDisplayName";
        public static final String BO_ATTR_ALIAS_NAME_KEY = "boAttrAliasName";
        public static final String ROW_NUMBER_KEY = "rowNumber";
        public static final String VALUE_KEY = "value";
        public static final String SOURCE_UNIT_KEY = "sourceUnit";
        public static final String TARGET_UNIT_KEY = "targetUnit";
        public static final String CAN_CONVERT_UNITS_KEY = "canConversionUnits";

        public static final String UNIT_ID_KEY = "unit_id";
        public static final String BASE_UNIT_TYPE_KEY = "base_type_id";
        public static final String UNIT_NAME_KEY = "unit_name";
        public static final String UNIT_LABEL_KEY = "unit_label";
        public static final String SUPPORTED_DATATPES_KEY = "supportedDataTypes";
        public static final String WAF_BYPASS_RULES = "wafByPassRules";
    }

    public static interface DSPDM_REQUEST {
        public static final String LANGUAGE_KEY = "language";
        public static final String TIMEZONE_KEY = "timezone";
        public static final String READ_BACK_KEY = "readBack";
        public static final String FULL_DROP_KEY = "fullDrop";
        public static final String DELETE_CASCADE_KEY = "deleteCascade";
        public static final String SHOW_SQL_STATS_KEY = "showSQLStats";
        public static final String COLLECT_SQL_SCRIPT_KEY = "collectSQLScript";
        public static final String WRITE_NULL_VALUES_KEY = "writeNullValues";
        public static final String DATA_KEY = "data";
        public static final String CHILDREN_KEY = "children";
        public static final String OPERATION_NAME_KEY = "operationName";
    }

    public static interface REVERSE_ENGINEER_METADATA {

        public static enum BoAttrNameToColumnNameMapping {
            SCHEMA_NAME(BoAttrName.SCHEMA_NAME, "table_schem", "table_schem"),
            ENTITY(BoAttrName.ENTITY, "table_name", "table_name"),
            ATTRIBUTE(BoAttrName.ATTRIBUTE, "column_name", "column_name"),
            BO_ATTR_NAME(BoAttrName.BO_ATTR_NAME, "column_name", "column_name"),
            ATTRIBUTE_DISPLAYNAME(BoAttrName.ATTRIBUTE_DISPLAYNAME, "column_name", "column_name"),
            ATTRIBUTE_DATATYPE(BoAttrName.ATTRIBUTE_DATATYPE, "data_type", "data_type"),
            MAX_ALLOWED_LENGTH(BoAttrName.MAX_ALLOWED_LENGTH, "column_size", "column_size"),
            MAX_ALLOWED_DECIMAL_PLACES(BoAttrName.MAX_ALLOWED_DECIMAL_PLACES, "decimal_digits", "decimal_digits"),
            ATTRIBUTE_DESC(BoAttrName.ATTRIBUTE_DESC, "remarks", "remarks"),
            ATTRIBUTE_DEFAULT(BoAttrName.ATTRIBUTE_DEFAULT, "column_def", "column_def"),
            SEQUENCE_NUM(BoAttrName.SEQUENCE_NUM, "ordinal_position", "ordinal_position"),
            IS_MANDATORY(BoAttrName.IS_MANDATORY, "is_nullable", "is_nullable");

            BoAttrNameToColumnNameMapping(String boAttrName, String postgresColumnName, String msSQLServerColumnName) {
                this.boAttrName = boAttrName;
                this.postgresColumnName = postgresColumnName;
                this.msSQLServerColumnName = msSQLServerColumnName;
            }

            private String boAttrName = null;
            private String postgresColumnName = null;
            private String msSQLServerColumnName = null;

            public String getBoAttrName() {
                return boAttrName;
            }

            public String getPostgresColumnName() {
                return postgresColumnName;
            }

            public String getMsSQLServerColumnName() {
                return msSQLServerColumnName;
            }

            public static String getColumnName(String boAttrName, boolean isMSSQLServerDialect) {
                String columnName = null;
                if (isMSSQLServerDialect) {
                    for (BoAttrNameToColumnNameMapping mapping : values()) {
                        if (boAttrName.equalsIgnoreCase(mapping.boAttrName)) {
                            columnName = mapping.msSQLServerColumnName;
                            break;
                        }
                    }
                } else {
                    for (BoAttrNameToColumnNameMapping mapping : values()) {
                        if (boAttrName.equalsIgnoreCase(mapping.boAttrName)) {
                            columnName = mapping.postgresColumnName;
                            break;
                        }
                    }
                }
                return columnName;
            }
        }

        public static enum BusObjAttrUniqueConstraintToColumnNameMapping {
            SCHEMA_NAME(BoAttrName.SCHEMA_NAME, "table_schem", "table_schem"),
            ENTITY(BoAttrName.ENTITY, "table_name", "table_name"),
            ATTRIBUTE(BoAttrName.ATTRIBUTE, "column_name", "column_name"),
            BO_ATTR_NAME(BoAttrName.BO_ATTR_NAME, "column_name", "column_name"),
            CONSTRAINT_NAME(BoAttrName.CONSTRAINT_NAME, "index_name", "index_name");

            BusObjAttrUniqueConstraintToColumnNameMapping(String boAttrName, String postgresColumnName, String msSQLServerColumnName) {
                this.boAttrName = boAttrName;
                this.postgresColumnName = postgresColumnName;
                this.msSQLServerColumnName = msSQLServerColumnName;
            }

            private String boAttrName = null;
            private String postgresColumnName = null;
            private String msSQLServerColumnName = null;

            public String getBoAttrName() {
                return boAttrName;
            }

            public String getPostgresColumnName() {
                return postgresColumnName;
            }

            public String getMsSQLServerColumnName() {
                return msSQLServerColumnName;
            }

            public static String getColumnName(String boAttrName, boolean isMSSQLServerDialect) {
                String columnName = null;
                if (isMSSQLServerDialect) {
                    for (BusObjAttrUniqueConstraintToColumnNameMapping mapping : values()) {
                        if (boAttrName.equalsIgnoreCase(mapping.boAttrName)) {
                            columnName = mapping.msSQLServerColumnName;
                            break;
                        }
                    }
                } else {
                    for (BusObjAttrUniqueConstraintToColumnNameMapping mapping : values()) {
                        if (boAttrName.equalsIgnoreCase(mapping.boAttrName)) {
                            columnName = mapping.postgresColumnName;
                            break;
                        }
                    }
                }
                return columnName;
            }
        }

        public static enum BusObjRelationshipToColumnNameMapping {
            CHILD_BO_NAME(BoAttrName.CHILD_BO_NAME, "fktable_name", "fktable_name"),
            CHILD_BO_ATTR_NAME(BoAttrName.CHILD_BO_ATTR_NAME, "fkcolumn_name", "fkcolumn_name"),
            PARENT_BO_NAME(BoAttrName.PARENT_BO_NAME, "pktable_name", "pktable_name"),
            PARENT_BO_ATTR_NAME(BoAttrName.PARENT_BO_ATTR_NAME, "pkcolumn_name", "pkcolumn_name"),
            CHILD_ENTITY_NAME(BoAttrName.CHILD_ENTITY_NAME, "fktable_name", "fktable_name"),
            PARENT_ENTITY_NAME(BoAttrName.PARENT_ENTITY_NAME, "pktable_name", "pktable_name"),
            CONSTRAINT_NAME(BoAttrName.CONSTRAINT_NAME, "fk_name", "fk_name"),
            BUS_OBJ_RELATIONSHIP_NAME(BoAttrName.BUS_OBJ_RELATIONSHIP_NAME, "relation_name", "relation_name"),
            IS_PRIMARY_KEY_RELATIONSHIP(BoAttrName.IS_PRIMARY_KEY_RELATIONSHIP, "is_primary_key", "is_primary_key"),
            CHILD_ORDER(BoAttrName.CHILD_ORDER, "child_order", "child_order");

            BusObjRelationshipToColumnNameMapping(String boBusObjRelationshipAttrName, String postgresColumnName, String msSQLServerColumnName) {
                this.boBusObjRelationshipAttrName = boBusObjRelationshipAttrName;
                this.postgresColumnName = postgresColumnName;
                this.msSQLServerColumnName = msSQLServerColumnName;
            }

            private String boBusObjRelationshipAttrName = null;
            private String postgresColumnName = null;
            private String msSQLServerColumnName = null;

            public String getBoBusObjRelationshipAttrName() {
                return boBusObjRelationshipAttrName;
            }

            public String getPostgresColumnName() {
                return postgresColumnName;
            }

            public String getMsSQLServerColumnName() {
                return msSQLServerColumnName;
            }

            public static String getColumnName(String boAttrName, boolean isMSSQLServerDialect) {
                String columnName = null;
                if (isMSSQLServerDialect) {
                    for (BusObjRelationshipToColumnNameMapping mapping : values()) {
                        if (boAttrName.equalsIgnoreCase(mapping.boBusObjRelationshipAttrName)) {
                            columnName = mapping.msSQLServerColumnName;
                            break;
                        }
                    }
                } else {
                    for (BusObjRelationshipToColumnNameMapping mapping : values()) {
                        if (boAttrName.equalsIgnoreCase(mapping.boBusObjRelationshipAttrName)) {
                            columnName = mapping.postgresColumnName;
                            break;
                        }
                    }
                }
                return columnName;
            }
        }
    }

    public static enum DataTypes {
        /**
         * ******* If datatypeA starts with datatypeB, datatypeA must come after datatypteB in the list below.*******
         *
         * put character varying before character
         * put timestamp before time data type
         * put bigger before small in case if both starts with same like time and timestamp
         */

        BOOLEAN("boolean", "bit", java.lang.Boolean.class, new int[]{Types.BOOLEAN}, ControlTypes.CHECKBOX, 2000, null, null),
        BIT("boolean", "bit", java.lang.Boolean.class, new int[]{Types.BIT}, ControlTypes.CHECKBOX, 2000, null, null),
        SMALL_INT("smallint", "smallint", java.lang.Short.class, new int[]{Types.SMALLINT}, ControlTypes.INPUT_INT, 2000, null, null),
        INTEGER("integer", "int", java.lang.Integer.class, new int[]{Types.INTEGER}, ControlTypes.INPUT_INT, 4000, null, null),
        BIG_INT("bigint", "bigint", java.lang.Long.class, new int[]{Types.BIGINT}, ControlTypes.INPUT_INT, 4000, null, null),
        DECIMAL("decimal", "decimal", java.math.BigDecimal.class, new int[]{Types.DECIMAL}, ControlTypes.INPUT_NUMERIC, 4000, 38, 18),
        NUMERIC("numeric", "numeric", java.math.BigDecimal.class, new int[]{Types.NUMERIC}, ControlTypes.INPUT_NUMERIC, 4000, 38, 18),
        REAL("real", "real", java.lang.Float.class, new int[]{Types.REAL}, ControlTypes.INPUT_NUMERIC, 4000, null, null),
        // java data type for float must be java.lang.Double
        FLOAT("float", "float", java.lang.Double.class, new int[]{Types.FLOAT}, ControlTypes.INPUT_NUMERIC, 4000, null, null),
        DOUBLE_PRECISION("double precision", "float", java.lang.Double.class, new int[]{Types.DOUBLE}, ControlTypes.INPUT_NUMERIC, 4000, null, null),
        // character varying must come before character
        CHARACTER_VARYING("character varying", "varchar", java.lang.String.class, new int[]{Types.VARCHAR}, ControlTypes.INPUT, 5000, 4000, null),
        CHARACTER("character", "char", java.lang.String.class, new int[]{Types.CHAR}, ControlTypes.INPUT, 5000, 2000, null),
        TEXT("text", "text", java.lang.String.class, new int[]{Types.LONGVARCHAR}, ControlTypes.INPUT, 6000, null, null),
        // or byte Byte array or binary data
        BYTEA("bytea", "varbinary", java.lang.Byte.class, new int[]{Types.VARBINARY}, ControlTypes.INPUT, 8000, 8000, null),
        BYTE("char", "varbinary", java.lang.Byte.class, new int[]{Types.BINARY}, ControlTypes.INPUT, 1000, null, null),
        JSONB("jsonb", "jsonb", java.lang.String.class, new int[]{Types.LONGVARBINARY, DSPDMConstants.WRONG_DATATYPE_FROM_DRIVER_FOR_JSONB}, ControlTypes.INPUT, 6000, 10485760, null),
        // Following data types are just for sql server only
        // Unicode characters types only for MS SQL Server and must be defined after other character/string types
        N_CHARACTER_VARYING("character varying", "nvarchar", java.lang.String.class, new int[]{Types.NVARCHAR}, ControlTypes.INPUT, 5000, 2000, null),
        N_CHARACTER("character", "nchar", java.lang.String.class, new int[]{Types.NCHAR}, ControlTypes.INPUT, 5000, 1000, null),
        N_TEXT("text", "ntext", java.lang.String.class, new int[]{Types.LONGNVARCHAR}, ControlTypes.INPUT, 6000, null, null),
        BINARY("bytea", "binary", java.lang.Byte.class, new int[]{Types.BINARY}, ControlTypes.BINARY_IMAGE, 1000, 7500, null),
        IMAGE("bytea", "image", java.lang.Byte.class, new int[]{Types.BINARY}, ControlTypes.BINARY_IMAGE, 1000, null, null),
        TINY_INT("char", "tinyint", java.lang.Integer.class, new int[]{Types.TINYINT}, ControlTypes.INPUT_INT, 2000, null, null),
        // DATE and Time. timestamp must be defined before time and datetime must be defined before simple date
        TIMESTAMP_WITH_TIMEZONE("timestamp with time zone", "datetimeoffset", java.time.OffsetDateTime.class, new int[]{Types.TIMESTAMP_WITH_TIMEZONE, DSPDMConstants.WRONG_DATATYPE_FROM_DRIVER_FOR_TIMESTAMP_WITH_TIMEZONE}, ControlTypes.CALENDAR_WITH_DATETIME, 5000, null, null),
        // put big data type before small. Put smalldatetime before datetime2
        TIMESTAMP_WITHOUT_TIMEZONE("timestamp without time zone", "datetime", java.sql.Timestamp.class, new int[]{Types.TIMESTAMP}, ControlTypes.CALENDAR_WITH_DATETIME, 5000, null, null),
        TIMESTAMP("timestamp", "datetime", java.sql.Timestamp.class, new int[]{Types.TIMESTAMP}, ControlTypes.CALENDAR_WITH_DATETIME, 5000, null, null),
        SMALL_DATETIME("timestamp", "smalldatetime", java.sql.Timestamp.class, new int[]{Types.TIMESTAMP}, ControlTypes.CALENDAR_WITH_DATETIME, 5000, null, null),
        DATETIME2("timestamp", "datetime2", java.sql.Timestamp.class, new int[]{Types.TIMESTAMP}, ControlTypes.CALENDAR_WITH_DATETIME, 5000, null, null),
        DATE("date", "date", java.sql.Date.class, new int[]{Types.DATE}, ControlTypes.CALENDAR_WITH_DATE, 3000, null, null),
        TIME("time", "time", java.sql.Time.class, new int[]{Types.TIME}, ControlTypes.CALENDAR_WITH_DATETIME, 5000, null, null);

        DataTypes(java.lang.String attributeDataType, java.lang.String sqlServerAttributeDataType, Class javaClass, int[] sqlDataType, String controlType, int columnWidthInExcel, Integer maxLength, Integer maxDecimalLength) {
            this.attributeDataType = attributeDataType;
            this.sqlServerAttributeDataType = sqlServerAttributeDataType;
            this.javaClass = javaClass;
            this.sqlDataTypes = sqlDataType;
            this.controlType = controlType;
            this.columnWidthInExcel = columnWidthInExcel;
            this.maxLength = maxLength;
            this.maxDecimalLength = maxDecimalLength;
        }

        /**
         * data type name for postgres
         */
        private String attributeDataType = null;
        /**
         * data type name for sql server only
         */
        private String sqlServerAttributeDataType = null;
        private String controlType = null;
        private Class javaClass = null;
        private int[] sqlDataTypes = new int[]{Types.NULL};
        private int columnWidthInExcel = 0;
        private Integer maxLength = null;
        private Integer maxDecimalLength = null;
        public static final DataTypes PRIMARY_KEY_DATA_TYPE = INTEGER;
        private static List<Map<String, Object>> supportedDataTypes = null;
        public static final String ATTRIBUTE_DATATYPE = "attributeDataType";
        public static final String ATTRIBUTE_DATATYPE_SQL_SERVER = "sqlServerAttributeDataType";
        public static final String SQL_DATATYPE = "sqlDataType";
        public static final String JAVA_CLASS = "javaClass";
        public static final String CONTROL_TYPE = "controlType";
        public static final String COLUMN_WIDTH_IN_EXCEL = "columnWidthInExcel";
        public static final String MAX_LENGTH = "maxLength";
        public static final String MAX_DECIMAL_LENGTH = "maxDecimalLength";
        public static final String IS_MS_SQL_SERVER_DIALECT = "isMSSQLServerDialect";
        public static final String IS_DATE_TIME_DATATYPE = "isDateOrTimeDataType";
        public static final String IS_FLOATING_DATATYPE = "isFloatingDataType";
        public static final String IS_INTEGER_DATATYPE = "isIntegerDataType";
        public static final String IS_STRING_DATATYPE = "isStringDataType";
        public static final String IS_PRIMARY_KEY_DATATYPE = "isPrimaryKeyDataType";
        public static final String IS_STRING_DATATYPE_WITH_LENGTH = "isStringDataTypeWithLength";

        public String getAttributeDataType(boolean isMSSQLServerDialect) {
            if (isMSSQLServerDialect) {
                return sqlServerAttributeDataType;
            } else {
                return attributeDataType;
            }
        }

        public String getControlType() {
            return controlType;
        }

        public Class getJavaClass() {
            return javaClass;
        }

        public int[] getSqlDataTypes() {
            return sqlDataTypes;
        }

        public int getColumnWidthInExcel() {
            return columnWidthInExcel;
        }

        public Integer getMaxLength() {
            return maxLength;
        }

        public Integer getMaxDecimalLength() {
            return maxDecimalLength;
        }

        public static String fixDataTypeError(String dataType) {
            if (StringUtils.hasValue(dataType)) {
                dataType = dataType.trim().toLowerCase();
                while (dataType.contains(" (")) {
                    dataType = dataType.replace(" (", "(");
                }
            }
            return dataType;
        }

        public static DataTypes fromAttributeDataType(String attributeDataType, boolean isMSSQLServerDialect) {
        	//When confused database and the field type, automatically change it to the right data type
//            if (isMSSQLServerDialect) {
//                if (attributeDataType.toLowerCase().startsWith(DataTypes.CHARACTER_VARYING.getAttributeDataType(!isMSSQLServerDialect))) {
//                    attributeDataType = DataTypes.CHARACTER_VARYING.getAttributeDataType(isMSSQLServerDialect);
//                }
//            } else {
//                if (attributeDataType.toLowerCase().startsWith(DataTypes.CHARACTER_VARYING.getAttributeDataType(isMSSQLServerDialect))) {
//                    attributeDataType = DataTypes.CHARACTER_VARYING.getAttributeDataType(!isMSSQLServerDialect);
//                }
//            }
            attributeDataType = fixDataTypeError(attributeDataType);
            // varchar(50)||  numeric(10,5) || datetime
            if(attributeDataType.indexOf('(') > 0){
                attributeDataType = attributeDataType.substring(0, attributeDataType.indexOf('('));
            }
            DataTypes dataType = null;
            for (DataTypes type : DataTypes.values()) {
                if (attributeDataType.toLowerCase().equalsIgnoreCase(type.getAttributeDataType(isMSSQLServerDialect))) {
                    dataType = type;
                    break;
                }
            }
            if (dataType == null) {
                isMSSQLServerDialect = !isMSSQLServerDialect;
                for (DataTypes type : DataTypes.values()) {
                    if (attributeDataType.toLowerCase().equalsIgnoreCase(type.getAttributeDataType(isMSSQLServerDialect))) {
                        dataType = type;
                        break;
                    }
                }
            }
            return dataType;
        }

        public static Class getJavaDataTypeFromString(String attributeDataType, boolean isMSSQLServerDialect) {
            Class c = null;
            DataTypes dataType = fromAttributeDataType(attributeDataType, isMSSQLServerDialect);
            if (dataType != null) {
                c = dataType.getJavaClass();
            }
            return c;
        }

        public static int getSQLDataTypeFromString(String attributeDataType, boolean isMSSQLServerDialect) {
            int sqlDataType = Types.NULL;
            DataTypes dataType = fromAttributeDataType(attributeDataType, isMSSQLServerDialect);
            if (dataType != null) {
                sqlDataType = dataType.getSqlDataTypes()[0];
            }
            return sqlDataType;
        }

        public static DataTypes getDataTypeFromSQLDataType(int sqlDataType) {
            DataTypes matchedDataType = null;
            for (DataTypes dataType : DataTypes.values()) {
                for (int currentSqlDataType : dataType.getSqlDataTypes()) {
                    if (currentSqlDataType == sqlDataType) {
                        // any sql data type is matched
                        matchedDataType = dataType;
                        break;
                    }
                }
            }
            return matchedDataType;
        }

        public boolean isRealDataType() {
            boolean flag = false;
            switch (this) {
                case REAL:
                    flag = true;
                    break;
                default:
                    flag = false;
            }
            return flag;
        }

        public boolean isNumericDataType() {
            boolean flag = false;
            switch (this) {
                case NUMERIC:
                case DECIMAL:
                    flag = true;
                    break;
                default:
                    flag = false;
            }
            return flag;
        }

        public boolean isBinaryDataType() {
            boolean flag = false;
            switch (this) {
                case BINARY:
                case BYTEA:
                case IMAGE:
                    flag = true;
                    break;
                default:
                    flag = false;
            }
            return flag;
        }

        public boolean isFloatingDataType() {
            boolean flag = false;
            switch (this) {
                case DECIMAL:
                case NUMERIC:
                case REAL:
                case FLOAT:
                case DOUBLE_PRECISION:
                    flag = true;
                    break;
                default:
                    flag = false;
            }
            return flag;
        }

        public boolean isIntegerDataType() {
            boolean flag = false;
            switch (this) {
                case TINY_INT:
                case SMALL_INT:
                case INTEGER:
                case BIG_INT:
                    flag = true;
                    break;
                default:
                    flag = false;
            }
            return flag;
        }

        public boolean isDateOrTimeDataType() {
            boolean flag = false;
            switch (this) {
                case DATE:
                case TIMESTAMP_WITHOUT_TIMEZONE:
                case TIMESTAMP_WITH_TIMEZONE:
                case TIMESTAMP:
                case TIME:
                case DATETIME2:
                case SMALL_DATETIME:
                case BIG_INT:
                    flag = true;
                    break;
                default:
                    flag = false;
            }
            return flag;
        }

        public boolean isStringDataType() {
            boolean flag = false;
            switch (this) {
                case CHARACTER_VARYING:
                case CHARACTER:
                case TEXT:
                case N_CHARACTER_VARYING:
                case N_CHARACTER:
                case N_TEXT:
                    flag = true;
                    break;
                default:
                    flag = false;
            }
            return flag;
        }

        public boolean isCharacterVaryingDataType() {
            boolean flag = false;
            switch (this) {
                case CHARACTER_VARYING:
                    flag = true;
                    break;
                default:
                    flag = false;
            }
            return flag;
        }

        public boolean isStringDataTypeWithLength() {
            boolean flag = false;
            switch (this) {
                case CHARACTER_VARYING:
                case CHARACTER:
                case N_CHARACTER_VARYING:
                case N_CHARACTER:
                    flag = true;
                    break;
                default:
                    flag = false;
            }
            return flag;
        }

        public boolean isPrimaryKeyDataType() {
            boolean flag = false;
            switch (this) {
                case INTEGER:
                case BIG_INT:
                    flag = true;
                    break;
                default:
                    flag = false;
            }
            return flag;
        }

        public static List<Map<String, Object>> getSupportedDataTypes(boolean isMSSQLServerDialect) {
            if (supportedDataTypes == null) {
                synchronized (DSPDMConstants.DataTypes.class) {
                    if (supportedDataTypes == null) {
                        supportedDataTypes = new ArrayList<>(DataTypes.values().length);
                        for (DataTypes dataType : DataTypes.values()) {
                            if ((isMSSQLServerDialect) && (dataType == DSPDMConstants.DataTypes.JSONB)) {
                                continue;
                            }
                            Map<String, Object> map = new HashMap<>(15);
                            map.put(ATTRIBUTE_DATATYPE, dataType.attributeDataType);
                            map.put(ATTRIBUTE_DATATYPE_SQL_SERVER, dataType.sqlServerAttributeDataType);
                            map.put(SQL_DATATYPE, new ArrayList<>(Arrays.stream(dataType.sqlDataTypes).boxed().collect(Collectors.toList())));
                            map.put(JAVA_CLASS, dataType.javaClass.getName());
                            map.put(CONTROL_TYPE, dataType.controlType);
                            map.put(COLUMN_WIDTH_IN_EXCEL, dataType.columnWidthInExcel);
                            map.put(MAX_LENGTH, dataType.maxLength);
                            map.put(MAX_DECIMAL_LENGTH, dataType.maxDecimalLength);
                            // boolean values
                            map.put(IS_MS_SQL_SERVER_DIALECT, isMSSQLServerDialect);
                            map.put(IS_DATE_TIME_DATATYPE, dataType.isDateOrTimeDataType());
                            map.put(IS_FLOATING_DATATYPE, dataType.isFloatingDataType());
                            map.put(IS_INTEGER_DATATYPE, dataType.isIntegerDataType());
                            map.put(IS_STRING_DATATYPE, dataType.isStringDataType());
                            map.put(IS_PRIMARY_KEY_DATATYPE, dataType.isPrimaryKeyDataType());
                            map.put(IS_STRING_DATATYPE_WITH_LENGTH, dataType.isStringDataTypeWithLength());
                            // add the map to the list
                            addSupportedDataTypeIfNotAlreadyExists(map);
                        }

                    }
                }
            }
            return supportedDataTypes;
        }

        /**
         * add the given map to the list if and only if similar map does not already exist
         *
         * @param newSupportedDataType
         */
        public static void addSupportedDataTypeIfNotAlreadyExists(Map<String, Object> newSupportedDataType) {
            for (Map<String, Object> existingSupportedDataType : supportedDataTypes) {
                if ((existingSupportedDataType.get(ATTRIBUTE_DATATYPE).equals(newSupportedDataType.get(ATTRIBUTE_DATATYPE)))
                        && (existingSupportedDataType.get(ATTRIBUTE_DATATYPE_SQL_SERVER).equals(newSupportedDataType.get(ATTRIBUTE_DATATYPE_SQL_SERVER)))
                ) {
                    List<Integer> existingSqlDataType = (List<Integer>) existingSupportedDataType.get(SQL_DATATYPE);
                    List<Integer> newSqlDataType = (List<Integer>) newSupportedDataType.get(SQL_DATATYPE);
                    // merge sql data types
                    existingSqlDataType.addAll(newSqlDataType);
                    // return from method and do not add duplicated data type.
                    return;
                }
            }
            // similar does not already exist therefore add it
            supportedDataTypes.add(newSupportedDataType);
        }
    }


    public static interface ControlTypes {
        public static final String AUTO_COMPLETE = "autoComplete";
        public static final String BOOLEAN = "boolean";
        public static final String CALENDAR_WITH_DATE = "calenderWithDate";
        public static final String CALENDAR_WITH_DATETIME = "calenderWithDateTime";
        public static final String CHECKBOX = "checkbox";
        public static final String INPUT = "input";
        public static final String INPUT_INT = "inputInt";
        public static final String INPUT_NUMERIC = "inputNumeric";
        public static final String PARAGRAPH = "paragraph";
        public static final String COLOR_CODE = "Color_code";
        public static final String BINARY_IMAGE = "Binary_image";

        public static final String[] ALL_CONTROL_TYPES = {
                AUTO_COMPLETE,
                BOOLEAN,
                CALENDAR_WITH_DATE,
                CALENDAR_WITH_DATETIME,
                CHECKBOX,
                INPUT,
                INPUT_INT,
                INPUT_NUMERIC,
                PARAGRAPH,
                COLOR_CODE,
                BINARY_IMAGE};

        public static final String DEFAULT_CONTROL_TYPE = INPUT;
    }

    public static interface Units {
        public static final int MIN_UNIT_ROW_INDEX_FOR_IMPORT = 2;
        public static final int UNIT_MAX_LENGTH = 60;
        public static final String UNITS = "dspdmUnits";
        public static final String UNIT = "unit";
        public static final String UNITPOLICY_SYSTEM = "system";
        public static final String UNITPOLICY_USER = "user";
        public static final String UNITPOLICY_CUSTOM = "custom";
        public static final String USER_CURRENT_UNIT_TEMPLATES = "USER CURRENT UNIT TEMPLATES";
        public static final String UNIT_TEMPLATES = "UNIT TEMPLATES";
        public static final String UNIT_TEMPLATE_DETAILS = "UNIT TEMPLATE DETAILS";
        public static final String USER_UNIT_SETTINGS = "USER UNIT SETTINGS";
        public static final String CANCONVERSIONUNITS = "canConversionUnits";
        public static final String USER_NAME = "USER_NAME";
        public static final String UNIT_TEMPLATE_ID = "UNIT_TEMPLATE_ID";
        public static final String UNIT_TEMPLATE_NAME = "UNIT_TEMPLATE_NAME";
        public static final String IS_DEFAULT = "IS_DEFAULT";
        public static final String USER_UNIT_SETTING_ID = "USER_UNIT_SETTING_ID";
        public static final String USER_UNIT = "USER_UNIT";
        public static final String UNIT_TEMPLATE_DETAIL_ID = "UNIT_TEMPLATE_DETAIL_ID";
        public static final String UNITROW = "unitRow";
        public static final String USER_CURRENT_UNIT_TEMPLATE_ID = "USER_CURRENT_UNIT_ID";
        public static final String PDMBASEUNITTYPENAMES = "baseUnitTypeNames";
        public static final String PDMBASEUNITTYPENAME = "baseUnitTypeName";

    }

    public static interface FileExtentions {
        public static final String XLSX = "xlsx";
    }

    public static interface HTTP_STATUS_CODES {
        public static final int OK = 200;
        public static final int UN_AUTH = 401;
        public static final int BAD_REQUEST = 400;
        public static final int NOT_FOUND = 404;
        public static final int INTERNAL_SERVER_ERROR = 500;
        public static final int FORBIDDEN = 403;
    }

    public static interface API_Operation{
    	public static final String DELETE="delete";
    	public static final String SAVE="save";
    	public static final String IMPORT="import";
    }

    public static enum BusinessObjectOperations{

        SAVE_OPR(1,"save","saved successfully", true),
        UPDATE_OPR(2,"update","updated successfully", true),
        SAVE_AND_UPDATE_OPR(3,"save_and_update","updated and saved successfully", true),
        DELETE_OPR(4,"delete","deleted successfully", true),
        SIMPLE_IMPORT_OPR(5,"simple_import","simple imported successfully", true),
        BULK_IMPORT_OPR(6,"bulk_import","bulk imported successfully", true),
        INTRODUCE_BUSINESS_OBJECT_OPR(7,"introduce_business_objects","business objects(s) introduced successfully", false),
        DROP_BUSINESS_OBJECT_OPR(8,"drop_business_objects","business objects(s) dropped successfully", false),
        ADD_BUS_OBJ_RELATIONSHIPS_OPR(9,"add_bus_obj_relationships","business object relationship(s) added successfully", false),
        DROP_BUS_OBJ_RELATIONSHIPS_OPR(10,"drop_bus_obj_relationships","business object relationship(s) dropped successfully", false),
        ADD_UNIQUE_CONSTRAINTS_OPR(11,"add_unique_constraints","unique constraint(s) added successfully", false),
        DROP_UNIQUE_CONSTRAINTS_OPR(12,"drop_unique_constraints","unique constraint(s) dropped successfully", false),
        GENERATE_METADATA_OPR(13,"generate_metadata","metadata generated successfully", true),
        DELETE_METADATA_OPR(14,"delete_metadata","metadata deleted successfully", true),
        ADD_CUSTOM_ATTR_OPR(15,"add_custom_attributes","custom attributes(s) added successfully", false),
        DROP_CUSTOM_ATTR_OPR(16,"drop_custom_attributes","custom attributes(s) dropped successfully", false),
        ADD_BUS_OBJ_GROUP(17,"add_bus_obj_group","business object group added successfully", false),
        UPDATE_CUSTOM_ATTR_OPR(18,"update_custom_attributes","custom attributes(s) updated successfully", false),
        GENERATE_METADATA_FOR_ALL_OPR(19,"generate_metadata_for_all","metadata generated for all successfully", false),
        DELETE_METADATA_FOR_ALL_OPR(20,"delete_metadata_for_all","metadata deleted for all successfully", true),
        ADD_SEARCH_INDEXES_OPR(21,"add_search_indexes","search index(es) added successfully", false),
        DROP_SEARCH_INDEXES_OPR(22,"drop_search_indexes","search index(es) dropped successfully", false),
        DDL_CREATE_TABLE_OPR(23,"ddl_create_table_opr","physical table created successfully", false),
        DDL_DROP_TABLE_OPR(24,"ddl_drop_table_opr","physical table dropped successfully ", false),
        DDL_ADD_COLUMN_OPR(25,"ddl_add_column_opr","physical column(s) added successfully", false),
        DDL_DROP_COLUMN_OPR(26,"ddl_drop_column_opr","physical column(s) dropped successfully", false),
        DDL_ADD_RELATIONSHIP_OPR(27,"ddl_add_relationship_opr","physical relationship(s) added successfully", false),
        DDL_DROP_RELATIONSHIP_OPR(28,"ddl_drop_relationship_opr","physical relationship(s) dropped successfully", false),
        DDL_ADD_UNIQUE_CONSTRAINT_OPR(29,"ddl_add_unique_constraint_opr","physical unique constraint(s) added successfully", false),
        DDL_DROP_UNIQUE_CONSTRAINT_OPR(30,"ddl_drop_unique_constraint_opr","physical unique constraint(s) dropped successfully", false),
        DDL_ADD_SEARCH_INDEX_OPR(31,"ddl_add_search_indexes_opr","physical search index(es) added successfully", false),
        DDL_DROP_SEARCH_INDEX_OPR(32,"ddl_drop_search_indexes_opr","physical search index(es) dropped successfully", false),
        DDL_CREATE_SEQUENCE_OPR(33,"ddl_create_sequence_opr","physical sequence created successfully", false),
        DDL_DROP_SEQUENCE_OPR(34,"ddl_drop_sequence_opr","physical sequence dropped successfully", false),
        DDL_UPDATE_COLUMN_OPR(35,"ddl_update_column_opr","physical column updated successfully", false),
        DDL_DROP_DEFAULT_CONSTRAINT_OPR(36,"ddl_drop_default_constraint_opr","physical default constraint(s) dropped successfully", false);

        BusinessObjectOperations(Integer id, String oprShortName, String oprName, Boolean canUndoFlag){
            this.id = id;
            this.operationShortName = oprShortName;
            this.operationName = oprName;
            this.canUndoFlag = canUndoFlag;
        }
        private Integer id = null;
        private String operationShortName = null;
        private String operationName = null;
        private Boolean canUndoFlag = false;
        public Integer getId() {
            return id;
        }
        public String getOperationShortName() {
            return operationShortName;
        }
        public String getOperationName() {
            return operationName;
        }
        public Boolean getCanUndoFlag() { return canUndoFlag; }

        public static BusinessObjectOperations getOperationById(Integer opertaionId) {
            BusinessObjectOperations operation = null;
            if (opertaionId != null) {
                for (BusinessObjectOperations opr : BusinessObjectOperations.values()) {
                    if (opr.id.intValue() == opertaionId.intValue()) {
                        operation = opr;
                        break;
                    }
                }
            }
            return operation;
        }
    }

    public static interface UpdateCustomAttributeFlags{
        public static final String IS_ATTR_DEFAULT_UPDATE_REQUESTED = "isAttributeDefaultUpdateRequested";
        public static final String IS_ATTR_DATATYPE_UPDATE_REQUESTED = "isAttributeDataTypeUpdateRequested";
    }

    public static interface SpecialCharactersSearchAPI{
        public static final String ASTERISK = "*";
        public static final String ROUND_BRACKET_OPEN = "(";
        public static final String ROUND_BRACKET_CLOSE = ")";
        public static final String SQUARE_BRACKET_OPEN = "[";
        public static final String SQUARE_BRACKET_CLOSE = "]";
        public static final String QUESTION_MARK = "?";
        public static final String PLUS = "+";
        public static final String UNDERSCORE = "_";
        public static final String HYPHEN = "-";
        public static final String DOUBLE_QUOTES = "\"";
        public static final String SINGLE_QUOTES = "'";
    }
}
