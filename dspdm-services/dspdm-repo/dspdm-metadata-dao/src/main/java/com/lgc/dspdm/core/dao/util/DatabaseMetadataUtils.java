package com.lgc.dspdm.core.dao.util;

import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.DSPDMConstants;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

public class DatabaseMetadataUtils {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Connection connection = null;
        try {
            //Registering the Driver
           Class.forName("org.postgresql.Driver");
            String dburl = "jdbc:postgresql://localhost:5432/dspdm_data_model_qa";
            String u = "postgres";
            String p = "postgres";

           /* Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            String dburl = "jdbc:sqlserver://localhost:1433;databaseName=dspdm_data_model_dev";
            String u = "sa";
            String p = "Landmark1";*/


            connection = DriverManager.getConnection(dburl, u, p);
            System.out.println("Connection established......");
            //Retrieving the meta data object
            DatabaseMetaData metaData = connection.getMetaData();
            //Retrieving the columns in the database
            String tableName = "well_test_1";
            // Printing the column name and size
            printAllColumns(metaData, tableName);
            // Printing the primary keys
           // printAllPrimaryKeys(metaData, tableName);

            String parentTableName = null;
            String childTableName = "well_test_pump";
            // Printing the column name and size
            //printAllRelationships(metaData, parentTableName, childTableName);
            // Printing the column unique indexes
            //printAllUniqueIndexes(metaData, "alarm");
            // Printing the sequence names
            //printAllSequences(metaData);
            // Printing the table types
            //printAllTableTypes(metaData);
            // Printing all tables
            //printAllTableNames(metaData);
            // Taking All entity Names in a list
            //List<String> entityNamesList = allTableNamesInAList(metaData);
            // Printing relationship for PostgreSQL
            //List<ParentChildRelationshipModel> relationshipPostgres = readAllRelationshipsForPostgres(metaData);
            // Printing relationship for MSSQL Server
            //List<ParentChildRelationshipModel> relationshipMSSQL = readAllRelationshipsForMSSQL(metaData, entityNamesList);

        } catch (Throwable throwables) {
            throwables.printStackTrace();
        } finally {
            if ((connection != null) && (!connection.isClosed())) {
                connection.close();
                System.out.println("Connection closed......");
            }
        }
    }

    private static void printAllColumns(DatabaseMetaData databaseMetaData, String tableName) throws SQLException {
        //Retrieving the columns in the database
        System.out.println("Going to print all columns.......");
        try (ResultSet resultSet = databaseMetaData.getColumns(null, null, tableName, null);) {
            //Printing the column name and size
            printResultSet(resultSet);
        }
    }

    private static void printAllPrimaryKeys(DatabaseMetaData databaseMetaData, String tableName) throws SQLException {
        //Retrieving the columns in the database
        System.out.println("Going to print all pk.......");
        try (ResultSet resultSet = databaseMetaData.getPrimaryKeys(null, null, tableName);) {
            //Printing the column name and size
            printResultSet(resultSet);
        }
    }

    private static void printAllRelationships(DatabaseMetaData databaseMetaData, String parentTableName, String childTableName) throws SQLException {
        //Retrieving the relationships in the database
        System.out.println("Going to print all relationship.......");
        try (ResultSet resultSet = databaseMetaData.getCrossReference(
                null, null, parentTableName,
                null, null, childTableName);) {
            //Printing the column name and size
            printResultSet(resultSet);
        }
    }

    private static void printAllUniqueIndexes(DatabaseMetaData databaseMetaData, String tableName) throws SQLException {
        //Retrieving the columns in the database
        System.out.println("Going to print all unique indices.......");
        try (ResultSet resultSet = databaseMetaData.getIndexInfo(null, null, tableName, true, false);) {
            //Printing the column name and size
            printResultSet(resultSet);
        }
    }

    private static void printAllSearchIndexes(DatabaseMetaData databaseMetaData, String tableName) throws SQLException {
        //Retrieving the columns in the database
        System.out.println("Going to print all search indexes.......");
        try (ResultSet resultSet = databaseMetaData.getIndexInfo(null, null, tableName, false, false);) {
            //Printing the column name and size
            printResultSet(resultSet);
        }
    }

    private static void printAllSequences(DatabaseMetaData databaseMetaData) throws SQLException {
        //Retrieving the columns in the database. This only works for postgres. for sql, we need to query db like belwo
        // SELECT name from sys.sequences
        System.out.println("Going to print all sequence names.......");
        try (ResultSet resultSet = databaseMetaData.getTables(null, null, "seq_well",  new String[] { "SEQUENCE" });) {
            //Printing the column name and size
            printResultSet(resultSet);
        }
    }

    private static void printAllTableTypes(DatabaseMetaData databaseMetaData) throws SQLException {
        //Retrieving the columns in the database
        System.out.println("Going to print all table types.......");
        try (ResultSet resultSet = databaseMetaData.getTableTypes();) {
            //Printing the column name and size
            printResultSet(resultSet);
        }
    }

    private static void printAllTableNames(DatabaseMetaData databaseMetaData) throws SQLException {
        //Retrieving the columns in the database
        System.out.println("Going to print all table names.......");
        try (ResultSet resultSet = databaseMetaData.getTables(null,"dbo",null,new String[]{"TABLE","VIEW"})) {
            //Printing the column name and size
            printResultSet(resultSet);
        }
    }

    private static void printResultSet(ResultSet resultSet) throws SQLException {
        //tableNamesList(resultSet);
        //Printing the column name and value
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        String columnName = null;
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                columnName = resultSetMetaData.getColumnName(i);
                System.out.print(columnName + " : " + resultSet.getObject(columnName));
                System.out.print(",\t\t\t");
            }
            System.out.println(", ");
        }

    }
    private static List<String> allTableNamesInAList(DatabaseMetaData databaseMetaData){
        List<String> entityNamesList = new ArrayList<>();
        try (ResultSet resultSet = databaseMetaData.getTables(null,"dbo",null, new String[] {"TABLE","VIEW"})) {
            while(resultSet.next()){
                String tableName = (String) resultSet.getObject("TABLE_NAME");
                entityNamesList.add(tableName);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return  entityNamesList;
    }

    private static List<ParentChildRelationshipModel> readAllRelationshipsForPostgres(DatabaseMetaData databaseMetaData){
        List<ParentChildRelationshipModel> parentChildRelationshipList = new ArrayList<>();
        try (ResultSet resultSet = databaseMetaData.getCrossReference(null, null, null,
                null, null, null);) {
            while(resultSet.next()){
                String parentTableName = (String) resultSet.getObject("pktable_name");
                String childTableName = (String) resultSet.getObject("fktable_name");
                ParentChildRelationshipModel model = new ParentChildRelationshipModel();
                model.setParent(parentTableName);
                model.setChild(childTableName);
                parentChildRelationshipList.add(model);

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return parentChildRelationshipList;
    }

    private static List<ParentChildRelationshipModel> readAllRelationshipsForMSSQL(DatabaseMetaData databaseMetaData, List<String> entityNamesList){
        List<ParentChildRelationshipModel> parentChildRelationshipList = new ArrayList<>();
        for(String entity: entityNamesList){
            try (ResultSet resultSet = databaseMetaData.getCrossReference(null, null, entity,
                    null, null, null);) {
                while(resultSet.next()){
                    String parentTableName = (String) resultSet.getObject("pktable_name");
                    String childTableName = (String) resultSet.getObject("fktable_name");
                    ParentChildRelationshipModel model = new ParentChildRelationshipModel();
                    model.setParent(parentTableName);
                    model.setChild(childTableName);
                    parentChildRelationshipList.add(model);

                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return parentChildRelationshipList;
    }

    private static class ParentChildRelationshipModel{
        private String parent;
        private String child;

        public String getParent() {
            return parent;
        }

        public void setParent(String parent) {
            this.parent = parent;
        }

        public String getChild() {
            return child;
        }

        public void setChild(String child) {
            this.child = child;
        }
    }
}
