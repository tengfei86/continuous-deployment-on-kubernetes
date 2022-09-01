package com.lgc.dspdm.msp.mainservice.model;

import com.lgc.dspdm.core.common.data.criteria.CriteriaFilter;
import com.lgc.dspdm.core.common.data.criteria.Operator;
import com.lgc.dspdm.core.common.data.dto.dynamic.DynamicDTO;

import java.util.ArrayList;
import java.util.List;

/**
 * This class attribute maps the Hierarchy table data in the database.
 * Hierarchy table data will be converted to HierarchyStruct for use.
 *
 * @author Bo Liang
 * @createDate: 2020/06/08
 * @description: Hierarchy table data in this class mapping database
 */
public class HierarchyStruct {
    // hierarchyName in the hierarchy table
    private String hierarchyName;
    // The levelIndex name in the hierarchy table, Mapping field name: level_Index
    private String hierarchyIndexName;
    private String boName;
    private String levelType;
    private String columnID;
    private String columnName;
    private String rootValue;
    private String relationColumnID;
    private String relationColumnName;
    private String parentColumnName;
    private String parentColumnID;

    //Map field value: level_Index
    private int levelIndex = 0;
    /*
    Level data is shown/hidden. true is hidden, false is displayed.
    For example: Hierarchy [FIELD-COMPLETION] contains level [FIELD; WELL; WELLBORE; WELL COMPLETION];
    When the incoming level is [FIELD; WELL COMPLETION], then level [FIELD; WELL COMPLETION] data display, level [WELL; WELLBORE] data hiding
     */
    private boolean hierarchyHide = false;
    // Map filter conditions in BOQuery
    private List<com.lgc.dspdm.core.common.data.criteria.CriteriaFilter> filters;
    // parentLevelType, used to recursively combine tree structures.
    private String parentLevelType;
    // The corresponding value of the temporary ParentColumnID is used to recursively combine the tree structure.
    private String tempParentColumnID;

    public void setLevelIndex(int levelIndex) {
        this.levelIndex = levelIndex;
    }

    public int getLevelIndex() {
        return levelIndex;
    }

    public void setBoName(String boName) {
        this.boName = boName;
    }

    public String getBoName() {
        return boName;
    }

    public void setLevelType(String levelType) {
        this.levelType = levelType;
    }

    public String getLevelType() {
        return levelType;
    }

    public void setColumnID(String columnID) {
        this.columnID = columnID;
    }

    public String getColumnID() {
        return columnID;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setRootValue(String rootValue) {
        this.rootValue = rootValue;
    }

    public String getRootValue() {
        return rootValue;
    }

    public void setRelationColumnID(String relationColumnID) {
        this.relationColumnID = relationColumnID;
    }

    public String getRelationColumnID() {
        return relationColumnID;
    }

    public void setRelationColumnName(String relationColumnName) {
        this.relationColumnName = relationColumnName;
    }

    public String getRelationColumnName() {
        return relationColumnName;
    }

    public void setParentColumnID(String parentColumnID) {
        this.parentColumnID = parentColumnID;
    }

    public String getParentColumnID() {
        return parentColumnID;
    }

    public void setParentColumnName(String parentColumnName) {
        this.parentColumnName = parentColumnName;
    }

    public String getParentColumnName() {
        return parentColumnName;
    }

    public void setParentLevelType(String parentLevelType) {
        this.parentLevelType = parentLevelType;
    }

    public String getParentLevelType() {
        return parentLevelType;
    }

    public void setHierarchyHide(boolean hierarchyHide) {
        this.hierarchyHide = hierarchyHide;
    }

    public boolean getHierarchyHide() {
        return hierarchyHide;
    }

    public List<com.lgc.dspdm.core.common.data.criteria.CriteriaFilter> getFilters() {
        return filters;
    }

    public void setFilter(String columnName, Operator operator, Object... values) {
        if (filters == null) {
            filters = new ArrayList<>();
        }
        filters.add(new CriteriaFilter(columnName, operator, values));
    }

    public void setTempParentColumnID(String tempParentColumnID) {
        this.tempParentColumnID = tempParentColumnID;
    }

    public String getTempParentColumnID() {
        return tempParentColumnID;
    }
    public void setHierarchyName(String hierarchyName) {
        this.hierarchyName = hierarchyName;
    }

    public String getHierarchyName() {
        return hierarchyName;
    }
    public void setHierarchyIndexName(String hierarchyIndexName) {
        this.hierarchyIndexName = hierarchyIndexName;
    }

    public String getHierarchyIndexName() {
        return hierarchyIndexName;
    }


    public void setHierarchyColumnName(List<DynamicDTO> dataList)
    {
        if (dataList != null) {
            for (DynamicDTO dynamicDTO : dataList) {
                switch (dynamicDTO.get("attribute").toString())
                {
                    case "BO_NAME":
                        boName="BO_NAME";
                        break;
                    case "COLUMN_ID":
                        columnID="COLUMN_ID";
                        break;
                    case "COLUMN_NAME":
                        columnName="COLUMN_NAME";
                        break;
                    case "HIERARCHY_NAME":
                        hierarchyName="HIERARCHY_NAME";
                        break;
                    case "LEVEL_INDEX":
                        hierarchyIndexName="LEVEL_INDEX";
                        break;
                    case "LEVEL_TYPE":
                        levelType="LEVEL_TYPE";
                        break;
                    case "PARENT_COLUMN_ID":
                        parentColumnID="PARENT_COLUMN_ID";
                        break;
                    case "PARENT_COLUMN_NAME":
                        parentColumnName="PARENT_COLUMN_NAME";
                        break;
                    case "RELATION_COLUMN_ID":
                        relationColumnID="RELATION_COLUMN_ID";
                        break;
                    case "RELATION_COLUMN_NAME":
                        relationColumnName="RELATION_COLUMN_NAME";
                        break;
                    case "ROOT_VALUE":
                        rootValue="ROOT_VALUE";
                        break;
                }
            }
        }
    }
}
