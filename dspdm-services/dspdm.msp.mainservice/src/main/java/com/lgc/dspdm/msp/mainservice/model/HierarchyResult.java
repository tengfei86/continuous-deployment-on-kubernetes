package com.lgc.dspdm.msp.mainservice.model;

import java.util.ArrayList;
import java.util.List;

/**
 * This class indicates that Hierarchy returns the result object (infinite recursive value)
 * 1. Convert Hierarchy table data to HierarchyResult and return
 * 2. Convert BOQuery table data to HierarchyResult and return
 *
 * @author Bo Liang
 * @createDate: 2020/06/08
 * @description: This class represents the Hierarchy structure
 */
public class HierarchyResult {
    private String levelType;
    private String levelID;
    private String levelName;
    private String parentLevelType;
    private String parentLevelID;
    private List<HierarchyResult> childHierarchy;
    private String boName;

    public void setLevelType(String levelType) {
        this.levelType = levelType;
    }

    public String getLevelType() {
        return levelType;
    }

    public void setParentLevelType(String parentLevelType) {
        this.parentLevelType = parentLevelType;
    }

    public String getParentLevelType() {
        return parentLevelType;
    }

    public void setLevelID(String levelID) {
        this.levelID = levelID;
    }

    public String getLevelID() {
        return levelID;
    }

    public void setLevelName(String levelName) {
        this.levelName = levelName;
    }

    public String getLevelName() {
        return levelName;
    }

    public void setParentLevelID(String parentLevelID) {
        this.parentLevelID = parentLevelID;
    }

    public String getParentLevelID() {
        return parentLevelID;
    }

    public void setChildHierarchy(HierarchyResult childHierarchy) {
        if (this.childHierarchy == null) {
            this.childHierarchy = new ArrayList<>();
        }
        this.childHierarchy.add(childHierarchy);
    }

    public List<HierarchyResult> getChildHierarchy() {
        return childHierarchy;
    }

    public void setBoName(String boName) {
        this.boName = boName;
    }

    public String getBoName() {
        return boName;
    }
}
