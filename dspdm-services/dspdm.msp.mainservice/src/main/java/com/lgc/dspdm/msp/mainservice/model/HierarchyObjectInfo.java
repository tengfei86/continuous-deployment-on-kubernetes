package com.lgc.dspdm.msp.mainservice.model;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is the Hierarchy information class.
 * The main functions are:
 * 1. Hierarchy name and level information included
 * 2. The serial number value of the input BOQuery information (maximum and minimum)
 *    Use it when converting BOQuery data to HierarchyResult.
 *
 * @author Bo Liang
 * @createDate: 2020/06/08
 * @description:
 */
public class HierarchyObjectInfo {
    private String hierarchyName;
    private List<HierarchyStruct> childHierarchy;

    //BOQuery mapping Level minimum number
    private int levelMinIndex = 0;
    //BOQuery mapping Level maximum number
    private int levelMaxIndex = 0;

    public void setHierarchyName(String hierarchyName) {
        this.hierarchyName = hierarchyName;
    }

    public String getHierarchyName() {
        return hierarchyName;
    }

    public void setChildHierarchy(HierarchyStruct hierarchyStruct) {
        if (this.childHierarchy == null) {
            this.childHierarchy = new ArrayList<>();
        }
        this.childHierarchy.add(hierarchyStruct);
    }

    public List<HierarchyStruct> getChildHierarchy() {
        return childHierarchy;
    }

    public void setLevelMinIndex(int levelMinIndex) {
        this.levelMinIndex = levelMinIndex;
    }

    public int getLevelMinIndex() {
        return levelMinIndex;
    }

    public void setLevelMaxIndex(int levelMaxIndex) {
        this.levelMaxIndex = levelMaxIndex;
    }

    public int getLevelMaxIndex() {
        return levelMaxIndex;
    }
}
