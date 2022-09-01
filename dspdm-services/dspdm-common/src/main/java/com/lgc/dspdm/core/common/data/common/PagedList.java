package com.lgc.dspdm.core.common.data.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PagedList<T> extends ArrayList<T> {
    public static final String PAGED_LIST_KEY = "pagedList";
    public static final int FIRST_PAGE_NUMBER = 1;
    private int totalRecords = 0;
    
    public PagedList(int totalRecords, Collection<T> collection) {
        super(collection);
        this.totalRecords = totalRecords;
    }
    
    public int getTotalRecords() {
        return totalRecords;
    }
}
