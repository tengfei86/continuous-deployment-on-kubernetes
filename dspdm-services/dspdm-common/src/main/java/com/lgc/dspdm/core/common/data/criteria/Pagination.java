package com.lgc.dspdm.core.common.data.criteria;

import com.lgc.dspdm.core.common.config.ConfigProperties;
import com.lgc.dspdm.core.common.exception.DSPDMException;
import com.lgc.dspdm.core.common.util.CollectionUtils;
import com.lgc.dspdm.core.common.util.ExecutionContext;

import java.io.Serializable;
import java.util.Set;
import java.util.TreeSet;

public class Pagination implements Serializable, Cloneable {

    private int recordsPerPage = ConfigProperties.getInstance().default_max_records_to_read.getIntegerValue();
    private Set<Integer> pagesToRead = null;
    private boolean defaultPagination = true;

    public Pagination() {
    }

    public Pagination clone(ExecutionContext executionContext) {
        Pagination pagination = null;
        try {
            pagination = (Pagination) this.clone();
            if (this.pagesToRead != null) {
                pagination.pagesToRead = new TreeSet<Integer>(this.pagesToRead);
            }
        } catch (CloneNotSupportedException e) {
            DSPDMException.throwException(e, executionContext);
        }
        return pagination;
    }

    public int getRecordsPerPage() {
        return recordsPerPage;
    }

    public Pagination setRecordsPerPage(int recordsPerPage, ExecutionContext executionContext) {
        if (recordsPerPage <= 0) {
            throw new DSPDMException("Invalid value for records per page '{}' for pagination", executionContext.getExecutorLocale(), recordsPerPage);
        }
        this.recordsPerPage = recordsPerPage;
        defaultPagination = false;
        return this;
    }

    public Set<Integer> getPagesToRead() {
        return pagesToRead;
    }


    public Pagination addPagesToRead(ExecutionContext executionContext, Integer... pageNumbersToRead) {
        if (pagesToRead == null) {
            // must be sorted/tree set so that page number are added in ascending order
            pagesToRead = new TreeSet<Integer>();
        }
        for (Integer pageNumber : pageNumbersToRead) {
            if ((pageNumber == null) || (pageNumber <= 0)) {
                throw new DSPDMException("Wrong page number '{}' for pagination", executionContext.getExecutorLocale(), pageNumber);
            }
            pagesToRead.add(pageNumber);
        }
        defaultPagination = false;
        return this;
    }

    public Pagination setPagesToRead(ExecutionContext executionContext, Integer... pageNumbersToRead) {
        pagesToRead = new TreeSet<Integer>();
        for (Integer pageNumber : pageNumbersToRead) {
            if ((pageNumber == null) || (pageNumber <= 0)) {
                throw new DSPDMException("Wrong page number '{}' for pagination", executionContext.getExecutorLocale(), pageNumber);
            }
            pagesToRead.add(pageNumber);
        }
        defaultPagination = false;
        return this;
    }

    public int getReadFromIndex() {
        int readFromIndex = 0;
        if ((CollectionUtils.hasValue(pagesToRead)) && (!(pagesToRead.contains(1)))) {
            // get first page number from set
            readFromIndex = (pagesToRead.iterator().next().intValue() - 1) * recordsPerPage;
            if (readFromIndex < 0) {
                readFromIndex = 0;
            }
        }
        return readFromIndex;
    }

    public int getRecordsToRead() {
        int recordsToRead = recordsPerPage;
        if (CollectionUtils.hasValue(pagesToRead)) {
            // get first page number from set
            int firstPageNumber = pagesToRead.iterator().next();
            int lastPageNumber = 1;
            for (Integer pageNumber : pagesToRead) {
                lastPageNumber = pageNumber;
            }
            recordsToRead = ((lastPageNumber - firstPageNumber + 1) * recordsPerPage);
            if (recordsToRead <= 0) {
                recordsToRead = 1;
            }
        }
        return recordsToRead;
    }

    public int getOffset() {
        return getReadFromIndex() - 1;
    }

    public boolean isDefaultPagination() {
        return defaultPagination;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Pagination{");
        sb.append("recordsPerPage=").append(recordsPerPage);
        sb.append(", pagesToRead=").append((pagesToRead != null) ? CollectionUtils.getCommaSeparated(pagesToRead) : pagesToRead);
        sb.append('}');
        return sb.toString();
    }
}
