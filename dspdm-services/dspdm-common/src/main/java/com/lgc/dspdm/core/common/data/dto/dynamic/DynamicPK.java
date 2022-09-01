package com.lgc.dspdm.core.common.data.dto.dynamic;

import java.util.Arrays;
import java.util.Objects;

public class DynamicPK {
    private Object[] pk = null;
    private String boName = null;

    public DynamicPK(String boName, Object id) {
        this.boName = boName;
        this.pk = new Object[]{id};
    }

    public DynamicPK(String boName, Object[] id) {
        this.boName = boName;
        this.pk = id;
    }

    public String getBoName() {
        return boName;
    }

    public Object[] getPK() {
        return pk;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DynamicPK)) {
            return false;
        }
        DynamicPK dynamicPK = (DynamicPK) o;

        return Objects.equals(boName, dynamicPK.boName) && Arrays.equals(pk, dynamicPK.pk);
    }

    @Override
    public int hashCode() {
        return Objects.hash(boName, Arrays.hashCode(pk));
    }

    @Override
    public String toString() {
        String str = null;
        if ((this.pk == null) || (this.pk.length == 0)) {
            str = null;
        } else if ((this.pk.length == 1) && (this.pk[0] != null)) {
            str = pk[0].toString();
        } else {
            str = Arrays.toString(pk);
        }
        return str;
    }
}
