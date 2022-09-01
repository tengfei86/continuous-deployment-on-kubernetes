package com.lgc.dspdm.core.common.config;

public interface BaseProperty {

    public String getPropertyKey();
    public String getPropertyValue();
    public String getDefaultValue();
    public void setPropertyValue(String value);
    public boolean getBooleanValue();
    public int getIntegerValue();
    public float getFloatValue();
}