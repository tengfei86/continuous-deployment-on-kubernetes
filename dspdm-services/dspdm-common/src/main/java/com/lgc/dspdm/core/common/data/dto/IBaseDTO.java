package com.lgc.dspdm.core.common.data.dto;

import java.io.Serializable;

public interface IBaseDTO<PK> extends Serializable {
    public PK getId();
}
