/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
package org.apache.hadoop.fs.cephrgw;

import java.util.Locale;

/**
 * cephrgw exception implementation
 */
public class CephRgwException extends Exception {
    private static final long serialVersionUID = 1L;
    private int errcode;

    public CephRgwException(int errcode, String msg) {
        super(String.format(Locale.ROOT,"errcode:%d, errMsg:%s", errcode, msg));
        this.errcode = errcode;
    }

    public int getErrcode() {
        return errcode;
    }
}