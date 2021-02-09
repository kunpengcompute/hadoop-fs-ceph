/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 *
 * Description: Exception for librgw native call.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.cephrgw;

import java.io.IOException;

/**
 * User-defined librgw exception class, which inherits the IOException class.
 */
public class CephRgwException extends IOException {
    private static final long serialVersionUID = 1L;
    private int errcode;

    public CephRgwException(final String msg) {
        super(String.format(" errMsg:%s", msg));
    }

    public CephRgwException(final int errcode, final String msg) {
        super(String.format("errcode:%d, errMsg:%s", errcode, msg));
        this.errcode = errcode;
    }

    public int getErrcode() {
        return errcode;
    }
}