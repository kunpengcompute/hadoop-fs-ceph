/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 *
 * Description: File handle class for librgw.
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

import org.apache.hadoop.fs.FileStatus;

import java.io.Closeable;
import java.io.IOException;

/**
 * File handle class for librgw.
 */
class LibRGWFH implements Closeable {
    private final CephRgwFileSystem fileSystem;
    private long fhPtrLong;
    private FileStatus fileStatus;
    private int refNum = 1;
    private boolean cache;

    LibRGWFH(CephRgwFileSystem fileSystem, long fhPtr, FileStatus fileStatus, boolean cache) {
        this.fileSystem = fileSystem;
        this.fhPtrLong = fhPtr;
        this.fileStatus = fileStatus;
        this.cache = cache;
    }

    @Override
    public synchronized void close() {
        if (this == fileSystem.getRootFH()) {
            return;
        }
        if (!cache) {
            doClose();
            return;
        }
        refNum--;
        if (refNum == 0) {
            synchronized (CephRgwFileSystem.FH_CACHE_MAP) {
                if (!CephRgwFileSystem.FH_CACHE_MAP.containsKey(fileStatus.getPath().toString())) {
                    doClose();
                }
            }
        }

    }

    synchronized void ref() {
        refNum++;
    }

    void doClose() {
        fileSystem.rgwClose(fileSystem.getRgwFsPtr(), fhPtrLong);
        fhPtrLong = 0;
    }

    FileStatus getFileStatus() throws IOException {
        return fileStatus;
    }

    long getFhPtr() throws IOException {
        checkClosed();
        return fhPtrLong;
    }

    private void checkClosed() throws IOException {
        if (fhPtrLong == 0) {
            throw new IOException("File handler is closed.");
        }
    }
}
