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
 * librgw句柄对应的java类
 */
class LibRGWFH implements Closeable {
    private transient final CephRgwFileSystem fileSystem;
    private transient long fhPtrLong;
    private transient final FileStatus fileStatus;
    private transient int refNum = 1;
    private transient final boolean cache;

    LibRGWFH(CephRgwFileSystem fileSystem, long fhPtr, FileStatus fileStatus, boolean cache) {
        this.fileSystem = fileSystem;
        this.fhPtrLong = fhPtr;
        this.fileStatus = fileStatus;
        this.cache = cache;
    }

    // Closing a file handle.
    @Override
    public void close() {
        // The handle of the file root directory cannot be directly closed here.
        if (this != fileSystem.getRootFH()) {
            if (!cache) {
                doClose();
                return;
            }
            /* When the cache is enabled, each thread may share a handle.
             * Therefore, the number of references should be set to -1.
             */
            synchronized (this) {
                refNum--;
            }
            // Close the handle completely when the number of references decreases to 0.
            if (refNum == 0) {
                synchronized (CephRgwFileSystem.FH_CACHE_MAP) {
                    if (!CephRgwFileSystem.FH_CACHE_MAP.containsKey(fileStatus.getPath().toString())) {
                        doClose();
                    }
                }
            }
        }
    }

    // If a thread references this handle, the referrer increases by 1.
    void ref() {
        synchronized (this) {
            refNum++;
        }
    }

    // Operation for closing a handle
    void doClose() {
        fileSystem.rgwClose(fileSystem.getRgwFsPtr(), fhPtrLong);
        fhPtrLong = 0;
    }

    // Obtains the FileStatus information corresponding to the handle.
    FileStatus getFileStatus() {
        return fileStatus;
    }

    // Obtains the C language pointer of the handle.
    long getFhPtr() throws IOException {
        checkClosed();
        return fhPtrLong;
    }

    // Check whether the handle is closed.
    private void checkClosed() throws IOException {
        if (fhPtrLong == 0) {
            throw new IOException("File handler is closed.");
        }
    }
}
