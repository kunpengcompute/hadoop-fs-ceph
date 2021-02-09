/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 *
 * Description: FileHandlerReceiver is interface for handling struct stat.
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;

/**
 * Callback abstract class provided for the readdir and getattr functions of the librgw to receive the queried file information.
 */
public abstract class AbstractFileHandlerReceiver {
    private transient final CephRgwFileSystem fileSystem;

    AbstractFileHandlerReceiver(CephRgwFileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    /**
     * Callback for receiving file information invoked by the c file.
     */
    void receiveFileHandler(String name, long statPtr, int mask) throws IOException {
        Path currPath = null;
        /**
         *The name function is used to report the queried file name to the Java function.
         * If the query result does not contain the file name, null is returned.
         */
        if (name != null && name.length() > 0) {
            currPath = new Path(name);
        }
        final int mode = fileSystem.getMode(statPtr);
        receiveFileHandler(name, statPtr, mask, new FileStatus(fileSystem.getLength(statPtr), fileSystem.isDir(mode),
                0, fileSystem.getVirtualBlockSize(), fileSystem.getModifyTime(statPtr),
                fileSystem.getAccessTime(statPtr), new FsPermission(mode), CephRgwFileSystem.CONST_USER,
                CephRgwFileSystem.CONST_GROUP, currPath));
    }

    abstract void receiveFileHandler(String name, long statPtr, int mask, FileStatus fileStatus)
            throws IOException;
}
