/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
package org.apache.hadoop.fs.cephrgw;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;

/**
 * FileHandlerReceiver is interface for handling struct stat.
 */
public abstract class AbstractFileHandlerReceiver {
    private CephRgwFileSystem fileSystem;

    AbstractFileHandlerReceiver(CephRgwFileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    void receiveFileHandler(String name, long statPtr, int mask) throws IOException {
        Path currPath = null;
        if (name != null && name.length() > 0) {
            currPath = new Path(name);
        }
        int mode = fileSystem.getMode(statPtr);
        receiveFileHandler(name, statPtr, mask, new FileStatus(fileSystem.getLength(statPtr), fileSystem.isDir(mode),
                0, fileSystem.getVirtualBlockSize(), fileSystem.getModifyTime(statPtr),
                fileSystem.getAccessTime(statPtr), new FsPermission(mode), CephRgwFileSystem.CONST_USER,
                CephRgwFileSystem.CONST_GROUP, currPath));
    }

    abstract void receiveFileHandler(String name, long statPtr, int mask, FileStatus fileStatus)
            throws IOException;
}
