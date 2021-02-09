/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 *
 * Description: Input stream for librgw native call.
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

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Locale;

class CephRgwInputStream extends FSInputStream {
    private transient final LibRGWFH fhPtr;
    private transient long position;
    private transient final long fileSize;
    private transient final CephRgwFileSystem fileSystem;

    CephRgwInputStream(final CephRgwFileSystem fileSystem, final Path path) throws IOException {
        this.fileSystem = fileSystem;
        this.fhPtr = fileSystem.getFileHandleByAbsPath(path, CephRgwFileSystem.LOOKUP_FLAG_FILE, true, true);
        this.fileSize = fhPtr.getFileStatus().getLen();
    }

    @Override
    public void seek(long pos) {
        this.position = pos;
        if (this.position > fileSize) {
            this.position = fileSize;
        }
        if (this.position < 0) {
            this.position = 0;
        }
    }

    @Override
    public long getPos() {
        return position;
    }

    public long getFileSize() {
        return fileSize;
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
        return false;
    }

    @Override
    public int read() throws IOException {
        byte[] buf = new byte[1];
        int ret;
        do {
            ret = read(buf, 0, 1);
        } while (ret == 0);
        return ret < 0 ? ret : (int) (buf[0]) & 0xff;
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        int ret = read(position, buf, off, len);
        if (ret >= 0) {
            position += ret;
        }
        return ret;
    }

    @Override
    public void close() throws IOException {
        doClose();
    }

    public void doClose() throws IOException {
        fhPtr.close();
        super.close();
    }

    // Invoke the librgw function to read files.
    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        if (position >= fileSize) {
            return -1;
        }
        try {
            int expectedReadLength = (int) Math.min(fileSize - position, length);
            int ret = fileSystem.rgwRead(fileSystem.getRgwFsPtr(), fhPtr.getFhPtr(), position, expectedReadLength,
                    buffer, offset);
            if (ret > 0) {
                FileSystem.Statistics fileStatistics = new FileSystem.Statistics(fileSystem.getCephRgwStatistics());
                fileStatistics.incrementBytesRead(ret);
            }
            return ret;
        } catch (CephRgwException e) {
            throw new IOException(String.format(Locale.ROOT, "read file from position:%d, length:%d failed.",
                    position, length), e);
        }
    }
}