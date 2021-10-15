/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 *
 * Description: Output stream for librgw native call.
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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Locale;

public class CephRgwOutputStream extends OutputStream {
    private transient long currPos = 0;
    private transient final LibRGWFH fhPtr;
    private transient final CephRgwFileSystem fileSystem;

    CephRgwOutputStream(CephRgwFileSystem fileSystem, Path path) throws IOException {
        this.fileSystem = fileSystem;
        fhPtr = fileSystem.getFileHandleByAbsPath(path,
                CephRgwFileSystem.LOOKUP_FLAG_CREATE | CephRgwFileSystem.LOOKUP_FLAG_FILE,
                false, false);
        try {
            fileSystem.rgwOpen(fileSystem.getRgwFsPtr(), fhPtr.getFhPtr());
        } catch (CephRgwException e) {
            fhPtr.close();
            throw new IOException(String.format(Locale.ROOT, "open file %s failed.", path), e);
        }
    }

    @Override
    public void write(int byteData) throws IOException {}

    // Implementation of the librgw file writing function.
    @Override
    public void write(byte[] buf, int off, int len) throws IOException {
        try {
            fileSystem.rgwWrite(fileSystem.getRgwFsPtr(), fhPtr.getFhPtr(), currPos, len, buf, off);
            FileSystem.Statistics fileStatistics = new FileSystem.Statistics(fileSystem.getCephRgwStatistics());
            fileStatistics.incrementBytesWritten(len);
            currPos += len;
        } catch (CephRgwException e) {
            throw new IOException(String.format(Locale.ROOT, "write file to position:%d, length:%d failed.",
                    currPos, len), e);
        }
    }

    @Override
    public void close() throws IOException {
        fhPtr.close();
        super.close();
    }
}