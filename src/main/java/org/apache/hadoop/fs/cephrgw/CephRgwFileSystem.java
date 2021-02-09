/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 *
 * Description: The core librgw Filesystem implementation.
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

import org.apache.commons.io.FileExistsException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryNotEmptyException;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Locale;
import java.util.Iterator;

/**
 * The core librgw Filesystem implementation.
 * This subclass is marked as private as code should not be creating it
 * directly; use {@link #FileSystem # get(Configuration)} and variants to
 * create one.
 * If cast to {@code CephRgwFileSystem}, extra methods and features may be accessed.
 * Consider those private and unstable.
 */
public class CephRgwFileSystem extends FileSystem {
    /**
     * Scheme for this FileSystem.
     */
    public static final String SCHEME = "cephrgw";
    /**
     * Currently, the liibrgw does not have any permission control function.
     * Therefore, the user name and user group in the Hadoop file system are replaced by root.
     */
    public static final String CONST_USER = "root";
    public static final String CONST_GROUP = "root";
    public static final int ERR_NOT_EXISTS = -2;
    public static final int ERR_EXISTS = -17;
    public static final int ERR_DIR_NOT_EMPTY = -39;
    /**
     * The 15th bit in the permission query result of the librgw indicates whether the file is a directory,
     * that is, 0x4000.
     */
    public static final int FLAG_DIR = 0x4000;
    public static final int LOOKUP_FLAG_NONE = 0;
    public static final int LOOKUP_FLAG_CREATE = 1;
    public static final int LOOKUP_FLAG_RCB = 2;
    public static final int LOOKUP_FLAG_DIR = 4;
    public static final int LOOKUP_FLAG_FILE = 8;
    public static final byte[] EMPTY_BYTE_TMP = new byte[1];
    static final Map<String, LibRGWFH> FH_CACHE_MAP = new LinkedHashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(CephRgwFileSystem.class);
    private transient long vBlockSize;
    private transient boolean isReadonly;
    private transient int maxCacheSize;
    private transient int rgwBufferSize;
    private transient long librgwFsPtr;
    private transient LibRGWFH rootFH;
    private transient URI rootBucketPath;
    private transient Path rootDirectory;

    static {
        try {
            System.loadLibrary("rgw_jni");
            staticInit(AbstractFileHandlerReceiver.class);
        } catch (Exception t) {
            LOGGER.error("", t);
            throw new RuntimeException("Initialization failed.");
        }
    }

    /**
     * Initialize a FileSystem.
     * <p>
     * Called after the new FileSystem instance is constructed, and before it
     * is ready for use.
     * <p>
     * FileSystem implementations overriding this method MUST forward it to
     * their superclass, though the order in which it is done, and whether
     * to alter the configuration before the invocation are options of the
     * subclass.
     *
     * @param name a URI whose authority section names the host, port, etc.
     *             for this FileSystem
     * @param conf the configuration
     * @throws IOException              on any failure to initialize this instance.
     * @throws IllegalArgumentException if the URI is considered invalid.
     */
    @Override
    public void initialize(final URI name, final Configuration conf) throws IOException {
        vBlockSize = conf.getLong("fs.ceph.rgw.virtual.blocksize", Long.MAX_VALUE);
        isReadonly = conf.getBoolean("fs.ceph.rgw.ensure-readonly", false);

        URI internalName = name;
        if (internalName == null) {
            internalName = getDefaultUri(conf);
        }
        super.initialize(internalName, conf);
        try {
            rootBucketPath = new URI(internalName.getScheme(), internalName.getAuthority(), "", "");
        } catch (URISyntaxException e) {
            LOGGER.error("URISyntaxException:" + e);
            throw new IllegalArgumentException("initialize failed,please check  the path");
        }

        setWorkingDirectory(new Path("/"));
        // I/O buffer sizes 16MB
        rgwBufferSize = conf.getInt("fs.ceph.rgw.io.buffer.size", 1024 * 1024 * 16);
        maxCacheSize = conf.getInt("fs.ceph.rgw.max.inputstream.cache.size", 1024 * 64);
        try {
            String userId = conf.get("fs.ceph.rgw.userid", "");
            String accessKey = conf.get("fs.ceph.rgw.access.key", "");
            String secretKey = conf.get("fs.ceph.rgw.secret.key", "");
            librgwFsPtr = rgwMount(userId, accessKey, secretKey);
            long currFh = rgwLookup(librgwFsPtr, getRootFH(librgwFsPtr), internalName.getAuthority(), 0, 0, LOOKUP_FLAG_DIR);
            rgwGetattr(librgwFsPtr, currFh, new AbstractFileHandlerReceiver(this) {
                @Override
                void receiveFileHandler(String name, long statPtr, int mask, FileStatus fileStatus) {
                    fileStatus.setPath(getWorkingDirectory());
                    rootFH = new LibRGWFH(CephRgwFileSystem.this, currFh, fileStatus, false);
                }
            });
        } catch (CephRgwException e) {
            /*
              If the initialization fails,the system invokes the shutdown operation to clear the remaining resources
              and throws an exception.
             */
            close();
            LOGGER.error("IOException:Mount failed.", e);
            throw new IOException("Mount failed.");
        }
    }

    @Override
    public URI getUri() {
        return rootBucketPath;
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }

    public LibRGWFH getRootFH() {
        return rootFH;
    }

    public long getRgwFsPtr() {
        return librgwFsPtr;
    }

    public long getVirtualBlockSize() {
        return vBlockSize;
    }

    public Statistics getCephRgwStatistics() {
        return statistics;
    }

    @Override
    public FSDataInputStream open(final Path path, final int bufferSize) throws IOException {
        statistics.incrementReadOps(1);
        Path absPath = getAbsPath(path);
        return doOpen(absPath);
    }

    /**
     * Create an FSDataOutputStream at the indicated Path with write-progress
     * reporting.
     *
     * @param path        the file name to create
     * @param permission  file permission
     * @param overwrite   if a file with this name already exists, then if true,
     *                    the file will be overwritten, and if false an error will be thrown.
     * @param bufferSize  the size of the buffer to be used.
     * @param replication required block replication for the file.
     * @param blockSize   block size
     * @param progress    the progress reporter
     * @return FSDataOutputStream
     * @throws IOException IO failure
     */
    @Override
    public FSDataOutputStream create(final Path path, final FsPermission permission, final boolean overwrite,
                                     final int bufferSize, final short replication, final long blockSize,
                                     final Progressable progress) throws IOException {
        Path absPath = getAbsPath(path);
        Path parent = absPath.getParent();
        if (parent != null) {
            mkdirs(parent);
        }
        return createNonRecursive(absPath, permission, overwrite, bufferSize, replication,
                blockSize, progress);
    }

    /**
     * Opens an FSDataOutputStream at the indicated Path with write-progress
     * reporting. Same as create(), except fails if parent directory doesn't
     * already exist.
     *
     * @param newFilePath the file name to create
     * @param permission  file permission
     * @param isCreated   {@link CreateFlag}s to use for this stream.
     * @param bufferSize  the size of the buffer to be used.
     * @param replication required block replication for the file.
     * @param blockSize   block size
     * @param progress    the progress reporter
     * @return FSDataOutputStream
     * @throws IOException IO failure
     * @see #setPermission(Path, FsPermission)
     */
    @Override
    public FSDataOutputStream createNonRecursive(final Path newFilePath, final FsPermission permission,
                                                 final EnumSet<CreateFlag> isCreated, final int bufferSize, final short replication, final long blockSize,
                                                 final Progressable progress) throws IOException {
        statistics.incrementWriteOps(1);
        Path absPath = getAbsPath(newFilePath);
        Path parent = absPath.getParent();
        if (parent != null && !exists(parent)) {
            LOGGER.error("FileNotFoundException:Parent path doesn't exist." + parent.toString());
            throw new FileNotFoundException("Error:File doesn't exist.");
        }
        CephRgwOutputStream cos = new CephRgwOutputStream(this, absPath);
        boolean isException = false;
        try {
            cos.write(EMPTY_BYTE_TMP, 0, 0);
        } catch (IOException e) {
            isException = true;
            LOGGER.error("createNonRecursive Exception for this CephRgwFileSystem " + this.getClass() + "Method:cos.write()");
            throw new CephRgwException("createNonRecursive Exception for this CephRgwFileSystem.");
        } finally {
            if (isException) {
                cos.close();
            }
        }
        return new FSDataOutputStream(new BufferedOutputStream(cos, rgwBufferSize), statistics);
    }

    @Override
    public FSDataOutputStream append(final Path path, final int bufferSize, final Progressable progress) {
        throw new UnsupportedOperationException("append Not implement yet.");
    }

    /**
     * Rename the file or directory denoted by the first abstract pathname to
     * the second abstract pathname, returning <code>true</code> if and only if
     * the operation succeeds.
     */
    @Override
    public boolean rename(final Path src, final Path dst) throws IOException {
        Path absPathDst = getAbsPath(dst);
        if (exists(absPathDst)) {
            LOGGER.error("File already exist:" + src.toString());
            throw new FileExistsException("File already exist,please check the file path.");
        }
        Path absPathSrc = getAbsPath(src);
        FileStatus srcStatus = getFileStatus(absPathSrc);
        if (srcStatus.isDirectory()) {
            LOGGER.error("IllegalArgumentException:path rename unsupported," + src);
            throw new IllegalArgumentException("ERROR:path rename unsupported.");
        }
        return doRename(absPathSrc, absPathDst);
    }

    /**
     * Abstract class that implements the FileSystem class to delete files and directories.
     *
     * @param path      the file name to delete
     * @param recursive Check whether the directory is empty.
     * @return boolean
     * @throws IOException IO Exception
     */
    @Override
    public boolean delete(final Path path, final boolean recursive) throws IOException {
        Path absPath = getAbsPath(path);
        if (absPath.isRoot()) {
            LOGGER.error("Delete path cannot be root:" + path);
            throw new IOException("Invalid delete path.");
        }
        Path parent = absPath.getParent();
        try (LibRGWFH parentFh = getFileHandleByAbsPath(parent, LOOKUP_FLAG_NONE, true, false)) {
            doDelete(absPath, parentFh, recursive);
            return true;
        } catch (FileNotFoundException fnfe) {
            LOGGER.error("FileNotFoundException:" + fnfe.toString());
            return false;
        }
    }

    // Abstract class for implementing the FileSystem class to obtain subfiles in a directory.
    @Override
    public FileStatus[] listStatus(final Path newFilePath) throws IOException {
        FileStatus checkFileTypeFs = getFileStatus(newFilePath);
        if (checkFileTypeFs.isFile()) {
            return new FileStatus[]{checkFileTypeFs};
        }
        Path absPath = getAbsPath(newFilePath);
        LinkedList<FileStatus> fileStatusList = new LinkedList<>();
        try (LibRGWFH fileHandle = getFileHandleByAbsPath(absPath, LOOKUP_FLAG_NONE, true, false)) {
            rgwReaddir(librgwFsPtr, fileHandle.getFhPtr(), new AbstractFileHandlerReceiver(this) {
                @Override
                void receiveFileHandler(String name, long statPtr, int mask, FileStatus fileStatus) {
                    if (fileStatus.getPath() == null) {
                        return;
                    }
                    fileStatus.setPath(new Path(absPath, fileStatus.getPath()));
                    fileStatusList.add(fileStatus);
                    if (isReadonly) {
                        try {
                            long subFileHandlePtr = rgwLookup(librgwFsPtr, fileHandle.getFhPtr(), name, statPtr, mask,
                                    LOOKUP_FLAG_RCB | (fileStatus.isDirectory() ? LOOKUP_FLAG_DIR : LOOKUP_FLAG_FILE));
                            putFhToCache(fileStatus.getPath(), new LibRGWFH(CephRgwFileSystem.this,
                                    subFileHandlePtr, fileStatus, true));
                        } catch (IOException e) {
                            LOGGER.error("", e);
                        }
                    }
                }
            });
        }
        return fileStatusList.toArray(new FileStatus[0]);
    }

    @Override
    public void setWorkingDirectory(final Path newDir) {
        rootDirectory = getAbsPath(newDir);
    }

    @Override
    public Path getWorkingDirectory() {
        return rootDirectory;
    }

    /**
     * Make the given file and all non-existent parents into
     * directories. Has roughly the semantics of Unix @{code mkdir -p}.
     * Existence of the directory hierarchy is not an error.
     *
     * @param path       path to create
     * @param permission to apply to path
     * @throws IOException IO failure
     */
    @Override
    public boolean mkdirs(final Path path, final FsPermission permission) throws IOException {
        Path absPath = getAbsPath(path);
        Path parent = absPath.getParent();
        // Create a trail in recursive mode. If the root directory is null, the path is successfully created.
        if (parent == null) {
            return true;
        }
        if (exists(absPath)) {
            if (!getFileStatus(absPath).isFile()) {
                return true;
            }
            LOGGER.error("File is already exist:" + path.toString());
            throw new FileExistsException("Failed to create the file because the file already exists.");
        }
        mkdirs(parent, permission);
        try (LibRGWFH fileHandle = getFileHandleByAbsPath(parent, LOOKUP_FLAG_CREATE | LOOKUP_FLAG_DIR, true, false)) {
            rgwMkdir(librgwFsPtr, fileHandle.getFhPtr(), absPath.getName(), permission.toShort());
            return true;
        } catch (CephRgwException e) {
            if (e.getErrcode() == ERR_EXISTS) {
                try (LibRGWFH fh2 = getFileHandleByAbsPath(absPath, LOOKUP_FLAG_NONE, true, false)) {
                    if (fh2.getFileStatus().isDirectory()) {
                        return true;
                    }
                    LOGGER.error("PathExistsException" + path.toString());
                    throw new PathExistsException("ERROR:Path already exisit.");
                }
            }
            LOGGER.error(String.format(Locale.ROOT, "Mkdir %s failed.", path), e);
            throw new CephRgwException("Mkdir file failed.");
        }
    }

    @Override
    public FileStatus getFileStatus(final Path path) throws IOException {
        Path absPath = getAbsPath(path);
        try (LibRGWFH libRGWFH = getFileHandleByAbsPath(absPath, LOOKUP_FLAG_NONE, true, true)) {
            return libRGWFH.getFileStatus();
        }
    }

    @Override
    public void close() throws IOException {
        if (rootFH != null) {
            rootFH.doClose();
        }
        rgwUmount(librgwFsPtr);
        super.close();
    }

    public boolean isDir(final int mode) {
        return (mode & FLAG_DIR) != 0;
    }

    private FSDataInputStream doOpen(final Path absPath) throws IOException {
        CephRgwInputStream rgwInputStream = new CephRgwInputStream(this, absPath);
        long fileSize = rgwInputStream.getFileSize();
        if (fileSize <= 0) {
            return new FSDataInputStream(rgwInputStream);
        }
        int maxIntFileSize = (int) Math.min(fileSize, Integer.MAX_VALUE);
        int newBufferSize = Math.min(maxIntFileSize, rgwBufferSize);
        LOGGER.debug("cephrgw read buffer size:{}.", newBufferSize);
        return new FSDataInputStream(new BufferedFSInputStream(rgwInputStream, newBufferSize));
    }

    public LibRGWFH getFileHandleByAbsPath(final Path path, final int flag, final boolean isCache,
                                           final boolean isLoadListStatus) throws IOException {
        if (path.isRoot()) {
            return rootFH;
        }
        String pathName = getCephPathStr(path).substring(1);
        boolean internalIsCache = isCache && isReadonly;
        // Obtaining Handles When the Handle Cache Is Enabled.
        if (internalIsCache) {
            LibRGWFH rgwfh;
            synchronized (FH_CACHE_MAP) {
                rgwfh = FH_CACHE_MAP.remove(path.toString());
            }
            if (rgwfh != null) {
                rgwfh.ref();
            } else {
                if (isLoadListStatus) {
                    listStatus(path.getParent());
                    return getFileHandleByAbsPath(path, flag, internalIsCache, false);
                }
                rgwfh = getRgwFh(path, pathName, internalIsCache, flag);
            }
            putFhToCache(path, rgwfh);
            return rgwfh;
        }
        return getRgwFh(path, pathName, internalIsCache, flag);
    }

    // Invokes the librgw interface to obtain handles.
    public LibRGWFH getRgwFh(Path path, String pathName, boolean internalIsCache, int flag) throws IOException {
	try {
        LibRGWFH[] libRgwFh = new LibRGWFH[1];
        long fhPtr = rgwLookup(librgwFsPtr, rootFH.getFhPtr(), pathName, 0, 0, flag);
        rgwGetattr(librgwFsPtr, fhPtr, new AbstractFileHandlerReceiver(this) {
            @Override
            void receiveFileHandler(String name, long statPtr, int mask, FileStatus fileStatus) {
                fileStatus.setPath(path);
                libRgwFh[0] = new LibRGWFH(CephRgwFileSystem.this, fhPtr, fileStatus, internalIsCache);
            }
        });
        return libRgwFh[0];
	} catch (CephRgwException e) {
	    if (e.getErrcode() == ERR_NOT_EXISTS) {
		LOGGER.error("FileNotFoundException" + path.toString());
		throw new FileNotFoundException("ERROR:File not found.");
	    }
	    throw e;
	}
    }

    private void putFhToCache(final Path path, final LibRGWFH fhs) {
        synchronized (FH_CACHE_MAP) {
            if (FH_CACHE_MAP.size() >= maxCacheSize) {
                Iterator<String> iterator = FH_CACHE_MAP.keySet().iterator();
                String minFinishTimePath = iterator.next();
                if (minFinishTimePath != null) {
                    LibRGWFH removeFH = FH_CACHE_MAP.remove(minFinishTimePath);
                    if (removeFH != null) {
                        removeFH.close();
                    }
                }
            }
            FH_CACHE_MAP.put(path.toString(), fhs);
        }
    }

    private boolean doRename(final Path src, final Path dst) throws IOException {
        try (InputStream srcIn = open(src);
             OutputStream dstOut = create(dst, false)) {
            IOUtils.copy(srcIn, dstOut);

        }
        return delete(src, false);
    }

    private void doDelete(final Path path, final LibRGWFH parentFh, final boolean recursive) throws IOException {
        try (LibRGWFH fileHandle = getFileHandleByAbsPath(path, LOOKUP_FLAG_NONE, false, false)) {
            rgwReaddir(librgwFsPtr, fileHandle.getFhPtr(), new AbstractFileHandlerReceiver(this) {
                @Override
                void receiveFileHandler(String name, long statPtr, int mask, FileStatus fileStatus)
                        throws IOException {
                    if (!recursive) {
                        LOGGER.error("path is not empty Directory: " + path.toString());
                        throw new PathIsNotEmptyDirectoryException("path is not empty Directory.");
                    }
                    if (fileStatus.getPath() == null) {
                        return;
                    }
                    fileStatus.setPath(new Path(path, fileStatus.getPath()));
                    try {
                        doDelete(fileStatus.getPath(), fileHandle, true);
                    } catch (FileNotFoundException e) {
                        LOGGER.error("FileNotFoundException:" + e);
                    }
                }
            });
            rgwUnlink(librgwFsPtr, parentFh.getFhPtr(), path.getName());
        } catch (CephRgwException e) {
            if (e.getErrcode() == ERR_NOT_EXISTS) {
                LOGGER.error("FileNotFoundException" + path.toString());
                throw new FileNotFoundException("ERROR:File not found.");
            }
            if (e.getErrcode() == ERR_DIR_NOT_EMPTY) {
                LOGGER.error("DirectoryNotEmptyException" + path.toUri().getPath());
                throw new DirectoryNotEmptyException("path is not empty Directory.");
            }
            LOGGER.error(String.format(Locale.ROOT, "delete path %s failed.", path), e);
            throw new CephRgwException("Delete file failed.");
        }
    }

    private static void throwRgwExceptionForNative(final int errcode, final String msg) throws CephRgwException {
        throw new CephRgwException(errcode, msg);
    }

    private Path getAbsPath(final Path path) {
        return makeQualified(path);
    }

    private String clearEndSeperator(final String src) {
        StringBuilder returnPath = new StringBuilder(src);
        while (returnPath.charAt(returnPath.length() - 1) == '/') {
            returnPath.deleteCharAt(returnPath.length() - 1);
        }
        return returnPath.toString();
    }

    private String getCephPathStr(final Path absPath) {
        String pathStr = Path.getPathWithoutSchemeAndAuthority(absPath).toString();
        pathStr = clearEndSeperator(pathStr);
        return pathStr.replaceAll("/+", "/");
    }

    private static native void staticInit(Class<AbstractFileHandlerReceiver> fileHandlerReceiver) throws CephRgwException;

    public native void rgwUmount(final long rgwFsPtr);

    public native int rgwRead(long rgwFsPtr, long fileHandlePtr, long position, int length, byte[] buffer, int offset)
            throws CephRgwException;

    public native void rgwWrite(long rgwFsPtr, long fileHandlePtr, long position, int length, byte[] buffer, int offset)
            throws CephRgwException;

    public native long rgwMount(String userId, String accessKey, String secretKey) throws CephRgwException;

    public native void rgwOpen(long rgwFsPtr, long fileHandlePtr) throws CephRgwException;

    public native void rgwClose(long rgwFsPtr, long fileHandlePtr);

    public native long rgwLookup(long fsrgwFsPtr, long parentFh, String pathName, long statPtr, int mask, int flag)
            throws CephRgwException;

    public native long getRootFH(long rgwFsPtr);

    public native void rgwUnlink(long rgwFsPtr, long fileHandlePtr, String name) throws CephRgwException;

    public native void rgwGetattr(long rgwFsPtr, long fileHandlePtr, AbstractFileHandlerReceiver receiver) throws CephRgwException;

    public native void rgwReaddir(long rgwFsPtr, long fileHandlePtr, AbstractFileHandlerReceiver receiver);

    public native void rgwMkdir(long rgwFsPtr, long fileHandlePtr, String name, int mode) throws CephRgwException;

    public native long getLength(long statPtr);

    public native long getAccessTime(long statPtr);

    public native long getModifyTime(long statPtr);

    public native int getMode(long statPtr);
}

