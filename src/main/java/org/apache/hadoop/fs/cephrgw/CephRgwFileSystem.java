/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package org.apache.hadoop.fs.cephrgw;

import com.amazonaws.auth.AWSCredentialsProvider;
import org.apache.commons.io.FileExistsException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;

import static org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet;

import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AUtils;
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
import java.util.Locale;
import java.util.function.Function;

/**
 * The core librgw Filesystem implementation.
 * This subclass is marked as private as code should not be creating it
 * directly; use {@link #FileSystem # get(Configuration)} and variants to
 * create one.
 * If cast to {@code CephRgwFileSystem}, extra methods and features may be accessed.
 * Consider those private and unstable.
 *
 */
public class CephRgwFileSystem extends FileSystem {
    /**
     * Scheme for this FileSystem.
     */
    public static final String SCHEME = "cephrgw";

    static final String CONST_USER = "root";
    static final String CONST_GROUP = "root";
    static final int ERR_NOT_EXISTS = -2;
    static final int ERR_EXISTS = -17;
    static final int ERR_DIR_NOT_EMPTY = -39;
    static final int FLAG_DIR = 0040000;
    static final int LOOKUP_FLAG_NONE = 0;
    static final int LOOKUP_FLAG_CREATE = 1;
    static final int LOOKUP_FLAG_RCB = 2;
    static final int LOOKUP_FLAG_DIR = 4;
    static final int LOOKUP_FLAG_FILE = 8;
    static final byte[] EMPTY_BYTE_TMP = new byte[1];
    static final LinkedHashMap<String, LibRGWFH> FH_CACHE_MAP = new LinkedHashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(CephRgwFileSystem.class);
    private long virtualBlockSize;
    private boolean ensureReadonly = false;
    private int maxInputStreamCacheSize;
    private int cephRgwBufferSize;
    private long librgwFsPtr = 0;
    private LibRGWFH rootFH;
    private URI rootBucketPath;
    private Path rootDirectory = null;
    private S3AFileSystem s3aFileSystemTmp;
    private AWSCredentialProviderList credentials;

    static {
        try {
            System.loadLibrary("rgw_jni");
            staticInit(AbstractFileHandlerReceiver.class);
        } catch (Exception t) {
            LOGGER.error("rgw init failed", t);
            System.exit(1);
        }
    }

    /**
     * Initialize a FileSystem.
     * Called after the new FileSystem instance is constructed, and before it
     * is ready for use.
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
        URI internalName = name;
        if(internalName == null){
            throw new NullPointerException();
        }
        super.initialize(internalName, conf);

        s3aFileSystemTmp = new S3AFileSystem();
        try{
            s3aFileSystemTmp.initialize(reSetUriToS3A(internalName), conf);
        } catch (URISyntaxException e){
        }

        virtualBlockSize = conf.getLong("fs.ceph.rgw.virtual.blocksize", 32 * 1024 * 1024);
        ensureReadonly = conf.getBoolean("fs.ceph.rgw.ensure-readonly", false);

        try{
            rootBucketPath = new URI(internalName.getScheme(), internalName.getAuthority(), "", "");
        } catch (URISyntaxException e){
        }
        setWorkingDirectory(new Path("/"));
        cephRgwBufferSize = conf.getInt("fs.ceph.rgw.io.buffer.size", 1024 * 1024 * 4);
        maxInputStreamCacheSize = conf.getInt("fs.ceph.rgw.max.inputstream.cache.size", 1024 * 64);

        String userId = conf.get("fs.ceph.rgw.userid", "");
        credentials = createAWSCredentialProviderSet(name, conf);
        String accessKey = credentials.getCredentials().getAWSAccessKeyId();
        String secretKey = credentials.getCredentials().getAWSSecretKey();
        try {
            librgwFsPtr = rgwMount(userId, accessKey, secretKey);
            long currFh =
                    rgwLookup(librgwFsPtr, getRootFH(librgwFsPtr), internalName.getAuthority(), 0, 0, LOOKUP_FLAG_DIR);
            rgwGetattr(
                    librgwFsPtr,
                    currFh,
                    new AbstractFileHandlerReceiver(this) {
                        @Override
                        void receiveFileHandler(String name, long statPtr, int mask, FileStatus fileStatus)
                                throws IOException {
                            fileStatus.setPath(getWorkingDirectory());
                            rootFH = new LibRGWFH(CephRgwFileSystem.this, currFh, fileStatus, false);
                        }
                    });

        } catch (CephRgwException e) {
            close();
            throw new IOException("Mount failed.", e);
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

    /**
     * get the RgwFsPtr
     *  @return the rgw fs ptr
     */
    public long getRgwFsPtr() {
        return librgwFsPtr;
    }

    public long getVirtualBlockSize() {
        return virtualBlockSize;
    }

    /**
     * get the CephRgw Statistics
     *  @return CephRgw Statistics
     */
    public Statistics getCephRgwStatistics() {
        return statistics;
    }

    /**
     * open the FSDataInputStream.
     *
     * @param path the data path
     * @param bufferSize the open buffer size
     * @return the FSDataInputStream
     * @throws IOException IO failure
     */
    @Override
    public FSDataInputStream open(final Path path, final int bufferSize) throws IOException {
        statistics.incrementReadOps(1);
        Path absPath = getAbsPath(path);
        if (isDirectory(path)){
            throw new PathIsDirectoryException("Error:Path is directory.");
        }
        FSDataInputStream returnInputStream = doOpen(absPath, bufferSize);
        return returnInputStream;
    }

    /**
     * Create an FSDataOutputStream at the indicated Path with write-progress reporting
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
    public FSDataOutputStream create(
            final Path path,
            final FsPermission permission,
            final boolean overwrite,
            final int bufferSize,
            final short replication,
            final long blockSize,
            final Progressable progress)
            throws IOException {
        Path absPath = getAbsPath(path);
        Path parent = absPath.getParent();
        if (parent != null) {
            mkdirs(parent, permission);
        }
        return createNonRecursive(absPath, permission, overwrite, bufferSize, replication,
                blockSize, progress);
    }

    /**
     * Opens an FSDataOutputStream at the indicated Path with write-progress reporting.
     * Same as create(), except fails if parent directory doesn't already exist.
     *
     * @param newFilePath the file name to create
     * @param permission  file permission
     * @param flags   {@link CreateFlag}s to use for this stream.
     * @param bufferSize  the size of the buffer to be used.
     * @param replication required block replication for the file.
     * @param blockSize   block size
     * @param progress    the progress reporter
     * @return FSDataOutputStream
     * @throws IOException IO failure
     * @see #setPermission(Path, FsPermission)
     */
    @Override
    public FSDataOutputStream createNonRecursive(
            final Path newFilePath,
            final FsPermission permission,
            final EnumSet<CreateFlag> flags,
            final int bufferSize,
            final short replication,
            final long blockSize,
            final Progressable progress)
            throws IOException {
        statistics.incrementWriteOps(1);
        Path absPath = getAbsPath(newFilePath);
        Path parent = absPath.getParent();
        if(flags.contains(CreateFlag.OVERWRITE)){
            if (parent != null && !exists(parent)) {
                throw new FileNotFoundException("Error:File doesn't exist.");
            }
        }else {
            if (parent != null && exists(absPath)) {
                throw new FileAlreadyExistsException("Error:File already exist.");
            }
        }
        if(isDirectory(absPath)){
            throw new PathIsDirectoryException("Error:Path is Directory");
        }
        CephRgwOutputStream cos = new CephRgwOutputStream(this, absPath);
        boolean isException = false;
        try {
            cos.write(EMPTY_BYTE_TMP, 0, 0);
        } catch (IOException e) {
            isException = true;
            LOGGER.error("createNonRecursive Exception for this CephRgwFileSystem " + this.getClass() + "Method:cos.write()");
            throw e;
        } finally {
            if (isException) {
                cos.close();
            }
        }
        return new FSDataOutputStream(new BufferedOutputStream(cos, cephRgwBufferSize), statistics);
    }

    /**
     * reset the uri to s3a
     *
     * @param cephRgwUri the ceph rgw uri
     * @return the s3a uri
     * @throws URISyntaxException failure
     */
    public URI reSetUriToS3A(URI cephRgwUri) throws URISyntaxException {
        String s3aString = "s3a" + ":" + cephRgwUri.toString().split(":")[1];
        URI tmpUri = new URI(s3aString);
        return tmpUri;
    }

    @Override
    public FSDataOutputStream append(final Path path, final int bufferSize, final Progressable progress)
            throws UnsupportedOperationException{
        throw new UnsupportedOperationException("append Not implement yet.");
    }

    @Override
    public boolean rename(final Path src, final Path dst) throws IOException {
        return s3aFileSystemTmp.rename(src, dst);
    }

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

    /**
     * getCanonicalServiceName because we don't support token in CephRgwFileSystem.
     *
     * @return a uri string
     */
    @Override
    public String getCanonicalServiceName() {
        return getUri().toString();
    }

    private  class ListStatusFileHandlerReceiver extends AbstractFileHandlerReceiver {
        private Path absPath;
        private LinkedList<FileStatus> ret;
        private LibRGWFH fileHandle;

        ListStatusFileHandlerReceiver(Path absPath, LinkedList<FileStatus> ret, LibRGWFH fileHandle) {
            super(CephRgwFileSystem.this);
            this.absPath = absPath;
            this.ret = ret;
            this.fileHandle = fileHandle;
        }

        @Override
        void receiveFileHandler(String name, long statPtr, int mask, FileStatus fileStatus) {
            if (fileStatus.getPath() == null) {
                return;
            }
            fileStatus.setPath(new Path(absPath, fileStatus.getPath()));
            ret.add(fileStatus);
            if (!ensureReadonly) {
                return;
            }
        }
    }

    /**
     * get the files status
     *
     * @param newFilePath the file path
     * @return the files status under the path
     * @throws IOException failure
     */
    @Override
    public FileStatus[] listStatus(final Path newFilePath) throws IOException {
        FileStatus checkFileTypeFs = getFileStatus(newFilePath);
        if (checkFileTypeFs.isFile()) {
            return new FileStatus[]{checkFileTypeFs};
        }
        Path absPath = getAbsPath(newFilePath);
        LinkedList<FileStatus> ret = new LinkedList<FileStatus>();
        try (LibRGWFH fileHandle = getFileHandleByAbsPath(absPath, LOOKUP_FLAG_NONE, true, false)) {
            rgwReaddir(librgwFsPtr, fileHandle.getFhPtr(), new ListStatusFileHandlerReceiver(absPath, ret, fileHandle));
        }
        return ret.toArray(new FileStatus[ret.size()]);
    }

    @Override
    public void setWorkingDirectory(final Path newDir) {
        rootDirectory = getAbsPath(newDir);
    }

    @Override
    public Path getWorkingDirectory() {
        return rootDirectory;
    }

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
            throw new IOException(String.format(Locale.ROOT, "Mkdir %s failed.", path),e);
        }
    }

    @Override
    public FileStatus getFileStatus(final Path path) throws IOException {
        Path absPath = getAbsPath(path);
        try (LibRGWFH fh = getFileHandleByAbsPath(absPath, LOOKUP_FLAG_NONE, true, true)) {
            FileStatus ret = fh.getFileStatus();
            return ret;
        }
    }

    @Override
    public void close() throws IOException {
        if (rootFH != null) {
            rootFH.doClose();
        }
        rgwUmount(librgwFsPtr);
        super.close();
        S3AUtils.closeAutocloseables(LOGGER, credentials);
        credentials = null;
    }

    /**
     * whether the mode is a directory
     *
     * @param mode the input param
     * @return result of the mode
     */
    public boolean isDir(final int mode) {
        return (mode & FLAG_DIR) != 0;
    }

    private FSDataInputStream doOpen(final Path absPath, final int bufSize) throws IOException {
        CephRgwInputStream in = new CephRgwInputStream(this, absPath);
        long fileSize = in.getFileSize();
        if (fileSize <= 0) {
            return new FSDataInputStream(in);
        }
        int maxIntFileSize = (int) Math.min(fileSize, Integer.MAX_VALUE);
        int newBufferSize = Math.min(maxIntFileSize, cephRgwBufferSize);
        return new FSDataInputStream(new BufferedFSInputStream(in, newBufferSize));
    }

    /**
     * get the file handler by the abs path
     *
     * @param path the input param
     * @param flag the file operate mode
     * @param isCache is cache field handler
     * @param isLoadListStatus whether get the all the files handlers status
     * @return LibRGWfile handler
     * @throws IOException failure
     */
    public LibRGWFH getFileHandleByAbsPath(
            final Path path, final int flag, final boolean isCache, final boolean isLoadListStatus) throws IOException {
        boolean internalIsCache = isCache && ensureReadonly;
        if (path.isRoot()) {
            return rootFH;
        }
        try {
            return getLibRGWFHDirect(path, flag, internalIsCache);
        } catch (CephRgwException e) {
            if (e.getErrcode() == ERR_NOT_EXISTS) {
                throw new FileNotFoundException(path.toString());
            }
            throw new IOException("Find path " + path.toString() + " failed.", e);
        }
    }

    private LibRGWFH getLibRGWFHDirect(final Path path, int flag, boolean cache) throws CephRgwException, IOException {
        String pathName = getCephPathStr(path).substring(1);
        LibRGWFH[] ret = new LibRGWFH[1];
        long fh = rgwLookup(librgwFsPtr, rootFH.getFhPtr(), pathName, 0, 0, flag);
        rgwGetattr(
                librgwFsPtr,
                fh,
                new AbstractFileHandlerReceiver(this) {
                    @Override
                    void receiveFileHandler(String name, long statPtr, int mask, FileStatus fileStatus)
                            throws IOException {
                        fileStatus.setPath(path);
                        ret[0] = new LibRGWFH(CephRgwFileSystem.this, fh, fileStatus, cache);
                    }
                });
        return ret[0];
    }

    private class DoDeleteFileHandlerReceiver extends AbstractFileHandlerReceiver{
        private Path path;
        private boolean recursive;
        private LibRGWFH fileHandle;

        DoDeleteFileHandlerReceiver(Path path, boolean recursive, LibRGWFH fileHandle) {
            super(CephRgwFileSystem.this);
            this.path = path;
            this.recursive = recursive;
            this.fileHandle = fileHandle;
        }

        @Override
        void receiveFileHandler(String name, long statPtr, int mask, FileStatus fileStatus) throws IOException {
            if (!recursive) {
                throw new PathIsNotEmptyDirectoryException(path.toString());
            }
            if (fileStatus.getPath() == null) {
                return;
            }
            fileStatus.setPath(new Path(path, fileStatus.getPath()));
            try{
                doDelete(fileStatus.getPath(), fileHandle, true);
            } catch (FileNotFoundException e) {
                LOGGER.debug("");
            }
        }
    }

    private void doDelete(final Path path, final LibRGWFH parentFh, final boolean recursive) throws IOException {
        try (LibRGWFH fileHandle = getFileHandleByAbsPath(path, LOOKUP_FLAG_NONE, false, false)) {
            AbstractFileHandlerReceiver recver = new DoDeleteFileHandlerReceiver(path, recursive, fileHandle);
            rgwReaddir(librgwFsPtr, fileHandle.getFhPtr(), recver);
            rgwUnlink(librgwFsPtr, parentFh.getFhPtr(), path.getName());
        } catch (CephRgwException e) {
            if (e.getErrcode() == ERR_NOT_EXISTS) {
                throw new FileNotFoundException(path.toString());
            }
            if (e.getErrcode() == ERR_DIR_NOT_EMPTY) {
                throw new DirectoryNotEmptyException(path.toUri().getPath());
            }
            throw new IOException(String.format(Locale.ROOT, "delete path %s failed.", path), e);
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

    private static native void staticInit(Class<AbstractFileHandlerReceiver> fileHandlerReceiver)
            throws CephRgwException;

    /**
     * umount rgw
     *
     * @param rgwFsPtr the rgw filesystem ptr
     */
    public native void rgwUmount(long rgwFsPtr);

    /**
     * rgw read the data
     *
     * @param rgwFsPtr the rgw filesystem ptr
     * @param fileHandlePtr the rgw file handle ptr
     * @param position position of file
     * @param length expected read length
     * @param buffer buffer to receive data
     * @param offset start location in buffer
     * @return the read result
     * @throws CephRgwException failure
     */
    public native int rgwRead(long rgwFsPtr, long fileHandlePtr, long position, int length, byte[] buffer, int offset)
            throws CephRgwException;

    /**
     * rgw write the data
     *
     * @param rgwFsPtr the rgw filesystem ptr
     * @param fileHandlePtr the rgw file handle ptr
     * @param position position of file
     * @param length expected write length
     * @param buffer data buffer to write
     * @param offset start location in buffer
     * @throws CephRgwException failure
     */
    public native void rgwWrite(long rgwFsPtr, long fileHandlePtr, long position, int length, byte[] buffer, int offset)
            throws CephRgwException;

    /**
     * create rgw file system
     *
     * @param userId the user id
     * @param accessKey access key same as s3
     * @param secretKey secret key same as s3
     * @return the rgw file system pointer
     * @throws CephRgwException failure
     */
    public native long rgwMount(String userId, String accessKey, String secretKey) throws CephRgwException;

    /**
     * open a file from file handle.
     *
     * @param rgwFsPtr the rgw filesystem ptr
     * @param fileHandlePtr the rgw file handle ptr
     * @throws CephRgwException failure
     */
    public native void rgwOpen(long rgwFsPtr, long fileHandlePtr) throws CephRgwException;

    /**
     * close a file handle
     *
     * @param rgwFsPtr
     * @param fileHandlePtr
     */
    public native void rgwClose(long rgwFsPtr, long fileHandlePtr);

    /**
     * find file by parent file handle and file name
     *
     * @param fsrgwFsPtr the rgw filesystem ptr
     * @param parentFh the parent file handle
     * @param pathName selected file name
     * @param statPtr stat ptr for receive rgw handle
     * @param mask mask for handle
     * @param flag lookup flag
     * @return the rgw file handle ptr
     * @throws CephRgwException failure
     */
    public native long rgwLookup(long fsrgwFsPtr, long parentFh, String pathName, long statPtr, int mask, int flag)
            throws CephRgwException;

    /**
     * get root file handle from rgw file system
     *
     * @param rgwFsPtr the rgw filesystem ptr
     * @return the root file handle from rgw file system
     */
    public native long getRootFH(long rgwFsPtr);

    /**
     * rgw rename the file
     *
     * @param rgwFsPtr the rgw filesystem ptr
     * @param srcFh the source file handle ptr
     * @param srcName the source file name
     * @param dstFh the destination file handler ptr
     * @param dstName the destination file name
     * @throws CephRgwException failure
     */
    public native void rgwRename(long rgwFsPtr, long srcFh, String srcName, long dstFh, String dstName)
        throws CephRgwException;

    /**
     * delete the file
     *
     * @param rgwFsPtr the rgw filesystem ptr
     * @param fileHandlePtr file handle ptr for the parent path od deleted file
     * @param name deleted file name
     * @throws CephRgwException failure
     */
    public native void rgwUnlink(long rgwFsPtr, long fileHandlePtr, String name) throws CephRgwException;

    /**
     * rgw get the attributes
     *
     * @param rgwFsPtr the rgw filesystem ptr
     * @param fileHandlePtr the file handler ptr
     * @param receiver the AbstractFileHandlerReceiver
     * @throws CephRgwException failure
     */
    public native void rgwGetattr(long rgwFsPtr, long fileHandlePtr, AbstractFileHandlerReceiver receiver) throws CephRgwException;

    /**
     * rgw read the file folder
     *
     * @param rgwFsPtr the rgw filesystem ptr
     * @param fileHandlePtr the file handler ptr
     * @param receiver the AbstractFileHandlerReceiver
     */
    public native void rgwReaddir(long rgwFsPtr, long fileHandlePtr, AbstractFileHandlerReceiver receiver);

    /**
     * rgw create folder
     *
     * @param rgwFsPtr the rgw filesystem ptr
     * @param fileHandlePtr the file handler ptr
     * @param name the file name
     * @param mode the input param
     * @throws CephRgwException failure
     */
    public native void rgwMkdir(long rgwFsPtr, long fileHandlePtr, String name, int mode) throws CephRgwException;

    /**
     * get the Length
     *
     * @param statPtr stat ptr for receive rgw handle
     * @return the length
     */
    public native long getLength(long statPtr);

    /**
     * get the AccessTime
     *
     * @param statPtr stat ptr for receive rgw handle
     * @return the access time
     */
    public native long getAccessTime(long statPtr);

    /**
     * get the ModifyTime
     *
     * @param statPtr stat ptr for receive rgw handle
     * @return the modify time
     */
    public native long getModifyTime(long statPtr);

    /**
     * get the mode
     *
     * @param statPtr stat ptr for receive rgw handle
     * @return the mode of the statPtr
     */
    public native int getMode(long statPtr);
}

