/*
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
package org.apache.hadoop.fs.s3a;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryNotEmptyException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedList;

import org.apache.commons.io.FileExistsException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CephRgwFileSystem extends FileSystem {
	private static final int ERR_NOT_EXISTS = -2;
	private static final int ERR_EXISTS = -17;
	private static final int ERR_DIR_NOT_EMPTY = -39;
	private static final int FLAG_DIR = 0040000;
	private static final Logger logger = LoggerFactory.getLogger(CephRgwFileSystem.class);
	private static final byte[] empty = new byte[1];

	static {
		try {
			System.loadLibrary("rgw_jni");
			staticInit(LinuxStat.class, HashSet.class);
		} catch (Throwable t) {
			logger.error("", t);
			System.exit(1);
		}
	}

	private long virtualBlockSize;
	private int bufferSize;
	private long fs = 0;
	private String accessKey = null;
	private String userId = null;
	private String secretKey = null;
	private URI rootBucketPath;
	private Path rootDirectory = null;

	public void initialize(URI name, Configuration conf) throws IOException {
	    super.initialize(name, conf);
	    userId = conf.get("fs.ceph.rgw.userid", "");
	    accessKey = conf.get("fs.ceph.rgw.access.key", "");
	    secretKey = conf.get("fs.ceph.rgw.secret.key", "");
	    virtualBlockSize = conf.getLong("fs.ceph.rgw.virtual.blocksize", 
	    		conf.getLong(HdfsClientConfigKeys.DFS_BLOCK_SIZE_KEY, HdfsClientConfigKeys.DFS_BLOCK_SIZE_DEFAULT));
	    if(name == null) {
			name = getDefaultUri(conf);
	    }
	    try {
			rootBucketPath = new URI(name.getScheme(), name.getAuthority(), "", "");
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException(e);
		}
	    setWorkingDirectory(new Path("/"));
	    bufferSize = conf.getInt("fs.ceph.rgw.io.buffer.size", 1024 * 1024 * 16);
		try {
			fs = rgwMount(userId, accessKey, secretKey);
		} catch (CephRgwException e) {
			close();
			throw new IOException(String.format("Mount uid:%s, access:%s,secret:%s failed.", 
					userId, accessKey, secretKey), e);
		} 
	}

	@Override
	public URI getUri() {
		return rootBucketPath;
	}

	@Override
	public FSDataInputStream open(Path path, int bufferSize) throws IOException {
		Path absPath = getAbsPath(path);
		bufferSize = this.bufferSize;
		CephRgwInputStream in = new CephRgwInputStream(absPath);
		long fileSize = in.getFileSize();
		if(fileSize <= 0){
			return new FSDataInputStream(in);
		}
		int maxIntFileSize = (int) Math.min(fileSize, Integer.MAX_VALUE);
		bufferSize = Math.min(maxIntFileSize, bufferSize);
		return new FSDataInputStream(new BufferedFSInputStream(in, bufferSize));
	}

	@Override
	public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize,
			short replication, long blockSize, Progressable progress) throws IOException {	
		Path absPath = getAbsPath(path);
		Path parent = absPath.getParent();
		if(parent != null){
			mkdirs(parent);
		}
		return createNonRecursive(absPath, permission, overwrite, bufferSize, replication, blockSize, progress);
	}
	
	public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
	        EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
	        Progressable progress) throws IOException {
		bufferSize = this.bufferSize;
		Path absPath = getAbsPath(f);
		Path parent = absPath.getParent();
		if(parent != null){
			mkdirs(parent);
		}
		CephRgwOutputStream cos = new CephRgwOutputStream(absPath);
		try {
			cos.write(empty, 0, 0);
		} catch (IOException e) {
			cos.close();
			throw e;
		}
		FSDataOutputStream ret = new FSDataOutputStream(new BufferedOutputStream(cos, bufferSize), statistics);
		return ret;
	}

	@Override
	public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
		throw new RuntimeException("Not implement yet.");
	}

	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		Path absPathSrc = getAbsPath(src);
		Path absPathDst = getAbsPath(dst);
		FileStatus srcStatus = getFileStatus(absPathSrc);
		if(srcStatus.isDirectory()){
			throw new IllegalArgumentException("Path rename not supported.");
		}
		FileStatus dstStatus;
		try{
			dstStatus = getFileStatus(absPathDst);
		} catch (FileNotFoundException e) {
			return doRename(absPathSrc, absPathDst);
		}
		if(!dstStatus.isDirectory()){
			throw new PathIsNotDirectoryException(absPathDst.toString());
		}
		return doRename(absPathSrc, new Path(absPathDst, absPathSrc.getName()));
	}

	@Override
	public boolean delete(Path path, boolean recursive) throws IOException {	
		Path absPath = getAbsPath(path);
		if(absPath.isRoot()) throw new IOException("Invalid delete path:" + absPath);
		Path parent = absPath.getParent();
		try (LibRGWFH parentFh = getFileHandleByAbsPath(parent, false)){
			doDelete(absPath, parentFh, recursive);
			return true;
		} catch (FileNotFoundException e){
			return true;
		}
	}

	@Override
	public FileStatus[] listStatus(Path p) throws IOException {
		try (SubPathHandleNames subPaths = getSubPaths(p)){
			LinkedList<FileStatus> ret = new LinkedList<FileStatus>();
			for(Path path:subPaths.subPaths){
				try (LibRGWFH fileFh = new LibRGWFH(rgwLookup(fs, subPaths.pathHandle.getFhPtr(), path.getName(), false))){
					ret.add(fileFh.getFileStatus(path));
				} catch (CephRgwException e) {
					throw new IOException(String.format("Get file status for %s failed.", path), e);
				} catch (FileNotFoundException e){}
			}
			return ret.toArray(new FileStatus[ret.size()]);
		}
	}

	@Override
	public void setWorkingDirectory(Path new_dir) {
		rootDirectory = getAbsPath(new_dir);
	}

	@Override
	public Path getWorkingDirectory() {
		return rootDirectory;
	}

	@Override
	public boolean mkdirs(Path path, FsPermission permission) throws IOException {
		Path absPath = getAbsPath(path);
		Path parent = absPath.getParent();
		if(parent == null){
			return true;
		}
		if(exists(absPath)) {
			if(getFileStatus(absPath).isFile()){
				throw new FileExistsException(absPath.toString());
			} else {
				return true;
			}
		}
		boolean ret = mkdirs(parent, permission);
		if(!ret) return false;
		try (LibRGWFH fh = getFileHandleByAbsPath(parent, true)) {
			rgwMkdir(fs, fh.getFhPtr(), absPath.getName(), permission.toShort());
			return true;
		} catch (CephRgwException e) {
			if(e.getErrcode() == ERR_EXISTS) {
				try(LibRGWFH fh2 = getFileHandleByAbsPath(absPath, false)){
					if(fh2.getFileStatus(absPath).isDirectory()){
						return true;
					} else {
						throw new PathExistsException(absPath.toUri().getPath());
					}
				}
			}
			throw new IOException(String.format("Mkdir %s failed.", path),e);
		} 	
	}

	@Override
	public FileStatus getFileStatus(Path path) throws IOException {
		Path absPath = getAbsPath(path);
		try (LibRGWFH fh = getFileHandleByAbsPath(absPath, false)){
			return fh.getFileStatus(absPath);
		}
	}

	@Override
	public void close() throws IOException{
		rgwUmount(fs);
		super.close();
	}

	private static boolean isDir(int mode){
		return (mode & FLAG_DIR) != 0;
	}

	private LibRGWFH getFileHandleByAbsPath(Path path, boolean createWhenAbsent) throws IOException {
		String cephPathStr = getCephPathStr(path);
		LibRGWFH ret_fh = new LibRGWFH(getRootFH(fs));
		String[] pathArr = cephPathStr.substring(1).split("/");
		for(int i = 0;i < pathArr.length;i++){
			String pathName = pathArr[i];
			try(LibRGWFH lastParent = ret_fh){
				ret_fh = new LibRGWFH(rgwLookup(fs, ret_fh.getFhPtr(), pathName, createWhenAbsent));
			} catch (CephRgwException e) {
				StringBuilder sb = new StringBuilder();
				for(int j = 0;j <= i;j++){
					sb.append("/").append(pathArr[j]);
				}
				if(e.getErrcode() == ERR_NOT_EXISTS){
					throw new FileNotFoundException(sb.toString());
				}
				throw new IOException("Find path " + sb.toString() + " failed.", e);
			} 
		}
		return ret_fh;
	}

	private boolean doRename(Path src, Path dst) throws IOException {
		Path absSrcPath = src.getParent();
		Path absDstPath = dst.getParent();
		if(absSrcPath == null || absDstPath == null) {
			return false;
		}
		try (LibRGWFH srcFh = getFileHandleByAbsPath(absSrcPath, false)){
			try (LibRGWFH dstFh = getFileHandleByAbsPath(absDstPath, true)) {
				rgwRename(fs, srcFh.getFhPtr(), src.getName(), dstFh.getFhPtr(), dst.getName());
				return true;
			} catch (CephRgwException e) {
				if(e.getErrcode() == ERR_NOT_EXISTS){
					throw new FileNotFoundException(String.format("src:%s, dst:%s", src, dst));
				}
				if(e.getErrcode() == ERR_EXISTS){
					throw new PathExistsException(String.format("src:%s, dst:%s", src, dst));
				}
				throw new IOException(String.format("rename src:%s, dst:%s failed.", src, dst), e);
			}
		} 
	}

	private void doDelete(Path path, LibRGWFH parentFh, boolean recursive) throws IOException {
		try (SubPathHandleNames subPaths = getSubPaths(path, new LibRGWFH(
				rgwLookup(fs, parentFh.getFhPtr(), path.getName(), false)))) {
			if(recursive){
				for(Path subPath:subPaths.subPaths){
					try {
						doDelete(subPath, subPaths.pathHandle, true);
					} catch (FileNotFoundException e){}
				} 
			} else if(subPaths.subPaths.size() > 0) {
				throw new PathIsNotEmptyDirectoryException(path.toString());
			}
			rgwUnlink(fs, parentFh.getFhPtr(), path.getName());
		} catch (CephRgwException e) {
			if(e.getErrcode() == ERR_NOT_EXISTS){
				throw new FileNotFoundException(path.toString());
			}
			if(e.getErrcode() == ERR_DIR_NOT_EMPTY) {
				throw new DirectoryNotEmptyException(path.toUri().getPath());
			}
			throw new IOException(String.format("delete path %s failed.", path), e);
		} 
	}

	private SubPathHandleNames getSubPaths(Path path) throws IOException {
		return getSubPaths(path, getFileHandleByAbsPath(path, false));
	}

	private SubPathHandleNames getSubPaths(Path path, LibRGWFH fh) throws IOException {
		HashSet<String> nameSet = new HashSet<String>();
		rgwReaddir(fs, fh.getFhPtr(), nameSet);
		HashSet<Path> subPaths = new HashSet<Path>();
		for(String subName:nameSet){
			subPaths.add(new Path(path, subName));
		}
		return new SubPathHandleNames(fh, subPaths);
	}

	private static void sleepForNative(long time){
		if(time > 0){
			try {
				Thread.sleep(time);
			} catch (InterruptedException e) {}
		}
	}

	private static void throwRgwExceptionForNative(int errcode, String msg) throws CephRgwException{
		throw new CephRgwException(errcode, msg);
	}

	private static class LinuxStat {
		long size;
		int mode;
		int mtime;
		int atime;
		int uid;
		int gid;
	}

	private class LibRGWFH implements Closeable {
		private long fhPtrLong;
		private LinuxStat stat = null;

		LibRGWFH(long fhPtr) {
			this.fhPtrLong = fhPtr;
		}

		@Override
		public void close() throws IOException {
			rgwClose(fs, fhPtrLong);
			fhPtrLong = 0;
		}

		boolean isAttrLoaded(){
			return stat != null;
		}

		FileStatus getFileStatus(Path p) throws IOException{
			if(!isAttrLoaded()){
				try {
					loadAttr();
				} catch (CephRgwException e) {
					if(e.getErrcode() == ERR_NOT_EXISTS){
						throw new FileNotFoundException(p.toString());
					}
					throw new IOException(String.format("Get file status for %s failed.", p), e);
				}
			}
			return new FileStatus(stat.size, isDir(stat.mode), 0, Math.min(virtualBlockSize, stat.size), 
					stat.mtime * 1000L, stat.atime * 1000L, new FsPermission(stat.mode), getUser(stat.uid), 
					getGroup(stat.gid), null, p, false, true, true);
		}

		long getFhPtr() throws IOException{
			checkClosed();
			return fhPtrLong;
		}

		void loadAttr() throws IOException, CephRgwException {
			checkClosed();
			LinuxStat tmp = stat == null?new LinuxStat():stat;
			rgwGetattr(fs, fhPtrLong, tmp);
			stat = tmp;
		}

		private void checkClosed() throws IOException{
			if(fhPtrLong == 0) {
				throw new IOException("File handler is closed.");
			}
		}
	}

	private static class SubPathHandleNames implements Closeable{
		LibRGWFH pathHandle;
		HashSet<Path> subPaths;
		SubPathHandleNames(LibRGWFH handle, HashSet<Path> subPaths) {
			this.pathHandle = handle;
			this.subPaths = subPaths;
		}
		@Override
		public void close() throws IOException {
			if(pathHandle != null){
				pathHandle.close();
			}
		}
	}

	private Path getAbsPath(Path path) {
		return makeQualified(path);
	}

	private String clearEndSeperator(String src){
		StringBuilder sb = new StringBuilder(src);
		while(sb.charAt(sb.length() - 1) == '/'){
			sb.deleteCharAt(sb.length() - 1);
		}
		return sb.toString();
	}

	private String getCephPathStr(Path absPath){
		String pathStr = "/" + rootBucketPath.getAuthority() + Path.getPathWithoutSchemeAndAuthority(absPath).toString();
		pathStr = clearEndSeperator(pathStr);
		return pathStr.replaceAll("/+", "/");
	}

	private class CephRgwInputStream extends FSInputStream {
		private LibRGWFH fhPtr;
		private long position = 0;
		private final long fileSize;

		CephRgwInputStream(Path path) throws IOException {
			fhPtr = getFileHandleByAbsPath(path, false);
			this.fileSize = fhPtr.getFileStatus(path).getLen();
		}

		@Override
		public void seek(long pos) throws IOException {
			if(pos > fileSize) pos = fileSize;
			if(pos < 0) pos = 0;
			this.position = pos;
		}

		@Override
		public long getPos() throws IOException {
			return position;
		}

		long getFileSize(){
			return fileSize;
		}

		@Override
		public boolean seekToNewSource(long targetPos) throws IOException {
			return false;
		}

		@Override
		public int read() throws IOException {
			byte[] b = new byte[1];
			int ret;
			do{
				ret = read(b, 0, 1);
			} while(ret == 0);
			return ret < 0?ret:(int)(b[0]) & 0xff;
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			int ret = read(position, b, off, len);
			if(ret >= 0){
				position += ret;
			}
			return ret;
		}

		@Override
		public void close() throws IOException{
			fhPtr.close();
			super.close();
		}

		@Override
		public int read(long position, byte[] buffer, int offset, int length) throws IOException {
			if(position >= fileSize) return -1;
			try {
				int ret = rgwRead(fs, fhPtr.getFhPtr(), position, (int)Math.min(fileSize - position, length), buffer, offset);
				return ret;
			} catch (CephRgwException e) {
				throw new IOException(String.format("read file from position:%d, length:%d failed.", 
						position, length), e);
			}
		}
	}

	private class CephRgwOutputStream extends OutputStream {
		private long currPos = 0;
		private LibRGWFH fhPtr;

		CephRgwOutputStream(Path path) throws IOException {
			fhPtr = getFileHandleByAbsPath(path, true);
			try {
				rgwOpen(fs, fhPtr.getFhPtr());
			} catch (CephRgwException e) {
				fhPtr.close();
				throw new IOException(String.format("open file %s failed.", path), e);
			}
		}

		@Override
		public void write(int b) throws IOException {
			byte[] buf = new byte[1];
			buf[0] = (byte) b;
			write(buf, 0, 1);
		}

		@Override
		public void write(byte[] buf, int off, int len) throws IOException {
			try {
				rgwWrite(fs, fhPtr.getFhPtr(), currPos, len, buf, off);
				currPos += len;
			} catch (CephRgwException e) {
				throw new IOException(String.format("write file to position:%d, length:%d failed.", 
						currPos, len), e);
			}
		}

		public void close() throws IOException {
			fhPtr.close();
			super.close();
		}
	}

	private static native void staticInit(Class<LinuxStat> linuxStatClass, 
			@SuppressWarnings("rawtypes") Class<HashSet> hashSetClass);

	private native void rgwUmount(long fs);

	private native int rgwRead(long fs, long fh, long position, int length, byte[] buffer, int offset) throws CephRgwException;

	private native void rgwWrite(long fs, long fh, long position, int length, byte[] buffer, int offset) throws CephRgwException;

	private native long rgwMount(String userId, String accessKey, String secretKey) throws CephRgwException;

	private native void rgwOpen(long fs, long fh) throws CephRgwException;

	private native void rgwClose(long fs, long fh);

	private native long rgwLookup(long fs, long parent_fh, String pathName, boolean createWhenAbsent) throws CephRgwException;

	private native long getRootFH(long fs);

	private native void rgwRename(long fs, long srcFh, String srcName, long dstFh, String dstName) throws CephRgwException;

	private native void rgwUnlink(long fs, long fh, String name) throws CephRgwException;

	private native void rgwGetattr(long fs, long fh, LinuxStat stat) throws CephRgwException;

	private native void rgwReaddir(long fs, long fh, HashSet<String> pathSet);

	private native void rgwMkdir(long fs, long fh, String name, int mode) throws CephRgwException;

	private native String getUser(int uid);

	private native String getGroup(int gid);
}

