/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 *
 * Description: This is core librgw function code.
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
#include "native_rgw.h"
#include <sys/stat.h>
#include <dirent.h>
#include <pwd.h>
#include <grp.h>
#include <stdarg.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "jni.h"
#include "rgw_file.h"
#include "librgw.h"
#define MS_PER_SEC 1000
#define MOD_FULL 0777
#define DIR_FLAG 0x4000
#define UID_ROOT 0
#define GID_ROOT 0
typedef struct rgw_file_handle *FhPtr;
typedef struct rgw_fs *FsPtr;
const char *g_root = "/";
char *g_nullStr = "NULL";
jclass g_fsclass = NULL;
jmethodID g_exceptionCallMethod = NULL;
jmethodID g_recvFsMethod = NULL;
librgw_t g_rgwMountId = NULL;
void Throw(JNIEnv *env, const char *exceptionName, const char *message)
{
    jclass ecls = (*(env))->FindClass(env, exceptionName);
    if (ecls) {
        (*(env))->ThrowNew(env, ecls, message);
        (*(env))->DeleteLocalRef(env, ecls);
    }
}
jfieldID GetFieldAndThrow(JNIEnv *env, jclass clazz, const char *name, const char *sig)
{
    char errMsg[256];
    jfieldID ret = (*env)->GetFieldID(env, clazz, name, sig);
    if (ret == NULL) {
        snprintf(errMsg, sizeof(errMsg), "Field %s not found", name);
        const char *msg = (const char*)errMsg;
        Throw(env, "java/lang/NoSuchFieldError", msg);
    }
    return ret;
}
jmethodID GetMethodAndThrow(JNIEnv *env, jclass clazz, const char *name, const char *sig)
{
    char errMsg[256];
    jmethodID ret = (*env)->GetMethodID(env, clazz, name, sig);
    if (ret == NULL) {
        snprintf(errMsg, sizeof(errMsg), "Method %s not found", name);
        const char *msg = (const char*)errMsg;
        Throw(env, "java/lang/NoSuchMethodError", msg);
    }
    return ret;
}
jmethodID GetStaticMethodAndThrow(JNIEnv *env, jclass clazz, const char *name, const char *sig)
{
    char errMsg[256];
    jmethodID ret = (*env)->GetStaticMethodID(env, clazz, name, sig);
    if (ret == NULL) {
        snprintf(errMsg, sizeof(errMsg), "Method %s not found", name);
        const char *msg = (const char*)errMsg;
        Throw(env, "java/lang/NoSuchMethodError", msg);
    }
    return ret;
}
bool CallCephRgwException(JNIEnv *env, const char *funcName, int errcode, const char *paramFmt, ...)
{
    if (errcode != 0) {
        char tmp[1024];
        int tmpSize = sizeof(tmp);
        tmp[tmpSize - 1] = '\0';
        va_list ap;
        va_start(ap, paramFmt);
        vsnprintf(tmp, tmpSize - 1, paramFmt, ap);
        const char *msg = (const char*)tmp;
        (*env)->CallStaticVoidMethod(env, g_fsclass, g_exceptionCallMethod, (jint)errcode,
            (*env)->NewStringUTF(env, msg));
        va_end(ap);
    }
    return errcode != 0;
}
JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_cephrgw_CephRgwFileSystem_staticInit(JNIEnv *env, jclass oriclass,
    jclass fsrecv)
{
    g_fsclass = oriclass;
    int errcode = librgw_create(&g_rgwMountId , 1, &g_nullStr);
    if (CallCephRgwException(env, "librgw_create", errcode, "")) {
        return;
    }
    g_exceptionCallMethod = GetStaticMethodAndThrow(env, oriclass, "throwRgwExceptionForNative",
        "(ILjava/lang/String;)V");
    if (!g_exceptionCallMethod) {
        return;
    }
    g_recvFsMethod = GetMethodAndThrow(env, fsrecv, "receiveFileHandler", "(Ljava/lang/String;JI)V");
    if (!g_recvFsMethod) {
        return;
    }
}
JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_cephrgw_CephRgwFileSystem_rgwUmount(JNIEnv *env, jobject thiz, jlong fs)
{
    if (fs != 0) {
        CallCephRgwException(env, "rgw_umount", rgw_umount((FsPtr)fs, RGW_UMOUNT_FLAG_NONE), "");
    }
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_cephrgw_CephRgwFileSystem_rgwRead(JNIEnv *env, jobject thiz, jlong fs,
    jlong fh, jlong pos, jint len, jbyteArray buf, jint off)
{
    size_t ret = -1;
    jboolean iscopy = JNI_FALSE;
    jbyte *buffer = (jbyte*)(*env)->GetByteArrayElements(env, buf, &iscopy);
    int errcode = rgw_read((FsPtr)fs, (FhPtr)fh, pos, len, &ret, buffer + off, RGW_READ_FLAG_NONE);
    (*env)->ReleaseByteArrayElements(env, buf, buffer, JNI_COMMIT);
    if (iscopy) {
        free(buffer);
    }
    CallCephRgwException(env, "rgw_read", errcode, "fh_read=%ld;pos=%ld;len=%d;offset=%d", fh, pos, len, off);
    return ret;
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_cephrgw_CephRgwFileSystem_rgwWrite(JNIEnv *env, jobject thiz, jlong fs,
    jlong fh, jlong pos, jint len, jbyteArray buf, jint off)
{
    size_t ret = -1;
    jboolean iscopy = JNI_FALSE;
    jbyte *buffer = (jbyte*)(*env)->GetByteArrayElements(env, buf, &iscopy);
    jlong remaining = len;
    int errcode = 0;
    while (remaining > 0) {
        int bias = len - remaining;
        errcode = rgw_write((FsPtr)fs, (FhPtr)fh, pos + bias, remaining, &ret, buffer + off + bias,
            RGW_WRITE_FLAG_NONE);
        if (errcode != 0) {
            break;
        }
        remaining -= ret;
    } 
    if (iscopy) {
        free(buffer);
    }
    CallCephRgwException(env, "rgw_write", errcode, "fh_write=%ld;pos=%ld;len=%d;offset=%d", fh, pos, len, off);
}

JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_cephrgw_CephRgwFileSystem_rgwMount(JNIEnv *env, jobject thiz,
    jstring uid, jstring accessKey, jstring secretKey)
{
    FsPtr fs = NULL;
    const char *uid_cstr = (*env)->GetStringUTFChars(env, uid, NULL);
    const char *accessKey_cstr = (*env)->GetStringUTFChars(env, accessKey, NULL);
    const char *secretKey_cstr = (*env)->GetStringUTFChars(env, secretKey, NULL);
    int errcode = rgw_mount2(g_rgwMountId , uid_cstr, accessKey_cstr, secretKey_cstr, g_root, &fs, RGW_MOUNT_FLAG_NONE);
    CallCephRgwException(env, "rgw_mount2", errcode, "");
    (*env)->ReleaseStringUTFChars(env, uid, uid_cstr);
    (*env)->ReleaseStringUTFChars(env, accessKey, accessKey_cstr);
    (*env)->ReleaseStringUTFChars(env, secretKey, secretKey_cstr);
    return (jlong)fs;
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_cephrgw_CephRgwFileSystem_rgwOpen(JNIEnv *env, jobject thiz, jlong fs,
    jlong fh)
{
    CallCephRgwException(env, "rgw_open", rgw_open((FsPtr)fs, (FhPtr)fh, 0, 0), "fh_open=%ld", fh);
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_cephrgw_CephRgwFileSystem_rgwClose(JNIEnv *env, jobject thiz, jlong fs,
    jlong fh)
{
    FhPtr fhptr = (FhPtr) fh;
    FsPtr fsptr = (FsPtr) fs;
	if (fsptr == NULL || fhptr == NULL || fhptr == fsptr->root_fh) {
        return;
    }
    rgw_close(fsptr, fhptr, RGW_CLOSE_FLAG_RELE);
}

JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_cephrgw_CephRgwFileSystem_rgwLookup(JNIEnv *env, jobject thiz, jlong fs,
    jlong fh, jstring name, jlong statPtr, jint mask, jint flag)
{
    FhPtr ret_fh = NULL;
    const char *name_cstr = (*env)->GetStringUTFChars(env, name, NULL);
    CallCephRgwException(env, "rgw_lookup", rgw_lookup((FsPtr)fs, (FhPtr)fh, name_cstr, &ret_fh,
        (struct stat*)statPtr, mask, flag), "fh_parent=%ld;name=%s;flag=%d", fh, name_cstr, flag);
    (*env)->ReleaseStringUTFChars(env, name, name_cstr);
    return (jlong) ret_fh;
}

JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_cephrgw_CephRgwFileSystem_getRootFH(JNIEnv *env, jobject thiz, jlong fs)
{
    FsPtr fsptr = (FsPtr)fs;
    CallCephRgwException(env, "getRootFH", !fsptr, "Exception:fsPtr is null");
    return (jlong)(fsptr->root_fh);
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_cephrgw_CephRgwFileSystem_rgwRename(JNIEnv *env, jobject thiz, jlong fs,
    jlong fh_src, jstring srcName, jlong fh_dst, jstring dstName)
{
    const char *srcName_cstr = (*env)->GetStringUTFChars(env, srcName, NULL);
    const char *dstName_cstr = (*env)->GetStringUTFChars(env, dstName, NULL);
    CallCephRgwException(env, "rgw_rename", rgw_rename((FsPtr)fs, (FhPtr)fh_src, srcName_cstr, (FhPtr)fh_dst,
        dstName_cstr, RGW_RENAME_FLAG_NONE), "fh_src=%ld;fh_dst=%ld;srcName=%s;dstName=%s", fh_src, fh_dst,
        srcName_cstr, dstName_cstr);
    (*env)->ReleaseStringUTFChars(env, srcName, srcName_cstr);
    (*env)->ReleaseStringUTFChars(env, dstName, dstName_cstr);
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_cephrgw_CephRgwFileSystem_rgwUnlink(JNIEnv *env, jobject thiz, jlong fs,
    jlong fh, jstring name)
{
    const char *name_cstr = (*env)->GetStringUTFChars(env, name, NULL);
    CallCephRgwException(env, "rgw_unlink", rgw_unlink((FsPtr)fs, (FhPtr)fh, name_cstr, RGW_UNLINK_FLAG_NONE),
        "fh_unlink=%ld;name=%s", fh, name_cstr);
    (*env)->ReleaseStringUTFChars(env, name, name_cstr);
}

void putFsToRecv(JNIEnv *env, jobject fsrecv, struct stat *st, uint32_t mask, const char *name)
{
    (*env)->CallVoidMethod(env, fsrecv, g_recvFsMethod, (*env)->NewStringUTF(env, name), (jlong)st, (jint)mask);
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_cephrgw_CephRgwFileSystem_rgwGetattr(JNIEnv *env, jobject thiz, jlong fs,
    jlong fh, jobject fsrecv)
{
    struct stat st;
    int errcode = rgw_getattr((FsPtr)fs, (FhPtr)fh, &st, RGW_GETATTR_FLAG_NONE);
    if (CallCephRgwException(env, "rgw_getattr", errcode, "")) {
        return;
    }
    putFsToRecv(env, fsrecv, &st, 0, ""); //to-do 為啥這個地方為0，後面為啥為空。
}
typedef struct _ReaddirArg {
    JNIEnv *env;
    jobject fsrecv;
} ReaddirArg;
static bool GetSubPathCallback(const char* name, void *arg, uint64_t offset, struct stat *st, uint32_t mask,
    uint32_t flags)
{
    ReaddirArg *args = (ReaddirArg*) arg;
    st->st_mode = MOD_FULL;
    if (flags & RGW_LOOKUP_FLAG_DIR) {
        st->st_mode += DIR_FLAG;
    }
    putFsToRecv(args->env, args->fsrecv, st, mask, name);
    return true;
}
JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_cephrgw_CephRgwFileSystem_rgwReaddir(JNIEnv *env, jobject thiz, jlong fs,
    jlong fh, jobject fsrecv)
{
    uint64_t offset = 0;
    bool eof = false;
    ReaddirArg args;
    args.env = env;
    args.fsrecv = fsrecv;
    do {
        if (CallCephRgwException(env, "rgw_readdir", rgw_readdir((FsPtr)fs, (FhPtr)fh, &offset, GetSubPathCallback,
            &args, &eof, RGW_READDIR_FLAG_NONE), "fh_readdir=%ld", fh)) {
            return;
        }
    } while (!eof);
}
JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_cephrgw_CephRgwFileSystem_rgwMkdir(JNIEnv *env, jobject thiz, jlong fs,
    jlong fh, jstring name, jint mode)
{
    struct stat st;
    const char *name_cstr = (*env)->GetStringUTFChars(env, name, NULL);
    st.st_uid = UID_ROOT;
    st.st_gid = GID_ROOT;
    st.st_mode = mode;
    FhPtr ret = NULL;
    CallCephRgwException(env, "rgw_mkdir", rgw_mkdir((FsPtr)fs, (FhPtr)fh, name_cstr, &st, RGW_SETATTR_UID |
        RGW_SETATTR_GID | RGW_SETATTR_MODE, &ret, RGW_MKDIR_FLAG_NONE), "fh_parent=%ld;name=%s;uid=%d;gid=%d;mode=%d",
        fh, name_cstr, st.st_uid, st.st_gid, st.st_mode);
    (*env)->ReleaseStringUTFChars(env, name, name_cstr);
}
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_cephrgw_CephRgwFileSystem_getLength(JNIEnv *env, jobject thiz, jlong ptr)
{
    struct stat *st = (struct stat*) ptr;
    return (jlong) st->st_size;
}
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_cephrgw_CephRgwFileSystem_getAccessTime(JNIEnv *env, jobject thiz,
    jlong ptr)
{
    struct stat *st = (struct stat*) ptr;
    return (jlong) st->st_atime * MS_PER_SEC;
}
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_cephrgw_CephRgwFileSystem_getModifyTime(JNIEnv *env, jobject thiz,
    jlong ptr)
{
    struct stat *st = (struct stat*) ptr;
    return (jlong) st->st_mtime * MS_PER_SEC;
}
JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_cephrgw_CephRgwFileSystem_getMode(JNIEnv *env, jobject thiz, jlong ptr)
{
    struct stat* st = (struct stat*) ptr;
    return (jint) st->st_mode;
}
