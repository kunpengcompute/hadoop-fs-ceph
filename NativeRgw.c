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
#include "jni.h"
#include "stdio.h"
#include "stdlib.h"
#include "rgw_file.h"
#include "librgw.h"
#include "string.h"
#include <sys/stat.h>
#include "unistd.h"
#include <dirent.h>
#include <pwd.h>
#include <grp.h>
#include <stdarg.h>
#include <pthread.h>
#include <signal.h>
#include <unistd.h>
#define THROW(env, exception_name, message) \
  { \
        jclass ecls = (*env)->FindClass(env, exception_name); \
        if (ecls) { \
          (*env)->ThrowNew(env, ecls, message); \
          (*env)->DeleteLocalRef(env, ecls); \
        } \
  }

struct sigaction segv,abrt,bus,kil,ill,fpe,xcpu,xfsz,sys;
typedef struct rgw_file_handle *fh_ptr;
typedef struct rgw_fs *fs_ptr;
bool debug = false;
const char * root = "/";
jclass fsclass = NULL;
jmethodID exception_call_method = NULL;
jmethodID sleep_method = NULL;
jmethodID add_set_method = NULL;
jfieldID size_fd = NULL;
jfieldID mode_fd = NULL;
jfieldID mtime_fd = NULL;
jfieldID atime_fd = NULL;
jfieldID uid_fd = NULL;
jfieldID gid_fd = NULL;
librgw_t rgw_h = NULL;
char * nullStr = "NULL";
jfieldID GetFieldAndThrow(JNIEnv *env, jclass clazz, const char * name, const char * sig)
{
   char errMsg[256];
   jfieldID ret = (*env)->GetFieldID(env, clazz, name, sig);
   if (ret == NULL) {
      sprintf(errMsg, "Field %s not found", name);
      THROW(env, "java/lang/NoSuchFieldError", errMsg);
   }
}
jmethodID GetMethodAndThrow(JNIEnv *env, jclass clazz, const char * name, const char * sig)
{
   char errMsg[256];
   jmethodID ret = (*env)->GetMethodID(env, clazz, name, sig);
   if (ret == NULL) {
      sprintf(errMsg, "Method %s not found", name);
      THROW(env, "java/lang/NoSuchMethodError", errMsg);
   }
}
jmethodID GetStaticMethodAndThrow(JNIEnv *env, jclass clazz, const char * name, const char * sig)
{
   char errMsg[256];
   jmethodID ret = (*env)->GetStaticMethodID(env, clazz, name, sig);
   if (ret == NULL) {
      sprintf(errMsg, "Method %s not found", name);
      THROW(env, "java/lang/NoSuchMethodError", errMsg);
   }
}
bool CallCephRgwException(JNIEnv *env, const char * funcName, int errcode, const char * paramFmt, ...)
{
   if(errcode != 0)
   {
      char tmp[1024];
      const char * errstr = strerror(-errcode);
      int cpylen = strlen(strcpy(tmp, errstr));
      tmp[cpylen] = ';';
      va_list ap;
      va_start(ap, paramFmt);
      vsprintf(tmp + cpylen + 1, paramFmt, ap);
      (*env)->CallStaticVoidMethod(env, fsclass, exception_call_method, (jint)errcode, (*env)->NewStringUTF(env, tmp));
      va_end(ap);
   }
   return errcode != 0;
}
JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_s3a_CephRgwFileSystem_staticInit
  (JNIEnv *env, jclass oriclass, jclass linuxStatClass, jclass hashsetClass)
{
   fsclass = oriclass;
   int errcode = librgw_create(&rgw_h, 1, &nullStr);
   if(CallCephRgwException(env, "librgw_create", errcode, "")) return;
   exception_call_method = GetStaticMethodAndThrow(env, oriclass, "throwRgwExceptionForNative", "(ILjava/lang/String;)V");
   sleep_method = GetStaticMethodAndThrow(env, oriclass, "sleepForNative", "(J)V");
   add_set_method = GetMethodAndThrow(env, hashsetClass, "add", "(Ljava/lang/Object;)Z");
   size_fd = GetFieldAndThrow(env, linuxStatClass, "size", "J");
   mode_fd = GetFieldAndThrow(env, linuxStatClass, "mode", "I");
   mtime_fd = GetFieldAndThrow(env, linuxStatClass, "mtime", "I");
   atime_fd = GetFieldAndThrow(env, linuxStatClass, "atime", "I");
   uid_fd = GetFieldAndThrow(env, linuxStatClass, "uid", "I");
   gid_fd = GetFieldAndThrow(env, linuxStatClass, "gid", "I");
}
JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_s3a_CephRgwFileSystem_rgwUmount
  (JNIEnv *env, jobject thiz, jlong fs)
{
   if(fs != 0) rgw_umount((fs_ptr)fs, RGW_UMOUNT_FLAG_NONE);
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_s3a_CephRgwFileSystem_rgwRead
  (JNIEnv *env, jobject thiz, jlong fs, jlong fh, jlong pos, jint len, jbyteArray buf, jint off)
{
   size_t ret = -1;
   jbyte * buffer = (*env)->GetByteArrayElements(env, buf, NULL);
   int errcode = rgw_read((fs_ptr)fs, (fh_ptr)fh, pos, len, &ret, buffer+off, RGW_READ_FLAG_NONE);
   (*env)->ReleaseByteArrayElements(env, buf, buffer, JNI_COMMIT);
   free(buffer);
   if(CallCephRgwException(env, "rgw_read", errcode, "fh_read=%d;pos=%d;len=%d;offset=%d", fh, pos, len, off)) return ret;
   return ret;
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_s3a_CephRgwFileSystem_rgwWrite
  (JNIEnv *env, jobject thiz, jlong fs, jlong fh, jlong pos, jint len, jbyteArray buf, jint off)
{
   size_t ret = -1;
   jbyte * buffer = (*env)->GetByteArrayElements(env, buf, NULL);
   int errcode = rgw_write((fs_ptr)fs, (fh_ptr)fh, pos, len, &ret, buffer+off, RGW_WRITE_FLAG_NONE);
   free(buffer);
   CallCephRgwException(env, "rgw_write", errcode, "fh_write=%d;pos=%d;len=%d;offset=%d", fh, pos, len, off);
}

JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_s3a_CephRgwFileSystem_rgwMount
  (JNIEnv *env, jobject thiz, jstring uid, jstring accessKey, jstring secretKey)
{
   fs_ptr fs;
   const char * uid_cstr = (*env)->GetStringUTFChars(env, uid, NULL);
   const char * accessKey_cstr = (*env)->GetStringUTFChars(env, accessKey, NULL);
   const char * secretKey_cstr = (*env)->GetStringUTFChars(env, secretKey, NULL);
   int errcode = rgw_mount2(rgw_h, uid_cstr, accessKey_cstr, secretKey_cstr, root, &fs, RGW_MOUNT_FLAG_NONE);
   CallCephRgwException(env, "rgw_mount2", errcode, "uid=%s;accessKey=%s;secretKey=%s", uid_cstr, accessKey_cstr, secretKey_cstr);
   (*env)->ReleaseStringUTFChars(env, uid, uid_cstr);
   (*env)->ReleaseStringUTFChars(env, accessKey, accessKey_cstr);
   (*env)->ReleaseStringUTFChars(env, secretKey, secretKey_cstr);
   return (jlong)fs;
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_s3a_CephRgwFileSystem_rgwOpen
  (JNIEnv *env, jobject thiz, jlong fs, jlong fh)
{
   CallCephRgwException(env, "rgw_open",rgw_open((fs_ptr)fs, (fh_ptr)fh, 0, 0), "fh_open=%d", fh);
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_s3a_CephRgwFileSystem_rgwClose
  (JNIEnv *env, jobject thiz, jlong fs, jlong fh)
{
   fh_ptr fhptr = (fh_ptr) fh;
   fs_ptr fsptr = (fs_ptr) fs;
   if(fhptr != NULL && fhptr != fsptr->root_fh)
   {
      rgw_close(fsptr, fhptr, RGW_CLOSE_FLAG_RELE);
   }
}

JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_s3a_CephRgwFileSystem_rgwLookup
  (JNIEnv *env, jobject thiz, jlong fs, jlong fh, jstring name, jboolean create)
{
   fh_ptr ret_fh = NULL;
   const char * name_cstr = (*env)->GetStringUTFChars(env, name, NULL);
   CallCephRgwException(env, "rgw_lookup", rgw_lookup((fs_ptr)fs, (fh_ptr)fh, name_cstr, &ret_fh, NULL, 0, create?RGW_LOOKUP_FLAG_CREATE:RGW_LOOKUP_FLAG_NONE), "fh_parent=%d;name=%s;create=%d", fh, name_cstr, create?RGW_LOOKUP_FLAG_CREATE:RGW_LOOKUP_FLAG_NONE);
   (*env)->ReleaseStringUTFChars(env, name, name_cstr);
   return (jlong) ret_fh;
}

JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_s3a_CephRgwFileSystem_getRootFH
  (JNIEnv *env, jobject thiz, jlong fs)
{
   fs_ptr fsptr = (fs_ptr) fs;
   return (jlong)(fsptr->root_fh);
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_s3a_CephRgwFileSystem_rgwRename
  (JNIEnv * env, jobject thiz, jlong fs, jlong fh_src, jstring srcName, jlong fh_dst, jstring dstName)
{
   const char * srcName_cstr = (*env)->GetStringUTFChars(env, srcName, NULL);
   const char * dstName_cstr = (*env)->GetStringUTFChars(env, dstName, NULL);
   CallCephRgwException(env, "rgw_rename", rgw_rename((fs_ptr)fs, (fh_ptr)fh_src, srcName_cstr, (fh_ptr)fh_dst, dstName_cstr, RGW_RENAME_FLAG_NONE), "fh_src=%d;fh_dst=%d;srcName=%s;dstName=%s", fh_src, fh_dst, srcName_cstr, dstName_cstr);
   (*env)->ReleaseStringUTFChars(env, srcName, srcName_cstr);
   (*env)->ReleaseStringUTFChars(env, dstName, dstName_cstr);
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_s3a_CephRgwFileSystem_rgwUnlink
  (JNIEnv *env, jobject thiz, jlong fs, jlong fh, jstring name)
{
   const char * name_cstr = (*env)->GetStringUTFChars(env, name, NULL);
   CallCephRgwException(env, "rgw_unlink", rgw_unlink((fs_ptr)fs, (fh_ptr)fh, name_cstr, RGW_UNLINK_FLAG_NONE), "fh_unlink=%d;name=%s", fh, name_cstr);
   (*env)->ReleaseStringUTFChars(env, name, name_cstr);
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_s3a_CephRgwFileSystem_rgwGetattr
  (JNIEnv * env, jobject thiz, jlong fs, jlong fh, jobject stat)
{
   struct stat st;
   int errcode = rgw_getattr((fs_ptr)fs, (fh_ptr)fh, &st, RGW_GETATTR_FLAG_NONE);
   (*env)->SetLongField(env, stat, size_fd, st.st_size);
   (*env)->SetIntField(env, stat, mode_fd, st.st_mode);
   (*env)->SetIntField(env, stat, uid_fd, st.st_uid);
   (*env)->SetIntField(env, stat, gid_fd, st.st_gid);
   (*env)->SetIntField(env, stat, atime_fd, st.st_atime);
   (*env)->SetIntField(env, stat, mtime_fd, st.st_mtime);
}

static bool getSubPathCallback(const char* name, void *arg, uint64_t offset, struct stat *st, uint32_t mask,
                    uint32_t flags) {
   void ** argPtrArr = (void**) arg;
   JNIEnv *env = argPtrArr[0];
   jobject retSet = *((jobject*)(argPtrArr[1]));
   (*env)->CallBooleanMethod(env, retSet, add_set_method, (*env)->NewStringUTF(env, name));
   return true;
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_s3a_CephRgwFileSystem_rgwReaddir
  (JNIEnv *env, jobject thiz, jlong fs, jlong fh, jobject nameSet)
{
   uint64_t offset = 0;
   bool eof = false;
   void *argPtrArr[2];
   argPtrArr[0] = env;
   argPtrArr[1] = &nameSet;
   do{
      if(CallCephRgwException(env, "rgw_readdir", rgw_readdir((fs_ptr)fs, (fh_ptr)fh, &offset, getSubPathCallback, &argPtrArr, &eof, RGW_READDIR_FLAG_NONE), "fh_readdir=%d", fh)) return;
   } while(!eof);
}
JNIEXPORT void JNICALL Java_org_apache_hadoop_fs_s3a_CephRgwFileSystem_rgwMkdir
  (JNIEnv *env, jobject thiz, jlong fs, jlong fh, jstring name, jint mode)
{
   struct stat st;
   const char * name_cstr = (*env)->GetStringUTFChars(env, name, NULL);
   st.st_uid = getuid();
   struct passwd *pwd;
   pwd = getpwuid(st.st_uid);
   st.st_gid = pwd->pw_gid;
   st.st_mode = mode;
   fh_ptr ret;
   CallCephRgwException(env, "rgw_mkdir",rgw_mkdir((fs_ptr)fs, (fh_ptr)fh, name_cstr, &st, RGW_SETATTR_UID | RGW_SETATTR_GID | RGW_SETATTR_MODE, &ret, RGW_MKDIR_FLAG_NONE), "fh_parent=%d;name=%s;uid=%d;gid=%d;mode=%d",
       fh, name_cstr, st.st_uid, st.st_gid, st.st_mode);
   (*env)->ReleaseStringUTFChars(env, name, name_cstr);
}
JNIEXPORT jstring JNICALL Java_org_apache_hadoop_fs_s3a_CephRgwFileSystem_getUser
  (JNIEnv *env, jobject thiz, jint uid)
{
   return (*env)->NewStringUTF(env, "root");
}
JNIEXPORT jstring JNICALL Java_org_apache_hadoop_fs_s3a_CephRgwFileSystem_getGroup
  (JNIEnv *env, jobject thiz, jint gid)
{
   return (*env)->NewStringUTF(env, "root");
}
