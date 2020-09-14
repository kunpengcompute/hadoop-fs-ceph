# hadoop-fs-ceph


Introduction
============

Using Ceph instead of HDFS as the storage system of Hadoop, it can separates computing and storage resources, and realizes the elastic expansion of resources on demand.



Building And Package
====================
(1) install ceph/java/maven.

(2) Compile under the "hadoop-fs-ceph" :

    mvn package -DskipTests

(3) get "hdfs-ceph-3.1.1.jar" under the "hadoop-fs-ceph/target/"

(4) get "librgw_jni.so" under the "hadoop-fs-ceph/target/"

 

Contact
=======

Hadoop is distributed through GitHub. For the latest version, a bug tracker,
and other information, see

  http://hadoop.apache.org/

or the repository at

  https://github.com/apache/hadoop/tree/release-3.1.1-RC0

