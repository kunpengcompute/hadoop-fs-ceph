# hadoop-fs-ceph


Introduction
============

Using Ceph instead of HDFS as the storage system of Hadoop separates computing and storage resources, and realizes on-demand, elastic resource expansion.



Building 
====================
(1) Install Ceph, Java and Maven.

(2) Build this module using the following command in "hadoop-fs-ceph/" folder.

    mvn package -DskipTests

(3) Get "hdfs-ceph-3.1.1.jar" from the "hadoop-fs-ceph/target/" folder.

(4) Get "librgw_jni.so" from the "hadoop-fs-ceph/target/" folder.

 

Contact
=======

Hadoop is distributed through GitHub. For the latest version, a bug tracker,
and other information, see

  http://hadoop.apache.org/

or the repository at

  https://github.com/apache/hadoop/tree/release-3.1.1-RC0

