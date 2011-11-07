Quest® Data Transporter for Hive
================================

Quest® Data Transporter for Hive is distributed with Quest® Data Connector for Oracle and Hadoop. Quest® Data Transporter for Hive is a Java command-line utility that allows you to execute a Hive query and insert the results into an Oracle table.

Development Prerequisites
-------------------------

To check the source code out you will need [Git](http://git-scm.com/).

You will need to have the following in order to compile the project:

* [Oracle Java SE 6 JDK](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
* [Apache Maven](http://maven.apache.org/)
* [Oracle Database 11g Release 2 (11.2+) JDBC Drivers](http://www.oracle.com/technetwork/database/enterprise-edition/jdbc-112010-090769.html)

You will also need to install the Oracle JDBC driver into your local maven repository with the following command:

	mvn install:install-file -Dfile=ojdbc6.jar -Dpackaging=jar -DgroupId=com.oracle -DartifactId=ojdbc6 -Dversion=11.2.0.3.0

Getting Started
---------------

1. To get a copy of the source code:

		git clone http://github.com/QuestSoftwareTCD/OracleHiveConnector.git

2. To compile and generate jar archive and tarball package:

		cd OracleHiveConnector
		mvn package

Using Eclipse
-------------

1. Generate the Eclipse project with the following:

		mvn eclipse:eclipse

2. Import the project into the Eclipse workspace.