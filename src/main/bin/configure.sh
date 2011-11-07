#!/bin/bash

# Copyright 2011 Quest Software, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# NAME
#   configure.sh
#
# SYNOPSIS
#   /bin/sh configure.sh [-h] [-o <output_file>]
#
# DESCRIPTION
#   Configures the Quest® Data Transporter for Hive application for copying data from Hadoop to an Oracle database.
#
# OPTIONS
#   -h                  Print this help page to stdout and exit without running the script.
#
#   -o <output_file>    The name of the file in which this configuration will be stored.  If not specified
#                       on the command line, the file name "defaultOraHive.sh" will be used.
#

###
### Set option defaults
###

CONFIG_FILE="defaultOraHive.sh"

###
### Process command line options
###

set -- `getopt 'o:h' $@`
while [ "$1" ]; do
	case "$1" in
		-o)
			CONFIG_FILE=$2
			shift 2;;
		--)
			shift
			break;;
		*)
			sed -n '17,33s/^#//p' $0
			exit -1;;
	esac
done

###
### Set up the required parameters and verify that we have a viable installation environment
###

# a shorthand road to dusty death
die() { echo "${0}: ERROR: ${1}" >&2; exit ${2:--1}; }

# read a variable from the user or use the default value
readvar() { read -p "$1 (${2:-no default}): "; echo ${REPLY:-$2}; }

echo
echo
echo
echo
echo
echo "*** WARNING ***"
echo
echo "Passwords entered for Hive or Oracle will be stored in PLAIN TEXT"
echo "within the \"${CONFIG_FILE}\" configuration file."
echo
echo "If you do not enter passwords now during this configuration phase,"
echo "you will be interactively prompted for them when Quest® Data Transporter for Hive runs."
echo
echo
echo "Prompts for mandatory configuration items are prefixed with asterisks."
echo "Default values are displayed in parentheses and can be accepted by"
echo "pressing the <ENTER> key."
echo
echo
read -n 1 -p "Press any key to continue... "
echo
echo
echo
echo

echo
echo "*** HADOOP configuration ***"
echo

[ -d "${HADOOP_HOME}" ] && HADOOP_CORE_JAR=`find ${HADOOP_HOME} -name "hadoop-*-core.jar" -print 2>/dev/null | head -1`
HADOOP_CORE_JAR=`readvar "*** Enter the path of the Hadoop core jar file" $HADOOP_CORE_JAR`
[ -f "${HADOOP_CORE_JAR}" ] || die "This Hadoop jar file does not exist: ${HADOOP_CORE_JAR}"

echo
echo "*** HIVE configuration ***"
echo

HIVE_HOME=`readvar "*** Enter the local Hive home directory" $HIVE_HOME`
[ "${HIVE_HOME}" ] || die "The home directory of a local Hive installation is required."
[ -d ${HIVE_HOME} ] || die "This Hive home is not a directory: ${HIVE_HOME}"
[ -x "${HIVE_HOME}/bin/hive" ] || die "This Hive home directory does not contain the \"bin/hive\" executable."

HIVE_HOST=`readvar "Enter the name of the Hive server host" "localhost"`
HIVE_URL=`readvar "Enter the URL to connect to the Hive server" "jdbc:hive://${HIVE_HOST}:10000/default"`
HIVE_USER=`readvar "Enter the name of the Hive user" "root"`
HIVE_PASSWORD=`readvar "Enter the password for the Hive user \"${HIVE_USER}\""`

echo
echo "*** ORACLE configuration ***"
echo

[ -d ${ORACLE_HOME} ] && ORACLE_JDBC_JAR=`find ${ORACLE_HOME} -name "ojdbc6.jar" -print 2>/dev/null | head -1`
ORACLE_JDBC_JAR=`readvar "*** Enter the path of the Oracle JDBC6 jar file" $ORACLE_JDBC_JAR`
[ -f "${ORACLE_JDBC_JAR}" ] || die "This Oracle JDBC jar file does not exist: ${ORACLE_JDBC_JAR}"

ORACLE_HOST=`readvar "Enter the name of the Oracle server host" "localhost"`
ORACLE_SID=`readvar "Enter the name of the target Oracle SID" "ORCL"`
ORACLE_URL=`readvar "Enter the URL to connect to the Oracle server" "jdbc:oracle:thin:@${ORACLE_HOST}:1521:${ORACLE_SID}"`
ORACLE_USER=`readvar "Enter the name of the Oracle user" "oracle"`
ORACLE_PASSWORD=`readvar "Enter the password for the Oracle user \"${ORACLE_USER}\""`

echo
echo "*** Target Oracle table configuration ***"
echo

ORACLE_TABLE=`readvar "Enter the name of the target Oracle table" "HIVE_RESULTS"`
INSERT_BATCH_SIZE=`readvar "Enter the number of rows for each batch insert" "500"`
COMMIT_BATCH_COUNT=`readvar "Enter the number of inserts per commit" "20"`

# Create the default profile for this installation, which can be changed or copied
# by the user
{
	echo "#!/bin/sh"
	echo
	echo "# turn on automatic exporting of environment variables"
	echo "set -a"
	echo
	echo "# set the location of the Hadoop core jar file"
	echo "HADOOP_CORE_JAR='${HADOOP_CORE_JAR}'"
	echo
	echo "# set the location of the local Hive home directory and server connection parameters"
	echo "HIVE_HOME='${HIVE_HOME}'"
	echo "HIVE_URL='${HIVE_URL}'"
	echo "HIVE_USER='${HIVE_USER}'"
	echo "HIVE_PASSWORD='${HIVE_PASSWORD}'"
	echo
	echo "# set the location of the local Oracle JDBC jar and connection data for the target instance"
	echo "ORACLE_JDBC_JAR='${ORACLE_JDBC_JAR}'"
	echo "ORACLE_URL='${ORACLE_URL}'"
	echo "ORACLE_USER='${ORACLE_USER}'"
	echo "ORACLE_PASSWORD='${ORACLE_PASSWORD}'"
	echo
	echo "# specify the target Oracle table and user-configurable performance settings"
	echo "ORACLE_TABLE='${ORACLE_TABLE}'"
	echo "INSERT_BATCH_SIZE='${INSERT_BATCH_SIZE}'"
	echo "COMMIT_BATCH_COUNT='${COMMIT_BATCH_COUNT}'"
	echo
	echo "# turn off automatic exporting"
	echo "set +a"
	echo
} > ${CONFIG_FILE}

# ensure it's executable
chmod a+x ${CONFIG_FILE}

echo
echo
echo "*** Configuration saved to file: ${CONFIG_FILE} ***"
echo
