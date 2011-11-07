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
#   orahive.sh
#
# SYNOPSIS
#   /bin/sh orahive.sh [-v] [-h] [-c <config_file>] [-t <table_name>] [-q <hql_file> | <hql_statement>]
#
# DESCRIPTION
#   Execute the given Quest® Data Transporter for Hive script.
#
# OPTIONS
#   -v                  Verbose mode that prints progress and context information to stdout.
#
#   -h                  Print this help page to stdout and exit without running the script.
#
#   -c <config_file>    The configuration file from which Quest® Data Transporter for Hive will set the environment for this run.
#                       If not specified on the command line then configuration will be read from the file
#                       "defaultOraHive.sh" in the current directory. 
#
#   -t <table_name>     Overrides the name of the target Oracle table that was specified in the configuration
#                       file.  This parameter is provided for convenience so that different tables can be 
#                       populated without constantly changing the configuration.
#
#   -e <export_mode>    Export mode to run. Either create or insert. By default will run create which will
#                       create a new table. Insert will insert into an existing table.
#
#   -q <hql_file>       The path of a Hive Query Language script file to execute. If this is parameter is
#                       not specified on the command line then it defaults to "hql.txt" in the current
#                       directory.  This parameter is ignored if <hql_statement> is specified.
#
#   <hql_statement>     A Hive Query Language statement to execute. If this is parameter is provided then
#                       any hql file specified with the "-q" option will be ignored, and this statement.
#                       will be executed instead.
#

# a shorthand road to dusty death
die() { echo "${0}: ERROR: ${1}" >&2; exit ${2:--1}; }

# check the version of Java that we have is at least 1.6
[ "`java -version 2>&1 | grep 'java version "1\.6'`" ] || die "Quest® Data Transporter for Hive requires java version 1.6"

###
### Set option defaults
###

CONFIG_FILE="defaultOraHive.sh"
HQL_FILE="hql.txt"
unset OVERIDE_TABLE_NAME

###
### Process command line options
###

set -f
set -- `getopt "c:q:t:e:vh" "$@"`
set +f
while [ "$1" ]; do
	case "$1" in
		-c)
			CONFIG_FILE=$2
			shift 2;;
		-q)
			HQL_FILE=$2
			shift 2;;
		-t)
			OVERIDE_TABLE_NAME=$2
			shift 2;;
		-e)
			EXPORT_MODE=$2
			shift 2;;
		-v)
			VERBOSE="true"
			shift;;
		--)
			shift
			break;;
		*)
			sed -n '17,51s/^#//p' $0
			exit -1;;
	esac
done

if [ "$*" ]
then
	TEMP_HQL_FILE="/tmp/oraHive.$$"
	echo "$*" > ${TEMP_HQL_FILE}
	HQL_FILE=${TEMP_HQL_FILE}
fi
		
# HQL to execute
[ -r "${HQL_FILE}" ] || die "Hive QL file not found: ${HQL_FILE}" 2

# source the specified configuration file
[ -r "${CONFIG_FILE}" ] || die "Quest® Data Transporter for Hive configuration file not found: ${CONFIG_FILE}" 2
[ "${VERBOSE}" ] && echo "loading configuration script: ${CONFIG_FILE}"
. ${CONFIG_FILE}

###
### Validate the parameters passed to Quest® Data Transporter for Hive
###

# Connection details for Hive
[ "${HIVE_URL}" ] || die "HIVE_URL must be set in Quest® Data Transporter for Hive configuration or the environment"
[ "${HIVE_USER}" ] || die "HIVE_USER must be set in Quest® Data Transporter for Hive configuration or the environment"
[ -z "${HIVE_PASSWORD}" ] && unset HIVE_PASSWORD

# Connection details for Oracle
[ "${ORACLE_URL}" ] || die "ORACLE_URL must be set in Quest® Data Transporter for Hive configuration or the environment"
[ "${ORACLE_USER}" ] || die "ORACLE_USER must be set in Quest® Data Transporter for Hive configuration or the environment"
[ -z "${ORACLE_PASSWORD}" ] && unset ORACLE_PASSWORD

# Target and performance parameters
[ "${OVERIDE_TABLE_NAME}" ] && ORACLE_TABLE=${OVERIDE_TABLE_NAME}
[ "${ORACLE_TABLE}" ] || die "ORACLE_TABLE must be set in Quest® Data Transporter for Hive configuration or the environment"
INSERT_BATCH_SIZE=${INSERT_BATCH_SIZE:-500}
COMMIT_BATCH_COUNT=${COMMIT_BATCH_COUNT:-20}

###
### validate locations of software Quest® Data Transporter for Hive depends upon
###

# Hadoop core jar
[ -f "${HADOOP_CORE_JAR}" ] || die "Hadoop core jar file not found: ${HADOOP_CORE_JAR}" 2
[ -r "${HADOOP_CORE_JAR}" ] || die "Hadoop core jar file is not readable: ${HADOOP_CORE_JAR}" 13

# Oracle JDBC driver
[ -f "${ORACLE_JDBC_JAR}" ] || die "Oracle JDBC jar file not found: ${ORACLE_JDBC_JAR}" 2
[ -r "${ORACLE_JDBC_JAR}" ] || die "Oracle JDBC jar file is not readable: ${ORACLE_JDBC_JAR}" 13

# Hive home must be valid
[ "${HIVE_HOME}" ] || die "HIVE_HOME must be set in Quest® Data Transporter for Hive configuration or the environment"
[ -d "${HIVE_HOME}" ] || die "Hive home is not a directory: ${HIVE_HOME}" 20
[ -x "${HIVE_HOME}/bin/hive" ] || die "Hive home does not contain hive executable: ${HIVE_HOME}/bin/hive" 2

# Quest® Data Transporter for Hive itself - usually this will just be the directory that contains this script
ORAHIVE_HOME=`dirname $0`

# Generate the CLASSPATH for Quest® Data Transporter for Hive...
CP=$ORAHIVE_HOME/bin/orahive.jar:${HADOOP_CORE_JAR}:${ORACLE_JDBC_JAR}:`find ${HIVE_HOME}/lib -name "*.jar" | tr "\n" ":"`

# dump our settings if we are running in verbose mode
if [ "${VERBOSE}" ]
then
	echo
	echo "*** Quest® Data Transporter for Hive runtime settings ***"
	echo
	echo "ORAHIVE_HOME='${ORAHIVE_HOME}'"
	echo "HQL_FILE='${HQL_FILE}'"
	echo "HADOOP_CORE_JAR='${HADOOP_CORE_JAR}'"
	echo "HIVE_HOME='${HIVE_HOME}'"
	echo "HIVE_URL='${HIVE_URL}'"
	echo "HIVE_USER='${HIVE_USER}'"
	echo "HIVE_PASSWORD='${HIVE_PASSWORD}'"
	echo "ORACLE_HOME='${ORACLE_JDBC_JAR}'"
	echo "ORACLE_URL='${ORACLE_URL}'"
	echo "ORACLE_USER='${ORACLE_USER}'"
	echo "ORACLE_PASSWORD='${ORACLE_PASSWORD}'"
	echo "ORACLE_TABLE='${ORACLE_TABLE}'"
	echo "INSERT_BATCH_SIZE='${INSERT_BATCH_SIZE}'"
	echo "COMMIT_BATCH_COUNT='${COMMIT_BATCH_COUNT}'"
	echo
	echo "HIVE_QUERY:"
	sed 's/^/    /' ${HQL_FILE}
	echo
fi

###
### Launch Quest® Data Transporter for Hive...
###

# redirect stderr unless in verbose mode
REDIRECT_STDERR="2>/dev/null"
[ "${VERBOSE}" ] && REDIRECT_STDERR=""

# set the Quest® Data Transporter for Hive command line, echo it if we are verbose, and then execute it
ORAHIVE_COMMAND="java -cp \"$CP\" com.quest.orahive.HiveJdbcClient \
	-hive $HIVE_URL -hiveuser $HIVE_USER ${HIVE_PASSWORD:+-hivepassword ${HIVE_PASSWORD}} -hqlfile $HQL_FILE \
	-oracle $ORACLE_URL -oracleuser $ORACLE_USER ${ORACLE_PASSWORD:+-oraclepassword ${ORACLE_PASSWORD}} -oracletable $ORACLE_TABLE \
	-insertbatchsize $INSERT_BATCH_SIZE -commitbatchcount $COMMIT_BATCH_COUNT ${EXPORT_MODE:+-exportmode ${EXPORT_MODE}} \
	-log4j $ORAHIVE_HOME/log4j.txt ${REDIRECT_STDERR}"
[ "${VERBOSE}" ] && echo "ORAHIVE COMMAND:" && echo ${ORAHIVE_COMMAND}
eval ${ORAHIVE_COMMAND}

# remove the temp file if we created one
[ "${TEMP_HQL_FILE}" ] && /bin/rm -f ${TEMP_HQL_FILE}
