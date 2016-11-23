#!/usr/bin/env bash

echo "Action script for installing MRS on HDI..."

#versions
MRO_FILE_VERSION=3.3
MRS_FILE_VERSION=9.0
AZUREML_VERSION=2.13

#filenames
MRO_PACKAGE=microsoft-r-open-mro-"$MRO_FILE_VERSION"
MRO_FILENAME="$MRO_PACKAGE".deb
MRO_INTEL_MKL_PACKAGE=microsoft-r-open-mkl-"$MRO_FILE_VERSION"
MRO_INTEL_MKL_FILENAME="$MRO_INTEL_MKL_PACKAGE".deb
MRO_FOREACHITERATORS_PACKAGE=microsoft-r-open-foreachiterators-"$MRO_FILE_VERSION"
MRO_FOREACHITERATORS_FILENAME="$MRO_FOREACHITERATORS_PACKAGE".deb
MRS_PACKAGES_PACKAGE=microsoft-r-server-packages-"$MRS_FILE_VERSION"
MRS_PACKAGES_FILENAME="$MRS_PACKAGES_PACKAGE".deb
MRS_HADOOP_PACKAGE=microsoft-r-server-hadoop-"$MRS_FILE_VERSION"
MRS_HADOOP_FILENAME="$MRS_HADOOP_PACKAGE".deb
REVO_PROBE_FILENAME=RevoProbe.py
AZUREML_TAR_FILE=AzureML_"$AZUREML_VERSION"_Compiled.tar.gz
ELAPSED_TIME_LOG=/tmp/installRServerElapsedTime.log

#storage with CDN enabled
#the pattern we're using here is, http://mrshdiprod.azureedge.net/{mrs release}/{mrs build number}.{version number of all other packages}
BLOB_STORAGE=https://mrshdiprod.azureedge.net/mrs-hdi-binaries-9-0-1/472.0
SAS="?sv=2014-02-14&sr=c&sig=D92aent8YxDetGS%2B6abbUou0oEzp2KDkLcwUq%2FUGmzs%3D&st=2016-11-04T07%3A00%3A00Z&se=2020-01-01T08%3A00%3A00Z&sp=r"
#hostname identifiers
HEADNODE=^hn
EDGENODE=^ed
WORKERNODE=^wn
ZOOKEEPERNODE=^zk

#misc
USERNAME=""
IS_HEADNODE=0
IS_EDGENODE=0
R_LIBRARY_DIR=/usr/lib64/microsoft-r/"$MRO_FILE_VERSION"/lib64/R/library
RPROFILE_PATH=/usr/lib64/microsoft-r/"$MRO_FILE_VERSION"/lib64/R/etc/Rprofile.site
PROBES_ROOT_DIR=/usr/lib/python2.7/dist-packages/hdinsight_probes
PROBES_CONFIG="$PROBES_ROOT_DIR"/probes_config.json
PROBES_PYTHON_DIR="$PROBES_ROOT_DIR"/probes
R_LOG_PYTHON="$R_LIBRARY_DIR"/RevoScaleR/pythonScripts/common/logScaleR.py
LIB_JVM_SYMLINK_DIR=/usr/local/lib64

#retry
MAXATTEMPTS=3

#set non-interactive mode
export DEBIAN_FRONTEND=noninteractive

#main function
main()
{
    #if there is any failure in the script, retry the entire script
	retry start
}

start()
{
	SECONDS=0

	executor downloadHelper
	if [[ $? -ne 0 ]]; then return 1; fi

	executor downloadMROMRS
	if [[ $? -ne 0 ]]; then return 1; fi
	
	executor installMRO
	if [[ $? -ne 0 ]]; then return 1; fi
	
	executor installMROIntelMKL
	if [[ $? -ne 0 ]]; then return 1; fi
	
	executor installMROForeachiterators
	if [[ $? -ne 0 ]]; then return 1; fi
	
	executor installMRSPackages
	if [[ $? -ne 0 ]]; then return 1; fi
	
	executor installMRSHadoop
	if [[ $? -ne 0 ]]; then return 1; fi
	
	executor configureRWithJava
	if [[ $? -ne 0 ]]; then return 1; fi
	
	executor updateDependencies
	if [[ $? -ne 0 ]]; then return 1; fi
	
	executor configureSSHUser
	if [[ $? -ne 0 ]]; then return 1; fi
	
	executor removeTempFiles
	if [[ $? -ne 0 ]]; then return 1; fi
	
	executor testR
	if [[ $? -ne 0 ]]; then return 1; fi
	
	executor determineNodeType
	if [[ $? -ne 0 ]]; then return 1; fi
	
	executor setupTelemetry
	if [[ $? -ne 0 ]]; then return 1; fi
	
	executor setupHealthProbe
	if [[ $? -ne 0 ]]; then return 1; fi
	
	executor writeClusterDefinition
	if [[ $? -ne 0 ]]; then return 1; fi
	
	executor installAzureMLRPackage
	if [[ $? -ne 0 ]]; then return 1; fi
	
	executor autoSparkSetting
	if [[ $? -ne 0 ]]; then return 1; fi
	
	echo "Total elapsed time = $SECONDS seconds" | tee -a $ELAPSED_TIME_LOG
	logElapsedTime
	echo "Finished"
	exit 0
}

retry()
{
	#retries to install if there is a failure
	
	ATTMEPTNUM=1
	RETRYINTERVAL=2
	RETVAL_RETRY=0

	"$1"
    if [ "$?" != "0" ]
    then
        RETVAL_RETRY=1
	fi

	while [ $RETVAL_RETRY -ne 0 ]; do
		if (( ATTMEPTNUM == MAXATTEMPTS ))
		then
			echo "Attempt $ATTMEPTNUM failed. no more attempts left."
			return 1
		else
			echo "Attempt $ATTMEPTNUM failed! Retrying in $RETRYINTERVAL seconds..."
			sleep $(( RETRYINTERVAL ))
			let ATTMEPTNUM=ATTMEPTNUM+1

			"$1"
			if [ "$?" != "0" ]
			then
				RETVAL_RETRY=1
			else
				return 0
			fi
		fi
	done
	
	return 0
}

executor()
{
	#wrapper function that calculates time to execute another function

	RETVAL_EXE=0
	START=`date +%s%N`
	
	#execute the function passed as a parameter
	"$1"
	if [ "$?" != "0" ]
	then
		RETVAL_EXE=1
	fi

	END=`date +%s%N`
	ELAPSED=`echo "scale=8; ($END - $START) / 1000000000" | bc`
	echo "Elapsed time for $1 = $ELAPSED seconds" >> $ELAPSED_TIME_LOG

	return $RETVAL_EXE
}

downloadHelper()
{
	echo "-------------------------------------------------------"
	echo "Import the helper method module..."
	echo "-------------------------------------------------------"
	
	wget -O /tmp/HDInsightUtilities-v01.sh -q https://hdiconfigactions.blob.core.windows.net/linuxconfigactionmodulev01/HDInsightUtilities-v01.sh && source /tmp/HDInsightUtilities-v01.sh && rm -f /tmp/HDInsightUtilities-v01.sh
}

downloadMROMRS()
{
	echo "-------------------------------------------------------"
	echo "Download MRO/MRS files..."
	echo "-------------------------------------------------------"

	download_file "$BLOB_STORAGE/$MRO_FILENAME$SAS" /tmp/$MRO_FILENAME
	download_file "$BLOB_STORAGE/$MRO_INTEL_MKL_FILENAME$SAS" /tmp/$MRO_INTEL_MKL_FILENAME
	download_file "$BLOB_STORAGE/$MRO_FOREACHITERATORS_FILENAME$SAS" /tmp/$MRO_FOREACHITERATORS_FILENAME
	download_file "$BLOB_STORAGE/$MRS_PACKAGES_FILENAME$SAS" /tmp/$MRS_PACKAGES_FILENAME
	download_file "$BLOB_STORAGE/$MRS_HADOOP_FILENAME$SAS" /tmp/$MRS_HADOOP_FILENAME
	download_file "$BLOB_STORAGE/$REVO_PROBE_FILENAME$SAS" /tmp/$REVO_PROBE_FILENAME
}

installMRO()
{
	echo "-------------------------------------------------------"
	echo "Install MRO..."
	echo "-------------------------------------------------------"

	#check if package is installed
	dpkg -l $MRO_PACKAGE > /dev/null 2>&1
	INSTALLED=$?
	if [ $INSTALLED == '0' ]; then
		return 0
	fi

	if [ -f /tmp/"$MRO_FILENAME" ]
	then
		dpkg --install /tmp/"$MRO_FILENAME"
	else
		echo "MRO not downloaded"
		return 1
	fi
}

installMROIntelMKL()
{
	echo "-------------------------------------------------------"
	echo "Install MRO Intel MKL..."
	echo "-------------------------------------------------------"

	#check if package is installed
	dpkg -l $MRO_INTEL_MKL_PACKAGE > /dev/null 2>&1
	INSTALLED=$?
	if [ $INSTALLED == '0' ]; then
		return 0
	fi

	if [ -f /tmp/"$MRO_INTEL_MKL_FILENAME" ]
	then
		dpkg --install /tmp/"$MRO_INTEL_MKL_FILENAME"
	else
		echo "MRO Intel MKL not downloaded"
		return 1
	fi
}

installMROForeachiterators()
{
	echo "-------------------------------------------------------"
	echo "Install MRO Foreachiterators..."
	echo "-------------------------------------------------------"

	#check if package is installed
	dpkg -l $MRO_FOREACHITERATORS_PACKAGE > /dev/null 2>&1
	INSTALLED=$?
	if [ $INSTALLED == '0' ]; then
		return 0
	fi

	if [ -f /tmp/"$MRO_FOREACHITERATORS_FILENAME" ]
	then
		dpkg --install /tmp/"$MRO_FOREACHITERATORS_FILENAME"
	else
		echo "MRO Foreachiterators not downloaded"
		return 1
	fi
}

installMRSPackages()
{
	echo "-------------------------------------------------------"
	echo "Install MRS Packages..."
	echo "-------------------------------------------------------"

	#check if package is installed
	dpkg -l $MRS_PACKAGES_PACKAGE > /dev/null 2>&1
	INSTALLED=$?
	if [ $INSTALLED == '0' ]; then
		return 0
	fi

	if [ -f /tmp/"$MRS_PACKAGES_FILENAME" ]
	then
		dpkg --install /tmp/"$MRS_PACKAGES_FILENAME"
	else
		echo "MRS Packages not downloaded"
		return 1
	fi
}

installMRSHadoop()
{
	echo "-------------------------------------------------------"
	echo "Install MRS Hadoop..."
	echo "-------------------------------------------------------"

	#check if package is installed
	dpkg -l $MRS_HADOOP_PACKAGE > /dev/null 2>&1
	INSTALLED=$?
	if [ $INSTALLED == '0' ]; then
		return 0
	fi

	if [ -f /tmp/"$MRS_HADOOP_FILENAME" ]
	then
		dpkg --install /tmp/"$MRS_HADOOP_FILENAME"
	else
		echo "MRS Hadoop not downloaded"
		return 1
	fi
}

configureRWithJava()
{
	echo "-------------------------------------------------------"
	echo "Configure R for use with Java..."
	echo "-------------------------------------------------------"

	ln -s /usr/bin/realpath /usr/local/bin/realpath

	echo "Configure R for use with Java..."
	R CMD javareconf
}

updateDependencies()
{
	echo "-------------------------------------------------------"
	echo "Update dependencies..."
	echo "-------------------------------------------------------"

	apt-get install -y -f
}

configureSSHUser()
{
	echo "-------------------------------------------------------"
	echo "Configuration for the specified 'ssh' user..."
	echo "-------------------------------------------------------"

	USERNAME=$( grep :Ubuntu: /etc/passwd | cut -d ":" -f1)

	$(hadoop fs -test -d /user/RevoShare/$USERNAME)
	if [[ "$?" != "0" ]]
	then
		echo "Creating HDFS directory..."
		hadoop fs -mkdir /user/RevoShare/$USERNAME
		hadoop fs -chmod 777 /user/RevoShare/$USERNAME
	fi
	if [ ! -d /var/RevoShare/$USERNAME ]
	then
		echo "Creating local directory..."
		mkdir -p /var/RevoShare/$USERNAME
		chmod 777 /var/RevoShare/$USERNAME
	fi
}

removeTempFiles()
{
	echo "-------------------------------------------------------"
	echo "Remove MRO/MRS temp files..."
	echo "-------------------------------------------------------"

	if [ -f /tmp/"$MRO_FILENAME" ]
	then
		rm -f /tmp/"$MRO_FILENAME"
	fi 
	if [ -f /tmp/"$MRO_INTEL_MKL_FILENAME" ]
	then
		rm -f /tmp/"$MRO_INTEL_MKL_FILENAME"
	fi
	if [ -f /tmp/"$MRO_FOREACHITERATORS_FILENAME" ]
	then
		rm -f /tmp/"$MRO_FOREACHITERATORS_FILENAME"
	fi
	if [ -f /tmp/"$MRS_PACKAGES_FILENAME" ]
	then
		rm -f /tmp/"$MRS_PACKAGES_FILENAME"
	fi
	if [ -f /tmp/"$MRS_HADOOP_FILENAME" ]
	then
		rm -f /tmp/"$MRS_HADOOP_FILENAME"
	fi
}

testR()
{
	#Run a small set of R commands to give some confidence that the install went ok
	echo "-------------------------------------------------------"
	echo "Test R..."
	echo "-------------------------------------------------------"

	R --no-save --no-restore -q -e 'options(mds.telemetry=0);d=rxDataStep(iris)'  2>&1 >> /tmp/rtest_inst.log
	if [ $? -eq 0 ]
	then
		echo "R installed properly"
	else
		echo "R not installed properly"
		return 1
	fi

	echo "-------------------------------------------------------"
	echo "Test Rscript..."
	echo "-------------------------------------------------------"

	Rscript --no-save --no-restore -e 'options(mds.telemetry=0);d=rxDataStep(iris)'  2>&1 >> /tmp/rtest_inst.log
	if [ $? -eq 0 ]
	then
		echo "Rscript installed properly"
	else
		echo "Rscript not installed properly"
		return 1
	fi
}

determineNodeType()
{
	echo "-------------------------------------------------------"
	echo "Determine node type..."
	echo "-------------------------------------------------------"

	if hostname | grep "$HEADNODE"0 2>&1 > /dev/null
	then
		IS_HEADNODE=1
	fi

	if hostname | grep $EDGENODE 2>&1 > /dev/null
	then
		IS_EDGENODE=1
	fi
}

setupTelemetry()
{
	# We only want to install telemetry on the headnode or edgenode
	if [ $IS_HEADNODE == 1 ] || [ $IS_EDGENODE == 1 ]
	then

		echo "-------------------------------------------------------"
		echo "Setup telemetry and logging..."
		echo "-------------------------------------------------------"

		MDS_OPTIONS='options(mds.telemetry=1)\noptions(mds.logging=1)\noptions(mds.target=\"azurehdi\")\n\n'

		if [ -f $RPROFILE_PATH ]
		then
			if ! grep 'options(mds' $RPROFILE_PATH 2>&1 > /dev/null
			then
				sed -i.bk -e "1s/^/$MDS_OPTIONS/" $RPROFILE_PATH
				sed -i 's/\r$//' $RPROFILE_PATH
			fi
		else
			echo "$RPROFILE_PATH does not exist"
			return 1
		fi
	fi
}

setupHealthProbe()
{
	# We only want to install the health probe on the headnode or edgenode
	if [ $IS_HEADNODE == 1 ] || [ $IS_EDGENODE == 1 ]
	then

		echo "-------------------------------------------------------"
		echo "Setup the R-Server HDI health probe..."
		echo "-------------------------------------------------------"


		#define the probe config entry
		read -d '' REVOPROBE <<-"EOF"
		[\\n
				   {\\n
					  \"name\" : \"RevoProbe\",\\n
					  \"version\" : \"0.1\",\\n
					  \"script\" : \"probes.RevoProbe.RevoProbe\",\\n
					  \"interval_seconds\" : 300,\\n
					  \"timeout_seconds\" : 60,\\n
					  \"node_types\" : \[\"headnode\"\]\\n
				   },
		EOF
		REVOPROBE=$(echo "$REVOPROBE"|tr '\n' ' ')
 

		if [ -f $PROBES_CONFIG ]
		then
			if ! grep 'RevoProbe' $PROBES_CONFIG 2>&1 > /dev/null
			then
				echo "Modify the probes config file..."
			
				# Remove all other probe configurations on the edgenode
				if [ $IS_EDGENODE == 1 ]
				then
					sed -i.bk -e '/\[/q' $PROBES_CONFIG
				fi

				# Insert the RevoProbe configuration
				sed -i.bk -e "0,/\[/s//${REVOPROBE}/" $PROBES_CONFIG

				# Tidy up the end of config file for an edgenode
				if [ $IS_EDGENODE == 1 ]
				then
					sed -i -e 's/headnode/edgenode/' $PROBES_CONFIG
					sed -i -e "0,/\},/s//}\n    ]\n}/" $PROBES_CONFIG
				fi

				# Get rid of any remaining '\r' characters
				sed -i 's/\r$//' $PROBES_CONFIG

				if [ -d $PROBES_PYTHON_DIR ]
				then
					if [ -f /tmp/"$REVO_PROBE_FILENAME" ]
					then
						cd $PROBES_PYTHON_DIR
						mv /tmp/"$REVO_PROBE_FILENAME" .
						pycompile "$REVO_PROBE_FILENAME"
						chmod 755 "$REVO_PROBE_FILENAME"
					fi

					echo "Restart the probes service.."
					service hdinsight-probes stop
					service hdinsight-probes start
				else
					echo "$PROBES_PYTHON_DIR does not exist"
					return 1
				fi
			fi
		else
			echo "$PROBES_CONFIG does not exist"
			return 1
		fi
	fi
}

writeClusterDefinition()
{
	echo "-------------------------------------------------------"
	echo "Writing cluster definition to HDFS..."
	echo "-------------------------------------------------------"

	NODETYPE="unknown"
	if hostname | grep $HEADNODE 2>&1 > /dev/null
	then
		NODETYPE="headnode"
	fi

	if hostname | grep $EDGENODE 2>&1 > /dev/null
	then
		NODETYPE="edgenode"
	fi

	if hostname | grep $WORKERNODE 2>&1 > /dev/null	
	then
		NODETYPE="workernode"
	fi

	if hostname | grep $ZOOKEEPERNODE 2>&1 > /dev/null
	then
		NODETYPE="zookeepernode"
	fi

	CORES=""
	if grep 'cpu cores' /proc/cpuinfo 2>&1 > /dev/null
	then
		CORES=$(grep 'cpu cores' /proc/cpuinfo | head -1 | cut -d ':' -f2 | tr -d '[:blank:]')
	else
		echo "Cannot get node cpu settings"
		return 1
	fi

	MEMORY=""
	if grep 'cpu cores' /proc/cpuinfo 2>&1 > /dev/null
	then
		MEMORY=$(grep 'MemTotal' /proc/meminfo |  cut -d ':' -f2 | tr -d '[:blank:]')
		MEMORY=${MEMORY::-2}
	else
		echo "Cannot get node memory settings"
		return 1
	fi

	HOSTNAME=`hostname`
	NODEINFO="$NODETYPE;$MEMORY;$CORES"
	echo $NODEINFO > /tmp/$HOSTNAME

	$(hadoop fs -test -d /cluster-info)
	if [[ "$?" != "0" ]]
	then
		echo "Creating HDFS directory for cluster-info..."
		hadoop fs -mkdir /cluster-info
	fi

	if [ -f /tmp/$HOSTNAME ]
	then
		echo "Creating HDFS hostname file..."
		hadoop fs -copyFromLocal -f /tmp/$HOSTNAME /cluster-info
		rm -rf /tmp/$HOSTNAME
	else
		echo "/tmp/$HOSTNAME does not exist"
	fi
}

installAzureMLRPackage()
{
	# We only want to install AzureML on the edgenode
	if  [ $IS_EDGENODE == 1 ]
	then

		echo "-------------------------------------------------------"
		echo "Installing AzureML R package..."
		echo "-------------------------------------------------------"

		if [ -d $R_LIBRARY_DIR ]
		then

			cd $R_LIBRARY_DIR

			echo "Download and install AzureML tar file..."
			#the tar file contains a "pre-compiled" archive of dependent R packages needed for the AzureML R package
			download_file "$BLOB_STORAGE/$AZUREML_TAR_FILE$SAS" ./$AZUREML_TAR_FILE

			if [ -f $R_LIBRARY_DIR/$AZUREML_TAR_FILE ]
			then
				tar -xzf $AZUREML_TAR_FILE
				rm $AZUREML_TAR_FILE
			else
				echo "AzureML R Package not downloaded"
				return 1
			fi
		else
			echo "Cannot find $R_LIBRARY_DIR"
			return 1
		fi
	fi
}

autoSparkSetting()
{
	if [ $IS_EDGENODE == 1 ]
	then
		echo "-------------------------------------------------------"
		echo "Setup spark executor settings..."
		echo "-------------------------------------------------------"
		HADOOP_CONFDIR=$(hadoop envvars | grep HADOOP_CONF_DIR | cut -d "'" -f 2)
		YARN_MEMORY=$(xmllint --xpath "/configuration/property[name[text()='yarn.nodemanager.resource.memory-mb']]/value/text()" ${HADOOP_CONFDIR}/yarn-site.xml)
		VCORES=$(xmllint --xpath "/configuration/property[name[text()='yarn.nodemanager.resource.cpu-vcores']]/value/text()" ${HADOOP_CONFDIR}/yarn-site.xml)
		CORES_AVL=$(($VCORES-3))
		EXECUTOR_MEMORY=$(($((YARN_MEMORY-3000))*2/5))
		CORE_NUM=$(($((YARN_MEMORY-3000))*8/35000))
		TMPCORE=$(($CORE_NUM<$CORES_AVL?$CORE_NUM:$CORES_AVL))
		EXECUTOR_CORES=$(($TMPCORE>1?$TMPCORE:1))
		EXECUTOR_NUM=$(grep -c -v '^ *$' ${HADOOP_CONFDIR}/slaves)

		SPARK_OPTIONS='RevoScaleR::rxOptions(spark.executorCores='${EXECUTOR_CORES}',spark.executorMem=\"'${EXECUTOR_MEMORY}'m\",spark.executorOverheadMem=\"'${EXECUTOR_MEMORY}'m\",spark.numExecutors='$EXECUTOR_NUM')\n'

		if [ -f $RPROFILE_PATH ]
		then
			if ! grep 'options(executor' $RPROFILE_PATH 2>&1 > /dev/null
			then
				sed -i.bk -e "$ a $SPARK_OPTIONS" $RPROFILE_PATH
			fi
		else
			echo "$RPROFILE_PATH does not exist"
			return 1
		fi
	fi
}

logElapsedTime()
{
	echo "-------------------------------------------------------"
	echo "Log Elapsed time..."
	echo "-------------------------------------------------------"

	if [ -f $R_LOG_PYTHON ]
	then
			python $R_LOG_PYTHON -m $ELAPSED_TIME_LOG -f 1 -p 1 2>&1 > /dev/null
	else
			echo "$R_LOG_PYTHON does not exist"
	fi
}

#call the main function
main "$@"
