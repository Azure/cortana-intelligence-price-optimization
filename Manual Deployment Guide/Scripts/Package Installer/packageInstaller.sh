#!/usr/bin/env bash

# Import the helper method module.
wget -O /tmp/HDInsightUtilities-v01.sh -q https://hdiconfigactions.blob.core.windows.net/linuxconfigactionmodulev01/HDInsightUtilities-v01.sh && source /tmp/HDInsightUtilities-v01.sh && rm -f /tmp/HDInsightUtilities-v01.sh


#wnload LP_Solve binary to temporary location.
echo "Download LP_Solve binary to temprorary location"
download_file https://<Storage-Account-Name>.blob.core.windows.net/actionscript/lp_solve.tar.gz /tmp/lp_solve.tar.gz

# Untar the lp_solve binary and move it to proper location.
echo "Untar binary file"
untar_file /tmp/lp_solve.tar.gz /tmp
mv /tmp/lp_solve_driver/* /usr/bin/anaconda/lib/python2.7/site-packages/
mv /tmp/lp_solve_lib/* /usr/bin/anaconda/lib/


# HDI comes with pandas 19 now
# Upgrade the pandas package to the latest one.
# echo "Upgrade the pandas package"
# sudo /usr/bin/anaconda/bin/pip install pandas==0.18 --upgrade
# echo "installed successfully"



# Upgrade the azure-stoare package.
echo "Upgrade the azure-stoare package"
sudo /usr/bin/anaconda/bin/pip install azure-storage --upgrade
echo "Azure Storage installed successfully"


