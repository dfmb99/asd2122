#!/bin/bash

processes=$1
pwd=$(pwd)

if [ -z $processes ] || [ $processes -lt 1 ]; then
	echo "please indicate a number of processes of at least one"
	exit 0
fi

port=10000
i=0

(java -jar target/asdProj.jar -conf babel_config.properties interface=bond0 port=$port my_index=$(($i + 1))| tee results/results-$(hostname)-$[$port+$i].txt) &

i=1

contactaddr=$(ifconfig bond0 | awk '/inet / {print $2}'):$port

while [ $i -lt $processes ]
do
	for node in $(oarprint host); do
	  if [ $i -lt $processes ]; then
		  oarsh $node "cd $pwd; nohup ./deploy/execute-local.sh $contactaddr $[$port+$i] $i $pwd &"
      i=$[$i+1]
    fi
  done
done