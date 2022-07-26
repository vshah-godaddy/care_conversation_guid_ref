#!/bin/bash
repo_name="care_conversation_guid_ref"
git_parent="GDLakeDataProcessors"
sudo yum install -y https://s3.amazonaws.com/ec2-downloads-windows/SSMAgent/latest/linux_amd64/amazon-ssm-agent.rpm
sudo systemctl enable amazon-ssm-agent
sudo systemctl start amazon-ssm-agent
set -e
sudo python3 -m pip install --upgrade pip setuptools wheel
sudo python3 -m pip install boto3
sudo yum install dos2unix -y

mkdir -p /home/hadoop/"$repo_name"

env=$(aws ssm get-parameter --name '/AdminParams/Team/Environment'|python3 -c "import sys,json; print(json.load(sys.stdin)['Parameter']['Value'])")
account_name=$(aws ssm get-parameter --name '/AdminParams/Team/Name'|python3 -c "import sys,json; print(json.load(sys.stdin)['Parameter']['Value'])")
sudo aws s3 cp s3://gd-"$account_name"-"$env"-code/"$git_parent"/"$repo_name"/ /home/hadoop/"$git_parent"/"$repo_name"/ --recursive
find /home/hadoop/"$git_parent"/"$repo_name"/ -type f -exec dos2unix {} \;
