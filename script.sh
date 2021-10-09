#!/bin/bash

folder=$1
bucket=$2
pwd=$3

PWD=$(head -n 1 $pwd)
echo $PWD
pwd1="$PWD"
echo $pwd1
rm -R $1
7zz x $bucket -p$pwd1
