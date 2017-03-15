#!/bin/bash

SOURCE_DIR=$1
EVENTING_DIR=$2
cd $EVENTING_DIR; git pull origin master
cd $SOURCE_DIR; make -j8
