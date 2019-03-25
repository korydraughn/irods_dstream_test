#! /bin/bash

ip_addr=$1
irods_resource=$2

export LD_LIBRARY_PATH=/opt/irods-externals/clang-runtime6.0-0/lib
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/opt/irods-externals/boost1.67.0-0/lib
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/lib/irods/plugins/api

./irods_dstream_test $ip_addr $irods_resource
