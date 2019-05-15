#! /bin/bash

        #-DUSE_FSTREAM \
        #-o s3_test main.cpp \
clang++ -std=c++17 -stdlib=libc++ -nostdinc++ -Wall -Wextra -O2 \
        -I/usr/include/irods \
        -I/usr/include/irods/plugins/api \
        -I/opt/irods-externals/clang6.0-0/include/c++/v1 \
        -I/opt/irods-externals/boost1.67.0-0/include \
        -I/opt/irods-externals/json3.1.2-0/include \
        -L/usr/lib/irods/plugins/api \
        -L/opt/irods-externals/clang6.0-0/lib \
        -L/opt/irods-externals/boost1.67.0-0/lib \
        -pthread \
        -o irods_dstream_test main.cpp \
        -lirods_common \
        -lirods_client \
        -lirods_server \
        -lirods_plugin_dependencies \
        -lirods_get_file_descriptor_info_client \
        -ludt \
        -lboost_system
