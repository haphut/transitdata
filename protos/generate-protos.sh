#!/bin/bash

# This scripts generates java classes based on the .proto files.

protoc gtfs-realtime.proto --java_out=../src/cacheMain/java/

protoc pubtrans-tables.proto --java_out=../src/cacheMain/java/

protoc internal-messages.proto --java_out=../src/cacheMain/java/

protoc mqtt.proto --java_out=../src/cacheMain/java/

protoc hfp.proto --java_out=../src/cacheMain/java/

protoc metro-ats.proto --java_out=../src/cacheMain/java/
