#!/bin/sh

export SERVICE_KOTLIN_OPTS="-Dtransport=$1"
service-kotlin/build/install/service-kotlin/bin/service-kotlin
