#!/usr/bin/env bash
set -e

AUTOTEST_DOCKER_IMAGE=distservices-docker.repo.openearth.io/distarch/com.lgc.dist.msp.autotest.base:2.4

# mount deploy home if it exists
args=("${@:2}")
for (( i=0; i<${#args[*]}; i++)); do
	if [ ${args[i]} == "-d" ]; then
		DEPLOY_HOME=${args[i+1]}
		MOUNT_DEPLOY="-v $DEPLOY_HOME:$DEPLOY_HOME"
		break;
	fi
done

# pull docker image from repo. If you have a local image only, pulling error is ignored
docker pull $AUTOTEST_DOCKER_IMAGE || true

docker run -i -e PYTHONUNBUFFERED=0 --rm -v $( cd "$(dirname "$1")" ; pwd -P ):/autotest/collection_home/ \
	$MOUNT_DEPLOY -v /var/run/docker.sock:/var/run/docker.sock $AUTOTEST_DOCKER_IMAGE $( basename "$1" ) "${@:2}"
