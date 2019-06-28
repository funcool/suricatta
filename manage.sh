#!/usr/bin/env bash
set -e

REV=`git log -n 1 --pretty=format:%h -- docker/`
IMGNAME="suricatta-devenv"

function kill-container {
    echo "Cleaning development container $IMGNAME:$REV..."
    if $(docker ps | grep -q $IMGNAME); then
        docker ps | grep $IMGNAME | awk '{print $1}' | xargs --no-run-if-empty docker kill
    fi
    if $(docker ps -a | grep -q $IMGNAME); then
        docker ps -a | grep $IMGNAME | awk '{print $1}' | xargs --no-run-if-empty docker rm
    fi
}

function remove-image {
    echo "Clean old development image $IMGNAME..."
    docker images | grep $IMGNAME | awk '{print $3}' | xargs --no-run-if-empty docker rmi
}

function build-devenv {
    kill-container
    echo "Building development image $IMGNAME:$REV..."
    docker build --rm=true -t $IMGNAME:$REV  -t $IMGNAME:latest --build-arg EXTERNAL_UID=$(id -u) docker/
}

function reset {
    kill-container

    if ! $(docker images | grep $IMGNAME | grep -q $REV); then
        build-devenv
    fi
}

function docker-run {
    docker run --rm -ti \
         -v `pwd`:/home/devuser/suricatta  \
         -v $HOME/.m2:/home/devuser/.m2 \
         -w /home/devuser/suricatta \
         $IMGNAME:latest $@
}


function run-devenv {
    reset || exit -1;
    mkdir -p $HOME/.m2
    docker-run /bin/zsh
}

function run-tests {
    reset || exit -1;
    docker-run clojure -Adev:test
}

function help {
    echo "suricatta devenv manager v$REV"
    echo "USAGE: $0 OPTION"
    echo "Options:"
    echo "- clean            Kill container and remove image"
    echo "- build-devenv     Build docker container for development"
    echo "- run-devenv       Run (and build if necessary) development container"
    echo "- run-tests        Execute unit tests for both backend and frontend"
}

case $1 in
    clean)
        kill-container
        remove-image
        ;;
    build-devenv)
        build-devenv
        ;;
    run-devenv)
        run-devenv
        ;;
    run-tests)
        run-tests
        ;;
    *)
        help
        ;;
esac
