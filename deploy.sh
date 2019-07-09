#!/usr/bin/env bash

if [ ! -e settings.xml ]; then
    echo "No 'settings.xml' file found.";
    cat <<EOF
Example:

<settings>
  <servers>
    <server>
      <id>clojars</id>
      <username>username</username>
      <password>password</password>
    </server>
  </servers>
</settings>
EOF
fi

if [ ! -e pom.xml ]; then
    echo "pom.xml file does not exit, you can generate it executing: clojure -Spom";
    exit 1;
fi

if [ ! $1 ]; then
    echo "Filename not provided."
    exit 1;
fi

if [ ! -e $1 ]; then
    echo "Filename '$1' does not exists"
    exit 1;
fi

mvn -s settings.xml deploy:deploy-file -Dfile=$1 -DrepositoryId=clojars -Durl=https://clojars.org/repo -DpomFile=pom.xml

