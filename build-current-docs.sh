#!/bin/sh
VERSION="0.1.x"

(cd doc; make)
cp -vr doc/index.html /tmp/index.html;
git checkout gh-pages;

mkdir -p ./$VERSION/
mv -fv /tmp/index.html ./$VERSION/
git add --all ./$VERSION/index.html
git commit -a -m "Update ${VERSION} doc"
