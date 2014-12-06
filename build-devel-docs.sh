#!/bin/sh
(cd doc; make)
cp -vr doc/index.html /tmp/index.html;
git checkout gh-pages;
mv -fv /tmp/index.html ./devel/
git add --all devel/index.html
git commit -a -m "Update devel doc"
