#!/bin/sh
(cd doc; make)
cp -vr doc/index.html /tmp/index.html;
git checkout gh-pages;
mv -fv /tmp/index.html ./latest/
git add --all ./latest/index.html
git commit -a -m "Update stable doc"
