emqtt
=====

erlang mqtt broker.

requires
========

erlang R15B+ 

git client

build
=======

make

release
=======

#generate a release in rel/emqtt folder

make generate

deloy
=====

cp -R rel/emqtt $INSTALL_DIR

start
======

cd $INSTALL_DRI/emqtt

#debug mode

./bin/emqtt console

#startup 

./bin/emqtt start

status
======

./bin/emqtt_ctl status

stop
====

./bin/emqtt stop

logs
====

log/*


design
=====

https://github.com/emqtt/emqtt/wiki
