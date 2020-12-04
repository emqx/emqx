
.PHONY: clean, clean_result, start_broker  stop_broker  case1 case2 case3

EMQX_DIR = emqx-enterprise-rel
EMQ    = $(EMQX_DIR)/relx.config
WAKAAMA  = build_wakaama/lightclient
PAHO_PYTHON = paho/mqtt/client.py

all:  clean_result $(EMQ) $(WAKAAMA) $(PAHO_PYTHON) start_broker clean_result case1 case2 case3 stop_broker
	@echo "  "
	@echo "  test complete"
	@echo "  "

clean_result:
	-rm -f case*.txt

start_broker:
	-rm -f $(EMQX_DIR)/_rel/emqx/log/*
	-$(EMQX_DIR)/_rel/emqx/bin/emqx stop
	sleep 3
	$(EMQX_DIR)/_rel/emqx/bin/emqx start
	sleep 1
	$(EMQX_DIR)/_rel/emqx/bin/emqx_ctl plugins load emqx_lwm2m


stop_broker:
	-$(EMQX_DIR)/_rel/emqx/bin/emqx stop


case1:
	-build_wakaama/lightclient -4 -n jXtestlwm2m &
	python case1.py
	-ps aux|grep lightclient|awk '{print $$2}'|xargs kill -2

case2:
	-build_wakaama/lightclient -4 -n jXtestlwm2m &
	python case2.py
	-ps aux|grep lightclient|awk '{print $$2}'|xargs kill -2

case3:
	-build_wakaama/lightclient -4 -n jXtestlwm2m &
	python case3.py
	-ps aux|grep lightclient|awk '{print $$2}'|xargs kill -2


$(EMQ):
	git clone https://github.com/emqx/emqx-enterprise-rel
	git clone https://github.com/emqx/emqx-lwm2m.git
	@echo "update emqx-lwm2m with this development code"
	mv emqx-lwm2m  emqx_lwm2m
	-rm -rf emqx_lwm2m/etc
	-rm -rf emqx_lwm2m/include
	-rm -rf emqx_lwm2m/priv
	-rm -rf emqx_lwm2m/src
	-rm -rf emqx_lwm2m/Makefile
	-rm -rf emqx_lwm2m/erlang.mk
	cp -rf ../etc       emqx_lwm2m/
	cp -rf ../include   emqx_lwm2m/
	cp -rf ../priv      emqx_lwm2m/
	cp -rf ../src       emqx_lwm2m/
	cp -rf ../Makefile  emqx_lwm2m/Makefile
	cp -rf ../erlang.mk emqx_lwm2m/erlang.mk
	-mkdir $(EMQX_DIR)/deps
	mv emqx_lwm2m  $(EMQX_DIR)/deps/
	@echo "start building ..."
	python insert_lwm2m_plugin.py
	make -C emqx-rel -f Makefile
	-cp -rf ../lwm2m_xml  $(EMQX_DIR)/_rel/emqx/etc/


w:
	cd build_wakaama && cmake -DCMAKE_BUILD_TYPE=Debug ../wakaama/examples/lightclient && make

$(WAKAAMA):
	git clone https://github.com/eclipse/wakaama
	-mkdir build_wakaama
	# replace lightclient's source code, change port from 5683 to 5683, since 5683 is the default port of emqx-lwm2m
	cp -f object_security.c  wakaama/examples/lightclient/object_security.c
	cd build_wakaama && cmake -DCMAKE_BUILD_TYPE=Debug ../wakaama/examples/lightclient && make


mqtt: $(PAHO_PYTHON)
	# short for paho python client

$(PAHO_PYTHON):
	git clone https://github.com/eclipse/paho.mqtt.python.git
	mv paho.mqtt.python/src/paho ./
	rm -rf paho.mqtt.python


r: rebuild_emq
	# r short for rebuild_emq
	@echo " rebuild complete "


rebuild_emq:
	-$(EMQX_DIR)/_rel/emqx/bin/emqx stop
	-mkdir $(EMQX_DIR)/deps
	-rm -rf $(EMQX_DIR)/deps/emqx_lwm2m/etc
	-rm -rf $(EMQX_DIR)/deps/emqx_lwm2m/include
	-rm -rf $(EMQX_DIR)/deps/emqx_lwm2m/priv
	-rm -rf $(EMQX_DIR)/deps/emqx_lwm2m/src
	-rm -rf $(EMQX_DIR)/deps/emqx_lwm2m/Makefile
	-rm -rf $(EMQX_DIR)/deps/emqx_lwm2m/erlang.mk
	cp -rf ../etc       $(EMQX_DIR)/deps/emqx_lwm2m/
	cp -rf ../include   $(EMQX_DIR)/deps/emqx_lwm2m/
	cp -rf ../priv      $(EMQX_DIR)/deps/emqx_lwm2m/
	cp -rf ../src       $(EMQX_DIR)/deps/emqx_lwm2m/
	cp -rf ../Makefile  $(EMQX_DIR)/deps/emqx_lwm2m/Makefile
	cp -rf ../erlang.mk $(EMQX_DIR)/deps/emqx_lwm2m/erlang.mk
	make -C $(EMQX_DIR) -f Makefile


clean: clean_result
	-rm -f client/*.exe
	-rm -f client/*.o
	-rm -rf emqx-rel
	-rm -rf build_wakaama
	-rm -rf wakaama



lazy: clean_result start_broker case1 case2 case3 stop_broker
	# custom your command here
	@echo "you are so lazy"



