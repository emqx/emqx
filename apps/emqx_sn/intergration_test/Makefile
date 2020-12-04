.PHONY: clean, clean_result, start_broker  stop_broker  case1

RELX_CONF = emqx-rel/relx.config
PAHO_GIT  = paho.mqtt-sn.embedded-c/README.md

PAHO_SRC  = paho.mqtt-sn.embedded-c/MQTTSNPacket/src/MQTTSNConnectClient.c        \
			paho.mqtt-sn.embedded-c/MQTTSNPacket/src/MQTTSNConnectServer.c        \
			paho.mqtt-sn.embedded-c/MQTTSNPacket/src/MQTTSNDeserializePublish.c   \
			paho.mqtt-sn.embedded-c/MQTTSNPacket/src/MQTTSNPacket.c               \
			paho.mqtt-sn.embedded-c/MQTTSNPacket/src/MQTTSNSearchClient.c         \
			paho.mqtt-sn.embedded-c/MQTTSNPacket/src/MQTTSNSearchServer.c         \
			paho.mqtt-sn.embedded-c/MQTTSNPacket/src/MQTTSNSerializePublish.c     \
			paho.mqtt-sn.embedded-c/MQTTSNPacket/src/MQTTSNSubscribeClient.c      \
			paho.mqtt-sn.embedded-c/MQTTSNPacket/src/MQTTSNSubscribeServer.c      \
			paho.mqtt-sn.embedded-c/MQTTSNPacket/src/MQTTSNUnsubscribeClient.c    \
			paho.mqtt-sn.embedded-c/MQTTSNPacket/src/MQTTSNUnsubscribeServer.c    \
			paho.mqtt-sn.embedded-c/MQTTSNPacket/samples/transport.c              \
			client/int_test_result.c

INCLUDE_PATH = -Ipaho.mqtt-sn.embedded-c/MQTTSNPacket/src -Ipaho.mqtt-sn.embedded-c/MQTTSNPacket/samples -Iclient

all:  clean_result $(RELX_CONF) $(PAHO_GIT) start_broker case1 case2 case3 case4 case5 case6 case7 case8 stop_broker disable_qos3
	@echo "  "
	@echo "  test complete"
	@echo "  "

clean_result:
	-rm -f client/*_FAIL.txt
	-rm -f client/*_PASS.txt
	-rm -f emq-relx/_rel/emqx/log/*

start_broker:
	-emqx-rel/_rel/emqx/bin/emqx stop
	sleep 3
	emqx-rel/_rel/emqx/bin/emqx start
	sleep 5
	emqx-rel/_rel/emqx/bin/emqx_ctl plugins load emqx_sn

stop_broker:
	emqx-rel/_rel/emqx/bin/emqx stop

disable_qos3:
	python ./disable_qos3.py

case1:
	-ps aux|grep case1_qos0pub|awk '{print $$2}'|xargs kill -9
	-ps aux|grep case1_qos0sub|awk '{print $$2}'|xargs kill -9
	gcc client/case1_qos0pub.c $(PAHO_SRC) $(INCLUDE_PATH) -o client/case1_qos0pub.exe
	gcc client/case1_qos0sub.c $(PAHO_SRC) $(INCLUDE_PATH) -o client/case1_qos0sub.exe
	client/case1_qos0sub.exe &
	sleep 1
	client/case1_qos0pub.exe

case2:
	-ps aux|grep case2_qos0pub|awk '{print $$2}'|xargs kill -9
	-ps aux|grep case2_qos0sub|awk '{print $$2}'|xargs kill -9
	gcc client/case2_qos0pub.c $(PAHO_SRC) $(INCLUDE_PATH) -o client/case2_qos0pub.exe
	gcc client/case2_qos0sub.c $(PAHO_SRC) $(INCLUDE_PATH) -o client/case2_qos0sub.exe
	client/case2_qos0sub.exe &
	sleep 1
	client/case2_qos0pub.exe

case3:
	-ps aux|grep case3_qos0pub|awk '{print $$2}'|xargs kill -9
	-ps aux|grep case3_qos0sub|awk '{print $$2}'|xargs kill -9
	gcc client/case3_qos0pub.c $(PAHO_SRC) $(INCLUDE_PATH) -o client/case3_qos0pub.exe
	gcc client/case3_qos0sub.c $(PAHO_SRC) $(INCLUDE_PATH) -o client/case3_qos0sub.exe
	client/case3_qos0sub.exe &
	sleep 1
	client/case3_qos0pub.exe

case4:
	-ps aux|grep case4_qos3pub|awk '{print $$2}'|xargs kill -9
	-ps aux|grep case4_qos3sub|awk '{print $$2}'|xargs kill -9
	gcc client/case4_qos3pub.c $(PAHO_SRC) $(INCLUDE_PATH) -o client/case4_qos3pub.exe
	gcc client/case4_qos3sub.c $(PAHO_SRC) $(INCLUDE_PATH) -o client/case4_qos3sub.exe
	client/case4_qos3sub.exe &
	sleep 1
	client/case4_qos3pub.exe

case5:
	-ps aux|grep case5_qos3pub|awk '{print $$2}'|xargs kill -9
	-ps aux|grep case5_qos3sub|awk '{print $$2}'|xargs kill -9
	gcc client/case5_qos3pub.c $(PAHO_SRC) $(INCLUDE_PATH) -o client/case5_qos3pub.exe
	gcc client/case5_qos3sub.c $(PAHO_SRC) $(INCLUDE_PATH) -o client/case5_qos3sub.exe
	client/case5_qos3sub.exe &
	sleep 1
	client/case5_qos3pub.exe

case6:
	-ps aux|grep case6_sleep|awk '{print $$2}'|xargs kill -9
	gcc client/case6_sleep.c $(PAHO_SRC) $(INCLUDE_PATH) -o client/case6_sleep.exe
	client/case6_sleep.exe

$(RELX_CONF):
	git clone -b master https://github.com/emqx/emqx-rel.git
	git clone https://github.com/emqx/emqx-sn.git
	@echo "update emqx-sn with this development code"
	mv emqx-sn  emqx_sn
	-rm -rf emqx_sn/etc
	-rm -rf emqx_sn/include
	-rm -rf emqx_sn/priv
	-rm -rf emqx_sn/src
	-rm -rf emqx_sn/Makefile
	cp -rf ../etc      emqx_sn/
	cp -rf ../include  emqx_sn/
	cp -rf ../priv     emqx_sn/
	cp -rf ../src      emqx_sn/
	cp -rf ../Makefile emqx_sn/Makefile
	-mkdir emqx-rel/deps
	mv emqx_sn  emqx-rel/deps/
	python ./enable_qos3.py
	@echo "start building ..."
	make -C emqx-rel -f Makefile

paho:
	make $(PAHO_GIT)

paho: $(PAHO_GIT)
	@echo "clone paho"

$(PAHO_GIT):
	git clone https://github.com/eclipse/paho.mqtt-sn.embedded-c/

r: rebuild_emq
	# short for rebuild_emq
	@echo "rebuild complete"

rebuild_emq:
	-rm -rf emqx-rel/deps/emqx_sn/etc
	-rm -rf emqx-rel/deps/emqx_sn/include
	-rm -rf emqx-rel/deps/emqx_sn/priv
	-rm -rf emqx-rel/deps/emqx_sn/src
	-rm -rf emqx-rel/deps/emqx_sn/Makefile
	-rm -f  emqx-rel/_rel/emqx/log/*
	cp -rf ../etc      emqx-rel/deps/emqx_sn/
	cp -rf ../include  emqx-rel/deps/emqx_sn/
	cp -rf ../priv     emqx-rel/deps/emqx_sn/
	cp -rf ../src      emqx-rel/deps/emqx_sn/
	cp -rf ../Makefile emqx-rel/deps/emqx_sn/Makefile
	python ./enable_qos3.py
	make -C emqx-rel -f Makefile

clean: clean_result
	-rm -f client/*.exe
	-rm -f client/*.o
	-rm -rf emqx-rel
	-rm -rf paho.mqtt-sn.embedded-c

lazy:  clean_result start_broker case1 case2 case3 case4 case5 stop_broker disable_qos3
	@echo "lazy work"
