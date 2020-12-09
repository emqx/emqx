.PHONY: clean, clean_result, start_broker  stop_broker  case1 case2 case3

RELX_CONF    = emqx-rel/relx.config
LIBCOAP_GIT  = libcoap/README.md

all:  clean_result $(RELX_CONF) $(LIBCOAP_GIT) start_broker clean_result case1  case2  case3 case4 stop_broker
	@echo "  "
	@echo "  test complete"
	@echo "  "
	
clean_result:
	-rm -f case*.txt

	
start_broker:
	-rm -f emqx-rel/_rel/emqx/log/*
	-emqx-rel/_rel/emqx/bin/emqx stop
	sleep 1
	emqx-rel/_rel/emqx/bin/emqx start
	sleep 1
	emqx-rel/_rel/emqx/bin/emqx_ctl plugins load emqx_coap

stop_broker:
	-emqx-rel/_rel/emqx/bin/emqx stop

case1:
	libcoap/examples/coap-client -m get -s 5  "coap://127.0.0.1/mqtt/topic1?c=client1&u=tom&p=secret" > case1_output.txt &
	sleep 1
	libcoap/examples/coap-client -m put -e w123G45 "coap://127.0.0.1/mqtt/topic1?c=client2&u=mike&p=pw12"
	sleep 6
	python check_result.py  case1  case1_output.txt==w123G45
	
case2:
	# subscribe to topic="x/y"
	libcoap/examples/coap-client -m get -s 5  "coap://127.0.0.1/mqtt/x%2Fy?c=client3&u=tom&p=secret" > case2_output1.txt &
	# subscribe to topic="+/z"
	libcoap/examples/coap-client -m get -s 5  "coap://127.0.0.1/mqtt/%2B%2Fz?c=client4&u=mike&p=pw12" > case2_output2.txt &
	sleep 1
	# publish to topic="x/y"
	libcoap/examples/coap-client -m put -e big9wolf "coap://127.0.0.1/mqtt/x%2Fy?c=client5&u=sun&p=pw3"
	# publish to topic="p/z"
	libcoap/examples/coap-client -m put -e black2ant "coap://127.0.0.1/mqtt/p%2Fz?c=client5&u=sun&p=pw3"
	sleep 6
	python check_result.py case2 case2_output1.txt==big9wolf  case2_output1.txt!=black2ant  case2_output2.txt!=big9wolf  case2_output2.txt==black2ant

case3:
	libcoap/examples/coap-client -m get -T tk12 -s 5  "coap://127.0.0.1/mqtt/a%2Fb?c=client3&u=tom&p=secret" > case3_output1.txt &
	libcoap/examples/coap-client -m get -T tk34 -s 5  "coap://127.0.0.1/mqtt/c%2Fd?c=client3&u=tom&p=secret" > case3_output2.txt &
	sleep 1
	libcoap/examples/coap-client -m put -e big9wolf "coap://127.0.0.1/mqtt/c%2Fd?c=client5&u=sun&p=pw3"
	libcoap/examples/coap-client -m put -e black2ant "coap://127.0.0.1/mqtt/a%2Fb?c=client5&u=sun&p=pw3"
	sleep 6
	python check_result.py case3 case3_output1.txt==black2ant  case3_output2.txt==big9wolf  case3_output2.txt!=black2ant
	


case4:
	# reload emqx_coap, does it work as expected?
	sleep 1
	emqx-rel/_rel/emqx/bin/emqx_ctl plugins unload emqx_coap
	sleep 1
	emqx-rel/_rel/emqx/bin/emqx_ctl plugins load emqx_coap
	sleep 1
	libcoap/examples/coap-client -m get -s 5  "coap://127.0.0.1/mqtt/topic1?c=client1&u=tom&p=secret" > case4_output.txt &
	sleep 1
	libcoap/examples/coap-client -m put -e w6J3G45 "coap://127.0.0.1/mqtt/topic1?c=client2&u=mike&p=pw12"
	sleep 6
	python check_result.py  case4  case4_output.txt==w6J3G45



	
$(RELX_CONF):
	git clone https://github.com/emqx/emqx-rel.git
	git clone https://github.com/emqx/emq-coap.git
	@echo "update emq-coap with this development code"
	mv emq-coap  emqx_coap
	-rm -rf emqx_coap/etc
	-rm -rf emqx_coap/include
	-rm -rf emqx_coap/priv
	-rm -rf emqx_coap/src
	-rm -rf emqx_coap/Makefile
	cp -rf ../etc      emqx_coap/
	cp -rf ../include  emqx_coap/
	cp -rf ../priv     emqx_coap/
	cp -rf ../src      emqx_coap/
	cp -rf ../Makefile emqx_coap/Makefile
	-mkdir emqx-rel/deps
	mv emqx_coap  emqx-rel/deps/
	@echo "start building ..."
	make -C emqx-rel -f Makefile
	

coap:  $(LIBCOAP_GIT)
	@echo "make coap"
	
$(LIBCOAP_GIT):
	git clone -b v4.1.2 http://github.com/obgm/libcoap
	cd libcoap && ./autogen.sh && ./configure --enable-documentation=no --enable-tests=no 
	make -C libcoap -f Makefile
	
r: rebuild_emq
	# r short for rebuild_emq
	@echo " rebuild complete "
	
rebuild_emq:
	-emqx-rel/_rel/emqx/bin/emqx stop
	-rm -rf emqx-rel/deps/emqx_coap/etc
	-rm -rf emqx-rel/deps/emqx_coap/include
	-rm -rf emqx-rel/deps/emqx_coap/priv
	-rm -rf emqx-rel/deps/emqx_coap/src
	-rm -rf emqx-rel/deps/emqx_coap/Makefile
	cp -rf ../etc      emqx-rel/deps/emqx_coap/
	cp -rf ../include  emqx-rel/deps/emqx_coap/
	cp -rf ../priv     emqx-rel/deps/emqx_coap/
	cp -rf ../src      emqx-rel/deps/emqx_coap/
	cp -rf ../Makefile emqx-rel/deps/emqx_coap/Makefile
	make -C emqx-rel -f Makefile
	
clean: clean_result
	-rm -f client/*.exe
	-rm -f client/*.o
	-rm -rf emqx-rel
	-rm -rf libcoap
	
lazy: clean_result start_broker case2  stop_broker
	# custom your command here
	@echo "you are so lazy"
	
