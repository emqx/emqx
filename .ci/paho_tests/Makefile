## default globals
TARGET ?= emqx/emqx
EMQX_NAME = $(subst emqx/,,$(TARGET))

.PHONY: all
all: test

define wait_emqx
	@while [ "$$(docker inspect -f '{{ .State.Health.Status}}' $$(docker ps -a -q -f name=paho_test_emqx1))" != "healthy" ] || [ "$$(docker inspect -f '{{ .State.Health.Status}}' $$(docker ps -a -q -f name=paho_test_emqx2))" != "healthy" ]; do \
		if [ $$(docker ps -a -f name=paho_test_emqx -f status=exited -q | wc -l) -ne 0 ]; then \
			echo "['$$(date -u +"%Y-%m-%dT%H:%M:%SZ")']:emqx stop"; \
			exit; \
		else \
			echo "['$$(date -u +"%Y-%m-%dT%H:%M:%SZ")']:waiting emqx"; \
			sleep 5; \
		fi; \
	done
endef

.PHONY: create_container
create_container: clean
	@docker-compose -p paho_test up -d
	
	$(call wait_emqx)

.PHONY: test
test: create_container
	@docker exec -i $$(docker ps -a -q -f name=paho_test_client) sh -c "apk update && apk add git curl \
	&& git clone -b $(PAHO_BRANCH) https://github.com/emqx/paho.mqtt.testing.git /paho.mqtt.testing \
	&& pip install pytest \
	&& pytest -v /paho.mqtt.testing/interoperability/test_client/ --host node1.emqx.io"
	
	@docker-compose -p paho_test down

.PHONY: cluster_test
cluster_test: create_container
	@docker exec -i $$(docker ps -a -q -f name=paho_test_client) sh -c "apk update && apk add git curl \
	&& git clone -b $(PAHO_BRANCH) https://github.com/emqx/paho.mqtt.testing.git /paho.mqtt.testing \
	&& pip install pytest \
	&& pytest -v /paho.mqtt.testing/interoperability/test_client/V5/test_connect.py -k test_basic --host node1.emqx.io \
	&& pytest -v /paho.mqtt.testing/interoperability/test_cluster --host1 node1.emqx.io --host2 node2.emqx.io"

	@docker-compose -p paho_test down

.PHONY: clean
clean:
	@if [ ! -z "$$(docker ps -a -q -f name=paho_test)" ]; then docker-compose -p paho_test down; fi
