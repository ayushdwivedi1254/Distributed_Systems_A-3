# Makefile

.PHONY: clean run

run: clean
	docker build -t distributed_systems_a-2-server:latest ./Server
	docker-compose up

clean:
	@if [ -n "$$(docker ps -a -q --filter ancestor=distributed_systems_a-2-load_balancer)" ]; then \
		docker rm -f $$(docker ps -a -q --filter ancestor=distributed_systems_a-2-load_balancer); \
	fi
	@if [ -n "$$(docker ps -a -q --filter ancestor=distributed_systems_a-2-server)" ]; then \
		docker rm -f $$(docker ps -a -q --filter ancestor=distributed_systems_a-2-server); \
	fi
	@if [ -n "$$(docker images -q distributed_systems_a-2-server)" ]; then \
		docker rmi -f distributed_systems_a-2-server; \
	fi
	@if [ -n "$$(docker images -q distributed_systems_a-2-load_balancer)" ]; then \
		docker rmi -f distributed_systems_a-2-load_balancer; \
	fi
