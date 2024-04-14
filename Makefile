# Makefile

.PHONY: clean run

run: clean
	docker build -t distributed_systems_a-3-server:latest ./Server
	docker build -t distributed_systems_a-3-db:latest ./Server_DB
	docker-compose up

clean:
	@if [ -n "$$(docker ps -a -q --filter ancestor=distributed_systems_a-3-load_balancer)" ]; then \
		docker rm -f $$(docker ps -a -q --filter ancestor=distributed_systems_a-3-load_balancer); \
	fi
	@if [ -n "$$(docker ps -a -q --filter ancestor=distributed_systems_a-3-shard_manager)" ]; then \
		docker rm -f $$(docker ps -a -q --filter ancestor=distributed_systems_a-3-shard_manager); \
	fi
	@if [ -n "$$(docker ps -a -q --filter ancestor=distributed_systems_a-3-metadata_db)" ]; then \
		docker rm -f $$(docker ps -a -q --filter ancestor=distributed_systems_a-3-metadata_db); \
	fi
	@if [ -n "$$(docker ps -a -q --filter ancestor=distributed_systems_a-3-server)" ]; then \
		docker rm -f $$(docker ps -a -q --filter ancestor=distributed_systems_a-3-server); \
	fi
	@if [ -n "$$(docker ps -a -q --filter ancestor=distributed_systems_a-3-db)" ]; then \
		docker rm -f $$(docker ps -a -q --filter ancestor=distributed_systems_a-3-db); \
	fi
	@if [ -n "$$(docker images -q distributed_systems_a-3-server)" ]; then \
		docker rmi -f distributed_systems_a-3-server; \
	fi
	@if [ -n "$$(docker images -q distributed_systems_a-3-load_balancer)" ]; then \
		docker rmi -f distributed_systems_a-3-load_balancer; \
	fi
	@if [ -n "$$(docker images -q distributed_systems_a-3-shard_manager)" ]; then \
		docker rmi -f distributed_systems_a-3-shard_manager; \
	fi
	@if [ -n "$$(docker images -q distributed_systems_a-3-metadata_db)" ]; then \
		docker rmi -f distributed_systems_a-3-metadata_db; \
	fi
	@if [ -n "$$(docker images -q distributed_systems_a-3-db)" ]; then \
		docker rmi -f distributed_systems_a-3-db; \
	fi