deps:
	pip install -r ./requirements.txt

dev_deps:
	pip install -r ./requirements_dev.txt

protobuf: clean
	python -m grpc_tools.protoc -I ./proto --python_out=./pyraft --grpc_python_out=./pyraft ./proto/raft.proto ./proto/kv.proto

start:
	export ADDRESS=127.0.0.1:8003; \
	python ./pyraft/kstored.py

docker_build:
	docker build . -t kvserviced

docker_start:
	docker run -it kvserviced

compose_clean:
	docker-compose rm -sf

compose_build: compose_clean
	docker-compose build

stop_cluster:
	docker-compose down

start_cluster: compose_build
	docker-compose up

clean:
	rm -f ./pyraft/*.pyc ./pyraft/kv_pb2.py ./pyraft/kv_client_pb2_grpc.py ./pyraft/raft_pb2.py ./pyraft/raft_pb2_grpc.py

cleanpyc:
	rm -rf ./pyraft/*.pyc

all: proto compose-build

.PHONY: cleanpyc
