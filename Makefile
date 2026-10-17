.PHONY: build test test-multireplica test-resilience model-validate model-lock model-verify model-accept clean

build:
	mkdir -p bin
	cd services/gateway && go build -trimpath -o ../../bin/infergate ./cmd/server

test:
	cd services/gateway && go test ./...
	cd services/optimizer && python3 -m unittest -v
	cd tools/perf-lab && python3 -m unittest discover -s tests -v
	python3 -m unittest discover -s scripts/tests -p 'test_*.py' -v

test-multireplica:
	mkdir -p bin
	cd services/gateway && go build -trimpath -o ../../bin/infergate ./cmd/server
	cd services/gateway && go build -trimpath -o ../../bin/usage-export ./cmd/usage-export
	python3 scripts/integration_multireplica.py --gateway-bin bin/infergate --export-bin bin/usage-export --redis-addr "$${REDIS_TEST_ADDR:-127.0.0.1:6379}"

test-resilience: build
	python3 scripts/integration_resilience.py --gateway-bin bin/infergate

model-validate:
	python3 scripts/model_delivery.py validate --manifest config/model-deliveries/qwen3-4b.json

model-lock:
	python3 scripts/model_delivery.py lock --manifest config/model-deliveries/qwen3-4b.json

model-verify:
	python3 scripts/model_delivery.py validate --release --manifest config/model-deliveries/qwen3-4b.json

model-accept:
	python3 scripts/accept_model.py --manifest config/model-deliveries/qwen3-4b.json --key-file "$${INFERGATE_CLIENT_KEY_FILE}" --output run/model-acceptance.json

clean:
	rm -rf bin
