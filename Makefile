.PHONY: build test verify up down logs run-redis run-api run-worker bench bench-http demo clean

MVN ?= ./mvnw
REDIS_PORT ?= 6379

build:            ## Compile and package all modules (skips tests)
	$(MVN) -B package -DskipTests

test:             ## Unit tests only
	$(MVN) -B test

verify:           ## Unit + integration tests (needs Docker for Testcontainers) + coverage report
	$(MVN) -B verify

up:               ## Run Redis + API + 2 workers in Docker
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f api worker

run-redis:
	redis-server --port $(REDIS_PORT) --appendonly yes

run-api: build
	REDIS_PORT=$(REDIS_PORT) java -jar redisflow-api/target/redisflow-api-*.jar

run-worker: build
	REDIS_PORT=$(REDIS_PORT) java -jar redisflow-worker/target/redisflow-worker-*.jar

bench: build      ## In-process engine benchmark (uses an isolated key prefix, cleans up after itself)
	REDIS_PORT=$(REDIS_PORT) java -jar redisflow-bench/target/redisflow-bench-*.jar --bench.mode=queue

bench-http: build ## Load-test a running API (start it with REDISFLOW_RATE_LIMIT_ENABLED=false)
	java -jar redisflow-bench/target/redisflow-bench-*.jar --bench.mode=http --bench.url=http://localhost:8080

demo:             ## Enqueue a few sample jobs against a running API
	@curl -s -X POST localhost:8080/api/v1/jobs -H 'Content-Type: application/json' \
	  -d '{"queue":"emails","type":"email","payload":{"to":"alice@example.com","subject":"Welcome"}}'; echo
	@curl -s -X POST localhost:8080/api/v1/jobs -H 'Content-Type: application/json' -H 'Idempotency-Key: order-1001' \
	  -d '{"queue":"payments","type":"payment","priority":1,"payload":{"amount":49.99,"currency":"USD","order_id":"1001"}}'; echo
	@curl -s -X POST localhost:8080/api/v1/jobs -H 'Content-Type: application/json' \
	  -d '{"type":"flaky","max_attempts":5,"payload":{"failure_rate":0.6}}'; echo
	@curl -s -X POST localhost:8080/api/v1/jobs -H 'Content-Type: application/json' \
	  -d '{"type":"payment","queue":"payments","payload":{"amount":-1,"currency":"usd"}}'; echo

clean:
	$(MVN) -B clean
