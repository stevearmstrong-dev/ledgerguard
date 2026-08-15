.PHONY: build test kafka-up kafka-down run-engine run-api

build:
	./mvnw clean verify

test:
	./mvnw test

kafka-up:
	docker compose up -d --wait

kafka-down:
	docker compose down

run-engine:
	java -jar ledgerguard-reconciliation/target/ledgerguard-reconciliation-0.1.0-SNAPSHOT.jar

run-api:
	java -jar ledgerguard-demo-api/target/ledgerguard-demo-api-0.1.0-SNAPSHOT.jar
