  
.PHONY: run stop rm

run:
	docker-compose -f docker-compose.yml up -d --remove-orphans
	@echo "Airflow running on http://localhost:8080"

stop:
	docker-compose -f docker-compose.yml stop

rm: stop
	docker-compose -f docker-compose.yml rm
