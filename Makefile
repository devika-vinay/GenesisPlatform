.PHONY: run run-all clean

# default: run all pipelines
run-all:
	docker compose up --build genesis

# run a specific country:  make run COUNTRY=mx
run:
	docker compose up --build genesis COUNTRY=$(COUNTRY)

clean:
	docker compose down --remove-orphans
	docker system prune --volumes -f
