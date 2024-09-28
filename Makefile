.PHONY: init_db start stop test_games test_boxscores test_bs_summaries

init_db:
	docker-compose up -d db
	sleep 10
	@echo "Database initialized and ready."

start:
	docker-compose up -d

stop:
	docker-compose down

test_games:
	python3 digital_ocean/get_games.py

test_boxscores:
	python3 digital_ocean/get_boxscores.py

test_bs_summaries:
	python3 digital_ocean/get_bs_summaries.py
