.PHONY: init_db start stop test_games test_boxscores test_bs_summaries test_track

WORK_POOL := banya-work-pool

init_db:
	docker-compose up -d db
	sleep 10
	@echo "Database initialized and ready."

start:
	docker-compose up -d

stop:
	docker-compose down

test_track:
	/bin/bash -c 'set -a; source .env; set +a; python3 digital_ocean/track_processed_dates.py'

test_games:
	python3 digital_ocean/get_games.py

test_boxscores:
	python3 digital_ocean/get_boxscores.py

test_bs_summaries:
	python3 digital_ocean/get_bs_summaries.py

init_droplet:
	git clone git@github.com:no-one2k/basnya_de.git
	python3 -m venv .venv
	apt install python3-pip python3.12-venv
	python3 -m venv .venv
	source .venv/bin/activate
	pip install -r requirements.txt

deploy_track_dates:
	/bin/bash -c 'set -a; source .venv/bin/activate; set +a; python3 prefect/deploy.py'

config_git:
	git config --global user.name "no-one2k"
	git config --global user.email no-one2k@yandex.ru

activate_env:
	source .venv/bin/activate

create_work_pool:
	prefect work-pool create $(WORK_POOL) --type prefect:managed


