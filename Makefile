.PHONY: init_db start stop test_games test_boxscores test_bs_summaries test_track init_droplet deploy_track_dates deploy_fill_pending config_git venv create_work_pool kill_vs stop_fill_pending stop_track_dates

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
	/bin/bash -c 'set -a; source .venv/bin/activate; set +a; nohup python3 prefect/track_processed_dates.py > track_dates.log 2>&1 &'
	@echo "Started track_processed_dates.py in background. Check track_dates.log for output."

deploy_fill_pending:
	/bin/bash -c 'set -a; source .venv/bin/activate; set +a; nohup python3 prefect/fill_pending.py > fill_pending.log 2>&1 &'
	@echo "Started fill_pending.py in background. Check fill_pending.log for output."

config_git:
	eval "$(ssh-agent -s)"
	ssh-add ../.ssh/id_ed25519_github_droplet
	git config --global user.name "no-one2k"
	git config --global user.email no-one2k@yandex.ru

venv:
	source /root/basnya_de/.venv/bin/activate

create_work_pool:
	prefect work-pool create $(WORK_POOL) --type prefect:managed

kill_vs:
	ps aux | grep .cursor-server | awk '{print $$2}' | xargs kill
	ps aux | grep .vscode-server | awk '{print $$2}' | xargs kill

stop_fill_pending:
	ps aux | grep "nohup python3 prefect/fill_pending.py" | awk '{print $$2}' | xargs kill

stop_track_dates:
	ps aux | grep "nohup python3 prefect/track_processed_dates.py" | awk '{print $$2}' | xargs kill
