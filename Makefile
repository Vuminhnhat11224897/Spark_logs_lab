install:
	python3 -m pip install -e .

test:
	python3 -m pytest -q

check-imports:
	PYTHONPATH=src python3 -c "import spark_log_lab; from spark_log_lab.common.paths import project_root; print(project_root())"

check-raw:
	./scripts/submit_raw_check.sh

profile-raw:
	./scripts/submit_raw_profile.sh

bronze:
	./scripts/submit_bronze_build.sh

check-bronze:
	./scripts/submit_bronze_check.sh

quality:
	python3 jobs/04_run_quality_checks.py

up:
	docker compose up -d

down:
	docker compose down

ps:
	docker compose ps

show-structure:
	find . -maxdepth 3 -type d | sort

show-results:
	ls -lah results || true

samples:
	python3 scripts/generate_sample_lakehouse_data.py
