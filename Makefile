setup: 
	@echo "activating environment"
	conda env create -f environment.yml

dataimport:
	@echo "importing data and setting base thresholds"
	conda activate mercer
	python data_import.py
	python set_thresholds.py

conflate:
	@echo "conflating all networks to base layer"
	python conflate.py
	python assign_points.py
	python pull_geojsons.py

all: setup dataimport conflate 
	@echo "running all necessary scripts"
