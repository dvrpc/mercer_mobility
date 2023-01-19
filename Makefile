setup: 
	conda install environment.yml
	conda activate mercer

dataimport:
	python data_import.py
	python set_thresholds.py

conflate: 
	python conflate.py
	python assign_points.py
	python pull_geojsons.py
