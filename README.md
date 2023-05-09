# mercer_mobility

This repo is for the FY23 Mercer Mobility project. It conflates various networks to a base network (NJ Centerlines).

The process then assigns points based on various thresholds to create a data-driven way for Mercer County to pick and prioritize projects.

## prerequisites

You'll need a .env file (gitignored) with a path to your working data folder. I used:
`/mnt/g/Shared drives/Mercer County Mobility Element Update (FY23)/Data`

You'll also need a pg-data-etl config file unless you want to refactor to use your own connection engine. for more details on this, see [the pg-data-etl library](https://github.com/aaronfraint/pg-data-etl), but in short, type `pg make-config-file` into a terminal after you've made your conda environment and the navigate to the file to set up your db secrets. 

conda environment can be created by typing the following into a terminal. if the conda solve is taking a while, try mamba instead.
`conda env create -f environment.yml`

for now, i'm using a local package called planbelt. you'll have to copy or symlink it into your python env's site packages for now, repo is [here](https://github.com/dvrpc/plan-belt). this will be added to pip eventually. 

## analysis runs

follow the commands in the makefile, in order, or type `make all` to run everything in order.

