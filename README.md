# mercer_mobility

a repo to automate download and analysis of data for mercer county

## analysis runs

```mermaid
  graph TD;
      data_import_py-->set_thresholds.py;
      set_thresholds.py-->filter_thresholds.py;
```
