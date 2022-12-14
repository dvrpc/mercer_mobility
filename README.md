# mercer_mobility

This repo is for the FY23 Mercer Mobility project. It conflates various networks to a base network (NJ Centerlines).

The process then assigns points based on various thresholds to create a data-driven way for Mercer County to pick and prioritize projects.

conflates various road networks to the NJ_centerlines layer, and then assigns points based on the threshold shown below.

## analysis runs

```mermaid
  graph TD;
      data_import.py-->set_thresholds.py;
      set_thresholds.py-->conflate.py;
      conflate.py-->assign_points.py;
```

| Urban Areas     | Goal                                     | Deficiency                       | 1 point                                             | 2 points         | CRITCAL FLAG | Total Available for Catagory |
| --------------- | ---------------------------------------- | -------------------------------- | --------------------------------------------------- | ---------------- | ------------ | ---------------------------- |
|                 | Preserve existing facilities             | bridge condition                 | 20 < x <= 50                                        | <= 20            | x            | 4                            |
|                 | pavement condition                       | 30 < x <= 60                     | <= 30                                               |                  |
|                 | Improve safety for all road useres       | vulnerable user crashes          |                                                     | Any              |              | 6                            |
|                 | KSI                                      |                                  | Any                                                 |                  |
|                 | intersection crashes                     | ?                                | ?                                                   |                  |
|                 | crash rate                               | \> 1 SD                          | \> 2 SD                                             |                  |
|                 | Promote choice of travel mode            | Missing Sidewalk                 | 50% (missing one side)                              | 0% (missing all) |              | 6                            |
|                 | Missing separated bike facilities        | Sharrows                         | No bike facility                                    |                  |
|                 | Transit Route                            | Exists on segment                | If segment gets any points for a congestion measure |                  |
|                 | Link to economic and environmental goals | % in V/C ratio from 2025 to 2050 | Top 25%                                             | Top 10%          |              | 3                            |
|                 | TTI                                      | \>= 1.5                          |                                                     |                  |
|                 | PTI                                      | \>= 3                            |                                                     |                  |
|                 | Bottlenecks                              | Top 20 in county                 |                                                     |                  |
|                 |                                          |                                  |                                                     |                  |              |                              |
|                 |                                          |                                  |                                                     |                  |              |                              |
|                 |                                          |                                  |                                                     |                  |              |                              |
|                 |                                          |                                  |                                                     |                  |              |                              |
| Non-urban Areas | Goal                                     | Deficiency                       | 1 point                                             | 2 points         | CRITCAL FLAG | Total Available for Catagory |
|                 | Preserve existing facilities             | bridge condition                 | 20 < x <= 50                                        | <= 20            | x            | 4                            |
|                 | pavement condition                       | 30 < x <= 60                     | <= 30                                               |                  |
|                 | Improve safety for all road useres       | vulnerable user crashes          |                                                     | Any              |              | 6                            |
|                 | KSI                                      |                                  | Any                                                 |                  |
|                 | intersection crashes                     | ?                                | ?                                                   |                  |
|                 | crash rate                               | \> 1 SD                          | \> 2 SD                                             |                  |
|                 | Promote choice of travel mode            | Missing Sidewalk                 | 0% (missing all)                                    |                  |              | 5                            |
|                 | Missing separated bike facilities        | Sharrows                         | No bike facility                                    |                  |
|                 | Transit Route                            | Exists on segment                | If segment gets any points for a congestion measure |                  |
|                 | Link to economic and environmental goals | % in V/C ratio from 2025 to 2050 | Top 25%                                             | Top 10%          |              | 5                            |
|                 | TTI                                      | 1.2 <= x < 1.5                   | \>= 1.5                                             |                  |
|                 | PTI                                      | 2 <= x < 3                       | \>= 3                                               |                  |
|                 | Bottlenecks                              | Top 20 in county                 |                                                     |
