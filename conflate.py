# LAYERS THAT USE LRS:
#
# lrs_clipped
# crash_statistics_by_segment_mc
# traffic_adt
#
#
# -- first query- basically works
# select * from crash_statistics_by_segment_mc a
#     inner join traffic_adt b
#     on a.sri = b.sri
#     and a.startmilep = b.start_mile
#     and a.endmilepo = b.end_milepo
#     and a.average_da = b.average_da
#
#
#
#
#
# --after conflated master layer is done, try something like this to clip to mercer.
# select * from traffic_adt a
# inner join mercercountyjurisdictionroads_frommercer b
# on st_contains(st_buffer(b.geom, .9), a.geom)
#
#
#
#
#
#
