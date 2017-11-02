select
	a.ebint, 
	a.clust30, 
	a.clust30a, 
	st_x(c.geom) as latitude, 
	st_y(c.geom) as longitude, 
	b.maxdepth, 
	b.area
from 
     bathymetry_predicted             b, 
     ecco_biwa_lakes_v_0_1_centroids  c, 
     ecco_biwa_lakes_v_0_1            a
where
	b.ebint = a.ebint and
	b.ebint = c.ebint and
	a.ccluster30 > 0



