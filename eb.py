import os
import numpy as np
import pandas as pd
import pg8000
from sqlalchemy import create_engine
import h5py
import datetime

depth_resolution = 0.1 # metre

def mylakeinit(zz, az, outpath=None):
    '''zz(depth): m, az(area_by_depth): m2.
    returns string to be written to an init file of MyLake'''
    
    lines = ['\t'.join([('%.2f' % d), ('%.0f' % a)] + ['4'] + ['0'] * 9)
 for d, a in zip(zz, az)]    
    lines[0] = lines[0] + '\t0\t0'  # snow and ice
    firstlines = '''-999	"MyLake init"
Z (m)	Az (m2)	Tz (deg C)	Cz	Sz (kg/m3)	TPz (mg/m3)	DOPz (mg/m3)	Chlaz (mg/m3)	DOCz (mg/m3)	TPz_sed (mg/m3)	Chlaz_sed (mg/m3)	"Fvol_IM (m3/m3	 dry w.)"	Hice (m)	Hsnow (m)'''
    lines = [firstlines] + lines
    out = '\n'.join(lines)
    if outpath is None:
        return out
    else:
        with open(outpath, 'w') as f:
            f.write(out)

def mylakepar(longitude, latitude, outpath=None, atten_coeff=1):
    '''atten_coeff: m-1 
    uses the Minesota area and BV parameters -> sets NaNs
    returns string to be written to a file'''

    out = '''-999	"MyLake parameters"			
Parameter	Value	Min	Max	Unit
dz	1	0.5	2	m
Kz_ak	NaN	NaN	NaN	(-)
Kz_ak_ice	0.000898	NaN	NaN	(-)
Kz_N0	7.00E-05	NaN	NaN	s-2
C_shelter	NaN	NaN	NaN	(-)
latitude	%.5f	NaN	NaN	dec.deg
longitude	%.5f	NaN	NaN	dec.deg
alb_melt_ice	0.3	NaN	NaN	(-)
alb_melt_snow	0.77	NaN	NaN	(-)
PAR_sat	3.00E-05	1.00E-05	1.00E-04	mol m-2 s-1
f_par	0.45	NaN	NaN	(-)
beta_chl	0.015	0.005	0.045	m2 mg-1
lambda_I	5	NaN	NaN	m-1
lambda_s	15	NaN	NaN	m-1
sed_sld	0.36	NaN	NaN	(m3/m3)
I_scV 	2.15	NaN	NaN	(-)
I_scT	0	NaN	NaN	deg C
I_scC	1	NaN	NaN	(-)
I_scS	1.5	1.1	1.9	(-)
I_scTP	0.59	0.4	0.8	(-)
I_scDOP	1	NaN	NaN	(-)
I_scChl	1	NaN	NaN	(-)
I_scDOC	1	NaN	NaN	(-)
swa_b0	2.5	NaN	NaN	m-1
swa_b1	%.2f	0.8	1.3	m-1
S_res_epi	3.30E-07	7.30E-08	1.82E-06	m d-1 (dry mass)
S_res_hypo	3.30E-08	NaN	NaN	m d-1 (dry mass)
H_sed	0.03	NaN	NaN	m
Psat_Lang	2500	NaN	NaN	mg m-3
Fmax_Lang	8000	5000	10000	mg kg-1
Uz_Sz	0.3	0.1	1	m d-1
Uz_Chl	0.16	0.05	0.5	m d-1
Y_cp	1	NaN	NaN	(-)
m_twty	0.2	0.1	0.3	d-1
g_twty	1.5	1	1.5	d-1
k_sed_twty	2.00E-04	NaN	NaN	d-1
k_dop_twty	0	NaN	NaN	d-1
P_half	0.2	0.2	2	mg m-3
PAR_sat2	3.00E-05	NaN	NaN	mol m-2 s-1
beta_chl2	0.015	NaN	NaN	m2 mg-1
Uz_Chl2	0.16	NaN	NaN	m d-1
m_twty2	0.2	NaN	NaN	d-1
g_twty2	1.5	NaN	NaN	d-1
P_half2	0.2	NaN	NaN	mg m-3
oc_DOC	0.01	NaN	NaN	m2 mg-1
qy_DOC	0.1	NaN	NaN	mg mol-1
''' % (latitude, longitude, atten_coeff)
    if outpath is None:
        return out
    else:
        with open(outpath, 'w') as f:
            f.write(out)

engine = create_engine('postgresql+pg8000://user:password@vm-srv-finstad.vm.ntnu.no/ecco_biwa_db')

# get an unfinished lake 
sqlnotdone = '''select * from kt_orders 
where lastorder is null or 
(extract(epoch from current_timestamp - lastorder) > 5400 and ts_completed is null)
limit 1''' # posgres extract(epoch from ..) is in seconds for interval
order = pd.read_sql(sqlnotdone, engine)
if len(order) == 0:
    os._exit(1)
ebint, lastorder, ts_completed = order.iloc[0, :3]
h = hex(ebint).rstrip('L').lstrip('0x')
eh = h
while len(eh) < 6:
    eh = '0' + eh
d1, d2, d3 = eh[:2], eh[:4], eh[:6]

# mark the lake as checked out
sqlcheckout = '''update kt_orders
set lastorder = current_timestamp
where ebint = %s''' % ebint
_ = engine.execute(sqlcheckout)

# get geographic coordinates
sqlgeographic = '''select * from centroids_geographic_v02
where ebint = %s''' % ebint
longitude, latitude = pd.read_sql(sqlgeographic, engine).iloc[0, :2]

# get bathymetric information
sqlbathymetry = '''select 
maxdepth, area, shapecoef from bathymetry_predicted
where ebint = %s''' % ebint
bathymetry = pd.read_sql(sqlbathymetry, engine)
maxdepth, area, shapecoef = bathymetry.iloc[0, :]

# bathymetry (area by depth)
zz = np.arange(start=0.0, stop=maxdepth, step=0.1)
az = area * (1.0 - zz / maxdepth) ** shapecoef
zz = np.concatenate((zz, np.array([maxdepth])))
az = np.concatenate((az, np.array([0.0])))

# cordex file names
# sqlfnames = '''select m.mm as mm, fa, fb, ya0, ya1, yb0, yb1 
# from fnames as f, mm as m where m.rcm_id = 2 and m.experimenta = 'rcp85' and m.mm = f.mm'''
sqlfnames = '''select m.mm as mm, fa, fb, ya0, ya1, yb0, yb1 
from fnames as f, mm as m 
where (m.mm = 22 or m.mm = 26 or m.mm = 27 or m.mm = 28) 
and m.mm = f.mm'''
fnames = pd.read_sql(sqlfnames, engine)

print('ebint: %s, ebhex: %s' % (ebint, eh))
print('coordinates: (E %6.3f, N %6.3f)' % (longitude, latitude))
print('last attempt: %s' % lastorder)
print('morphology used: area (%4.2f km2), max depth (%3.1f m), shape (%4.2f)'
      % (area / 1.0e6, maxdepth, shapecoef))
print ('will do the following:')
for mm, fa, fb, ya0, ya1, yb0, yb1 in fnames.itertuples(index=False):
    print('  mm: %s, years: %s-%s' % (mm, ya0, yb1))
    print('  fa: %s' % fa)
    print('  fb: %s' % fb)
# def h5pair(vname, ebhex, fa, fb, ya0, ya1, yb0, yb1, 
#            prefix='/data/cordex/'):
#     fna = 'Lakes_%s_%s_%s0101-%s1231.h5' % (vname, fa, ya0, ya1)
#     fnb = 'Lakes_%s_%s_%s0101-%s1231.h5' % (vname, fa, ya0, ya1)
#     a = h5py.File(os.path.join(prefix, fna))[ebhex][:]
#     b = h5py.File(os.path.join(prefix, fnb))[ebhex][:]


wd0 = os.path.join('/data/lakes', d1, d2, d3)

# init, par
os.makedirs(wd0)
initp = os.path.join(wd0, 'init') 
mylakeinit(zz, az, outpath=initp)
parp = os.path.join(wd0, 'par')
mylakepar(longitude, latitude, outpath=parp)

flag = True

# cycle mm
for mm, fa, fb, ya0, ya1, yb0, yb1 in fnames.itertuples(index=False):
    wd = os.path.join(wd0, '%02d' % mm)
    print(wd)
    os.makedirs(wd)
    temporarypath = os.path.join(wd, 'temp')
    inputp = os.path.join(wd, 'input')

    ndaysA = (datetime.date(ya1, 12, 31) - datetime.date(ya0, 1, 1)).days + 1
    datesA = [datetime.date(ya0, 1, 1) + datetime.timedelta(d) 
              for d in range(ndaysA)]
    ndaysB = (datetime.date(yb1, 12, 31) - datetime.date(yb0, 1, 1)).days + 1
    datesB = [datetime.date(yb0, 1, 1) + datetime.timedelta(d)
              for d in range(ndaysB)]
    dfA = pd.DataFrame(datesA, columns = ['date'])
    dfB = pd.DataFrame(datesB, columns = ['date'])
    p = '/data/cordex/Lakes' # note 'Lakes' is a file prefix
    dfA['clt'] = h5py.File('%s_clt_%s.h5' % (p, fa), mode='r')[h][:]
    dfB['clt'] = h5py.File('%s_clt_%s.h5' % (p, fb), mode='r')[h][:]
    dfA['hurs'] = h5py.File('%s_hurs_%s.h5' % (p, fa), mode='r')[h][:]
    dfB['hurs'] = h5py.File('%s_hurs_%s.h5' % (p, fb), mode='r')[h][:]
    dfA['pr'] = h5py.File('%s_pr_%s.h5' % (p, fa), mode='r')[h][:]
    dfB['pr'] = h5py.File('%s_pr_%s.h5' % (p, fb), mode='r')[h][:]
    dfA['ps'] = h5py.File('%s_ps_%s.h5' % (p, fa), mode='r')[h][:]
    dfB['ps'] = h5py.File('%s_ps_%s.h5' % (p, fb), mode='r')[h][:]
    dfA['rsds'] = h5py.File('%s_rsds_%s.h5' % (p, fa), mode='r')[h][:]
    dfB['rsds'] = h5py.File('%s_rsds_%s.h5' % (p, fb), mode='r')[h][:]
    dfA['sfcWind'] = h5py.File('%s_sfcWind_%s.h5' % (p, fa), mode='r')[h][:]
    dfB['sfcWind'] = h5py.File('%s_sfcWind_%s.h5' % (p, fb), mode='r')[h][:]
    dfA['tas'] = h5py.File('%s_tas_%s.h5' % (p, fa), mode='r')[h][:]
    dfB['tas'] = h5py.File('%s_tas_%s.h5' % (p, fb), mode='r')[h][:]
    df = pd.concat([dfA, dfB])
    df['clt'] *= 0.01
    df['pr'] *= (60 * 60 * 24)
    df['ps'] *=  0.01
    df['rsds'] *= (60 * 60 * 24 * 1e-6)
    df['tas'] -= 273.15
    ndays = len(datesA) + len(datesB)
    df.index = np.arange(ndays)
    repd = [datesA[0] + datetime.timedelta(d) for d in range(-(365 * 2), ndays)]
    mlyear = np.array([d.year for d in repd])
    mlmonth = np.array([d.month for d in repd])
    mlday = np.array([d.day for d in repd])
    mlndays = 365 + 365 + ndays
    repeati = range(365) + range(365) + range(ndays)
    spacer = np.repeat([0], repeats = ndays)[repeati].reshape((mlndays, 1))
    mli = np.concatenate((mlyear.reshape((mlndays, 1)),
                          mlmonth.reshape((mlndays, 1)), 
                          mlday.reshape((mlndays, 1)), 
                          df['rsds'][repeati].reshape((mlndays, 1)),
                          df['clt'][repeati].reshape((mlndays, 1)), 
                          df['tas'][repeati].reshape((mlndays, 1)), 
                          df['hurs'][repeati].reshape((mlndays, 1)), 
                          df['ps'][repeati].reshape((mlndays, 1)), 
                          df['sfcWind'][repeati].reshape((mlndays, 1)), 
                          df['pr'][repeati].reshape((mlndays, 1)), 
                          spacer, spacer, spacer, spacer, 
                          spacer, spacer, spacer, spacer), axis = 1)
    np.savetxt(temporarypath, mli, 
               fmt = ['%i', '%i', '%i', 
                      '%.4g', '%.3f', '%.2f', '%i', '%i', '%.2f', '%.3f', 
                      '%i', '%i', '%i', '%i', 
                      '%i', '%i', '%i', '%i'],
               delimiter = '\t', 
               header = 'mylake input\nYear	Month	Day	Global_rad (MJ/m2)	Cloud_cov (-)	Air_temp (deg C)	Relat_hum (%)	Air_press (hPa)	Wind_speed (m/s)	Precipitation (mm/day)	Inflow (m3/day)	Inflow_T (deg C)	Inflow_C	Inflow_S (kg/m3)	Inflow_TP (mg/m3)	Inflow_DOP (mg/m3)	Inflow_Chla (mg/m3)	Inflow_DOC (mg/m3)')
    with open(temporarypath) as f:
        with open(inputp, 'w') as g:
            g.write(f.read().replace('-99999999', 'NaN'))
    os.unlink(temporarypath)        
    cmd = 'octave --silent mylake.m %s %s %s %s %s %s > %s' % (
        initp, parp, inputp, ya0 - 2, yb1, wd, os.path.join(wd, 'octave-stdout'))
    print(cmd)
    os.system(cmd)
    os.unlink(os.path.join(wd, 'octave-stdout'))
    flag = flag and (os.path.exists(os.path.join(wd, 'His.csv.gz')) and
                     os.path.exists(os.path.join(wd, 'Kzt.csv.gz')) and
                     os.path.exists(os.path.join(wd, 'Qst.csv.gz')) and
                     os.path.exists(os.path.join(wd, 'Tzt.csv.gz')))

if flag:
    sqlreportdone = '''update kt_orders set ts_completed = current_timestamp
 where ebint = %s''' % ebint
    print('updated on kt_orders')
    _ = engine.execute(sqlreportdone)
                     





