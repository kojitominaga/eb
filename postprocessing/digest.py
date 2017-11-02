import os
import numpy as np
import pandas as pd
import pg8000
from sqlalchemy import create_engine
import datetime
import scipy.optimize
import scipy.interpolate

# templatenumeric = np.zeros((10, 1)) * np.nan
# templatebool = np.zeros((10, 1), dtype=bool) 

engine = create_engine('postgresql+pg8000://user:password@vm-srv-finstad.vm.ntnu.no/ecco_biwa_db')

sqlagnotdone = "select ebint from mylake.kt_orders o where o.lastaggdone is null"
orders = pd.read_sql(sqlagnotdone, engine)
if len(orders) == 0:
    os._exit(1)
ee = orders['ebint'].values
# ## just for COMSAT
# comsat = pd.read_csv('COMSAT_utf8.csv')
# ee = comsat.ebint.values

## get relevant mms
sqlgetmm = """select mm, ya0 as y0, yb1 as y1 
from mylake.mm
where mm = 22 or mm = 26 or mm = 27 or mm = 28"""
mms = pd.read_sql(sqlgetmm, engine)

first_time = True

# ee = ee[:100]

## looped version
for ei, e in enumerate(ee):
    print('%s: %s' % (ei, e))
    h = hex(e).rstrip('L').lstrip('0x')
    eh = h
    while len(eh) < 6:
        eh = '0' + eh
    d1, d2, d3 = eh[:2], eh[:4], eh[:6]
    # ## acknowledge it started
    # sqlupdate1 = """update kt_orders set lastaggtry = current_timestamp 
    #                 where ebint = %s""" % e
    # _ = engine.execute(sqlupdate1)
    
    
    # going through initial decade as the reference
    print('this current version does NOT work for multiple RCMs, reference to the historical needs to be RCM specific')
    mm0, y00, y10 = mms.iloc[0, :]
    wd0 = os.path.join('/data/lakes', d1, d2, d3, '%02d' % mm0)
    ndays0 = (datetime.date(y10, 12, 31) - datetime.date(y00, 1, 1)).days + 1
    dates0 = [datetime.date(y00, 1, 1) + datetime.timedelta(d) 
              for d in range(ndays0)]
    dti0 = pd.DatetimeIndex(dates0)
    period0 = pd.PeriodIndex(dates0, freq='D')
    if not os.path.exists(os.path.join(wd0, 'Tzt.csv.gz')):
        continue
    t0 = pd.read_csv(os.path.join(wd0, 'Tzt.csv.gz'), 
                     compression='gzip', header=None)
    t0.index = period0
    h0 = pd.read_csv(os.path.join(wd0, 'His.csv.gz'), 
                     compression='gzip', header=None)
    h0.index = period0
    q0 = pd.read_csv(os.path.join(wd0, 'Qst.csv.gz'), 
                     compression='gzip', header=None)
    q0.index = period0
    k0 = pd.read_csv(os.path.join(wd0, 'Kzt.csv.gz'), 
                     compression='gzip', header=None)
    k0.index = period0
    # weqs0 = h0.iloc[:, 1] * h0.iloc[:, 5] / 1000 
    # Hs * rho_snow / rho_freshwater rho_fw
    icesnowatt0 = np.exp(-5 * h0.iloc[:, 0]) * np.exp(-15 * h0.iloc[:, 1])
    icesnowatt0.iloc[np.logical_not(h0.iloc[:, 6] == 1).values] = 1
    # exp(-lambda_i * Hi) * exp(-lambda_s * (rho_fw/rho_snow)*weqs)
    sw15 = q0.iloc[:, 0] * icesnowatt0 * 0.45 * np.exp(-1 * 15) if \
           t0.shape[1] >= 15 \
           else pd.DataFrame(np.zeros((ndays0, 1)) * np.nan)
    sw15.index = period0
    sw15 = np.log(sw15)
    sw15 = sw15.resample('M')
    sw15 = sw15.groupby(sw15.index.month).mean()
    fr15pr = sw15 # Frode's PAR level at 15m
    sw05 = q0.iloc[:, 0] * icesnowatt0 * 0.45 * np.exp(-1 * 5) if \
           t0.shape[1] >= 5 \
           else pd.DataFrame(np.zeros((ndays0, 1)) * np.nan)
    sw05.index = period0
    sw05 = np.log(sw05)
    sw05 = sw05.resample('M')
    sw05 = sw05.groupby(sw05.index.month).mean()
    # Frode's PAR level at 05m    
    fr15tr = (t0.iloc[:, 14] + t0.iloc[:, 15]) / 2 if \
             t0.shape[1] >= 16 \
             else pd.DataFrame(np.zeros((ndays0, 1)) * np.nan)
    fr15tr.index = period0
    fr15tr = fr15tr.resample('M')
    fr15troriginal = fr15tr
    fr15tr[fr15tr < 3.98] = np.nan
    grouped15 = fr15tr.groupby(fr15tr.index.month)
    allwarm15 = np.array([np.logical_not(np.isnan(g)).sum() == 10 
                          for k, g in grouped15])
    fr15tr = grouped15.mean()
    fr15tr[np.logical_not(allwarm15)] = np.nan
    ## Frode's 15m temperature ref
    fr05tr = (t0.iloc[:, 4] + t0.iloc[:, 5]) / 2 if \
             t0.shape[1] >= 6 \
             else pd.DataFrame(np.zeros((ndays0, 1)) * np.nan)
    fr05tr.index = period0
    fr05tr = fr05tr.resample('M')
    fr05troriginal = fr05tr
    fr05tr[fr05tr < 3.98] = np.nan
    grouped05 = fr05tr.groupby(fr05tr.index.month)
    allwarm05 = np.array([np.logical_not(np.isnan(g)).sum() == 10 
                          for k, g in grouped05])
    fr05tr = grouped05.mean()
    fr05tr[np.logical_not(allwarm05)] = np.nan
    ## Frode's 05m temperature ref

    print('this current version does NOT work with varying extinction coefficients')
    # Hs * rho_snow / rho_freshwater rho_fw
    icesnowatt = np.exp(-5 * h0.iloc[:, 0]) * np.exp(-15 * h0.iloc[:, 1])
    icesnowatt.iloc[np.logical_not(h0.iloc[:, 6] == 1).values] = 1
    # exp(-lambda_i * Hi) * exp(-lambda_s * (rho_fw/rho_snow)*weqs)
    sw15 = q0.iloc[:, 0] * icesnowatt * 0.45 * np.exp(-1 * 15) if \
           t0.shape[1] >= 15 \
           else pd.DataFrame(np.zeros((ndays0, 1)) * np.nan)
    sw15.index = period0
    sw15 = np.log(sw15)
    sw15 = sw15.resample('M')
    sw15 = sw15.groupby(sw15.index.month).mean()
    fr15pr = sw15
    sw05 = q0.iloc[:, 0] * icesnowatt * 0.45 * np.exp(-1 * 5) if \
           t0.shape[1] >= 5 \
           else pd.DataFrame(np.zeros((ndays0, 1)) * np.nan)
    sw05.index = period0
    sw05 = np.log(sw05)
    sw05 = sw05.resample('M')
    sw05 = sw05.groupby(sw05.index.month).mean()
    fr05pr = sw05
    
    # e: ebint
    # mm: myriadmyriad
    # sim: sim_id
    # y0: first year of the decade
    # y1: last year of the decade
    # ndays: number of days in the decade
    # dti: pd.DatetimeIndex of the dates in the decade
    # t: temperature profile [day x depth]
    # h: ice variables [day x variable]
    # q: heat variables [day x variable]
    # k: 
    # r: rho, water density [day x depth]
    # dr: delta rho [day x (depth - 1)]
    # stratified: bool [day]
    # pycd: max pycnocline or mixing depth [day]
    #       if mixed completely lake's maximum depth
    # 

    # pcn00 ... pcn12: annual and monthly mixing depth [day]
    # tsw00 ... tws12: annual and monthly temperature surface water [day]

    for mm, y0, y1 in mms.itertuples(index=False):
        sim = mm * 2e7 + e  # the sim_id
        wd = os.path.join('/data/lakes', d1, d2, d3, '%02d' % mm)
        # wd = os.path.join('/Users/kojito/Desktop/lakes', d1, d2, d3, '%02d' % mm)
        ndays = (datetime.date(y1, 12, 31) - datetime.date(y0, 1, 1)).days + 1
        dates = [datetime.date(y0, 1, 1) + datetime.timedelta(d) 
                 for d in range(ndays)]
        dti = pd.DatetimeIndex(dates)
        period = pd.PeriodIndex(dates, freq='D')
        if not os.path.exists(os.path.join(wd, 'Tzt.csv.gz')):
            continue
        t = pd.read_csv(os.path.join(wd, 'Tzt.csv.gz'), 
                        compression='gzip', header=None)
        t.index = period
        h = pd.read_csv(os.path.join(wd, 'His.csv.gz'), 
                        compression='gzip', header=None)
        h.index = period
        q = pd.read_csv(os.path.join(wd, 'Qst.csv.gz'), 
                        compression='gzip', header=None)
        q.index = period
        k = pd.read_csv(os.path.join(wd, 'Kzt.csv.gz'), 
                        compression='gzip', header=None)
        k.index = period
        if np.logical_not(np.isnan(t.values)).sum() == 0:
            continue 
        # rho density
        r = 999.842594 + t * (6.793952e-2 + t * (-9.09529e-3 + 
            t * (1.001685e-4 + t * (-1.120083e-6 + t * 6.536332e-9))))
        # delta rho
        dr = np.concatenate((np.repeat(np.array([[0]]), r.shape[0], axis=0), 
                             r.iloc[:, :-1].values - r.iloc[:, 1:].values), 
                            axis=1)
        dr = np.abs(dr)
        drd0, drd1 = dr.shape
        drthreshold = 0.05
        dr[dr < drthreshold] = 0
        dr[t.values < 3.98] = 0 
        ## this .values is important as pd.DataFrame < 3.98 does not work
        drz = dr * np.arange(drd1).reshape((1, drd1)).repeat(drd0, axis=0)
        pycd = drz.sum(axis=1) / dr.sum(axis=1)
        stratified = np.logical_not(np.isnan(pycd))
        stratified = pd.DataFrame(stratified)
        stratified.index = dti
        pycd[np.logical_not(stratified.iloc[:, 0].values)] = drd1 - 1.0 # if not stratified mix to the bottom 
        pycd = pd.DataFrame(pycd)
        pycd.index = dti

        # pcd01-pcd12 # depth
        pcd = pycd.resample('M').values.reshape((10, 12))
        # pcd00
        pcd00 = pcd.mean(axis=1).reshape((10, 1))
        # pcn01-pcn12 # number of stratified days in a month
        pcn = stratified.resample('M', how='sum').values.reshape((10, 12))
        # pcn00
        pcn00 = pcn.mean(axis=1).reshape((10, 1))

        # tsw01-tsw12
        tsw = t.iloc[:, 0].resample('M').reshape((10, 12))
        # tsw00
        tsw00 = tsw.mean(axis=1).reshape((10, 1))

        ndice = h.iloc[:, 6].resample('M', how='sum').values.reshape((10, 12))
        ndice00 = ndice.sum(axis=1).reshape((10, 1))



        

        ########
        # first work on warm stratification and lake categories
        # ice will be done later and only in between years
        ########

        # csomeice: is there at least one ice-covered day?
        # clongstrat: is there unusually long stratification? 
        #             (if yes stats mix... are meaningless)
        # # cwarm: does it at least go up > 3.98 degrees C?
        # maxdoy: doy at which temperature at surface is at its maximum
        # mixspr0: spring mixing doy starts (first day t > 3.98)
        # mixspr1: spring mixing doy ends (if stratification turns on)
        # mixaut0: autumn mixing doy starts (if stratification has been on)
        # mixaut1: autumn mixing doy ends (last day t > 3.98)
        # cwarm = False ## can be sub'ed with "not np.isnan(maxdoy)"
        csomeice = np.zeros((10, 1), dtype=bool) 
        clongstrat = np.zeros((10, 1), dtype=bool) 
        maxdoy =  np.zeros((10, 1)) * np.nan
        mixspr0 =  np.zeros((10, 1)) * np.nan
        mixspr1 =  np.zeros((10, 1)) * np.nan
        mixaut0 =  np.zeros((10, 1)) * np.nan
        mixaut1 =  np.zeros((10, 1)) * np.nan
        # ndmix and nemix ONLY MEANINGFUL for cat 4 or 7
        ndmix =  np.zeros((10, 1)) * np.nan # number of mixing days 
        nemix =  np.zeros((10, 1)) * np.nan # number of mixing events      
        category =  np.zeros((10, 1)) * np.nan
        # lake categories:
        # 0: unknown
        # 1: amictic
        # 2: cold monomictic
        # 3: continuous cold polymictic
        # 4: discontinuous cold polymictic
        # 5: dimictic
        # 6: continuous warm polymictic
        # 7: discontinous warm polymictic
        # 8: warm monomictic

        for yi, thisy in enumerate(t.resample('A').index.year):
            thiscsomeice = False
            # thisclongstrat = False not used
            thismaxdoy = np.nan
            thismixspr0 = np.nan
            thismixspr1 = np.nan
            thismixaut0 = np.nan
            thismixaut1 = np.nan
            thisndmix = np.nan 
            thisnemix = np.nan 

            # ndaysthisyear = pd.to_datetime('%s-12-31' % thisy).dayofyear
            thish = h[h.index.year == thisy]
            
            # I. AMICTIC if completely ice-covered
            if np.all(thish.iloc[:, 6] == 1):
                category[yi, 0] = 1
                # keep everything else default
                continue 
                
            thist = t[t.index.year == thisy]
            s = thist.iloc[:, 0]
            smax = s.max()

            # II. COLD MONOMICTIC if t at maxdoy < 3.98
            if smax < 3.98:
                category[yi, 0] = 2
                # keep everything else default
                continue 
                
            thismaxdoy = s[s == s.max()].index.dayofyear[0] # just pick the 1st
            maxdoy[yi, 0] = thismaxdoy
            
            thiscsomeice = np.any(thish.iloc[:, 6] == 1)
            csomeice[yi, 0] = thiscsomeice
            # needed later unless category = 8
            thisstrat = stratified[stratified.index.year == thisy]
            
            # III. if always stratified put it together with WARM MONOMICTIC
            if np.all(thisstrat):
                category[yi, 0] = 8
                clongstrat[yi, 0] = True
                # keep everything else default
                continue
            
            # check for stratification anyway
            elif np.any(thisstrat):
                thismixspr1 = thisstrat[thisstrat.iloc[:, 0]].index.dayofyear.min() - 1
                thismixaut0 = thisstrat[thisstrat.iloc[:, 0]].index.dayofyear.max() + 1
                ## fix for the situation where the first day > 3.98 is already stratified
                if (s[s > 3.98].index.dayofyear.min() - 1) == thismixspr1:
                    thismixspr0 = thismixspr1
                else:
                    thismixspr0 = thisstrat[np.logical_not(thisstrat.iloc[:, 0]) & \
                                            (thisstrat.index.dayofyear < thismaxdoy) & \
                                            (s > 3.98).values] \
                                  .index.dayofyear.min()
                if (s[s > 3.98].index.dayofyear.max() + 1) == thismixaut0:
                    thismixaut1 = thismixaut0
                else:
                    thismixaut1 = thisstrat[np.logical_not(thisstrat.iloc[:, 0]) & \
                                            (thisstrat.index.dayofyear > thismaxdoy) & \
                                            (s > 3.98).values] \
                                  .index.dayofyear.max()
                mixspr0[yi, 0] = thismixspr0 # can be overwritten in IV
                mixspr1[yi, 0] = thismixspr1
                mixaut0[yi, 0] = thismixaut0
                mixaut1[yi, 0] = thismixaut1 # can be overwritten in IV
            # else: all thismix.... remains np.nan

            # # IV. if either end (1 January or 31 December) is warmer than 3.98 then 
            # #     put together with WARM MONOMICTIC
            # # the year started warm 
            # flag4a = np.all(s[s.index.dayofyear < thismaxdoy] > 3.98):
            # # the year ended warm
            # flag4b = np.all(s[s.index.dayofyear > thismaxdoy] > 3.98):
            # if flag4a and flag4b:
            #     mixspr0[yi, 0] = thismixspr0 = np.nan
            #     mixaut1[yi, 0] = thismixaut1 = np.nan
            #     clongstrat[yi, 0] = True
            #     # keep everything else default
            #     if 
            #     continue
                
            # V. if always mixed (i.e., no warm stratification) either 
            #    CONTINUOUS COLD POLYMICTIC or CONTINUOUS WARM POLYMICTIC
            if np.all(np.logical_not(thisstrat)):
                category[yi, 0] = 3 if thiscsomeice else 6
                continue

            # check the stability of warm stratification
            st = thisstrat[(thisstrat.index.dayofyear > thismixspr1) \
                           & (thisstrat.index.dayofyear < thismixaut0)]

            # VI. if stratification is stable either
            #     DIMICTIC or WARM MONOMICTIC
            #     otherwise DISCONTINOUS COLD POLYMICTIC or 
            #               DISCONTINOUS WARM POLYMICTIC, 
            #               and calculate ndmix and nemix
            if np.all(st):
                category[yi, 0] = 5 if thiscsomeice else 8
            elif st.shape[0] <= 4: ## too short stratification 
                category[yi, 0] = 4 if thiscsomeice else 7
                thisndmix = 0 # overwrite on np.nan
                thisnemix = 0 # overwrite on np.nan
            elif np.all(st.iloc[4:, 0]) and st.shape[0] > 10: 
                # if only unstable in the beginning consider stable
                category[yi, 0] = 5 if thiscsomeice else 8
            else:
                category[yi, 0] = 4 if thiscsomeice else 7
                # get number of days and events of mixing
                thisndmix = 0 # overwrite on np.nan
                thisnemix = 0 # overwrite on np.nan
                # only consider stratification period > 4 days
                # ignore the first 4 days for initial unstable stratification
                thisndmix = np.logical_not(st.iloc[4:, 0]).sum()
                # for the number of events only count the number of first days
                if thisndmix >= 1:
                    previous = False 
                    for d in np.logical_not(st.iloc[:, 0]):
                        if previous: # if mixed on previous day do not accrue nemix
                            if not d:
                                previous = False # mixing ended on this day
                        elif d:
                            thisnemix += 1
                            previous = True # mixing started on this day
            ndmix[yi, 0] = thisndmix
            nemix[yi, 0] = thisnemix

        ######
        # now work on ice related matters 
        ######
    
        # y0 missing
        # record for winter y0 to y1 is recorded in y1
        
        # iceon: first ice-covered doy of the year before (can be negative)
        # iceof: last ice-covered doy of the year
        # icedu: number of days inclusive between iceon and iceof
        # ndtha: number of days between iceon and iceof not ice-covered
        # netha: number of events between iceon and iceof not ice-covered
        # clongice: unusually long ( > 365 days) of ice-cover 
        #           in the year or the year before or the year after
        #           above stats not meaningful
        iceon =  np.zeros((10, 1)) * np.nan
        iceof =  np.zeros((10, 1)) * np.nan
        icedu =  np.zeros((10, 1)) 
        icedu[0, 0] = np.nan
        ndtha =  np.zeros((10, 1)) * np.nan
        netha =  np.zeros((10, 1)) * np.nan
        clongice = np.zeros((10, 1), dtype=bool) 

        for yi, thisy in enumerate(t.resample('A').index.year):
            if yi == 0:
                continue
            md0 = maxdoy[yi - 1, 0] # maxdoy the year before
            md1 = maxdoy[yi, 0] # maxdoy the year
            
            # I. if neither md0 and md1 exists, then completely ice-covered
            if np.isnan(md0) and np.isnan(md1):
                icedu[yi, 0] = 365
                clongice[yi, 0] = True
                continue
                
            # II. if only md0 is non-existent, record iceon
            elif np.isnan(md0):
                icedu[yi, 0] = 365
                iceon[yi, 0] = (h[(((h.index.year == (thisy - 1)) \
                                    & (h.index.dayofyear > md0)) \
                                   | (h.index.year == thisy))\
                                  & (h.iloc[:, 6] == 1)] \
                                .index.min().start_time.to_pydatetime().date() \
                                - datetime.date(thisy - 1, 12, 31)).days
                # either last year days since maxdoy or this year, 
                # AND ice-covered --- take the date and count how many days
                # from the New Year's Eve the year before
                clongice[yi, 0] = True
                continue
            
            # III. if only md1 is non-existent, record iceof, 
            #      and do complex calculation of icedu
            elif np.isnan(md1):
                iceof[yi, 0] = thisiceof = h[(h.index.year == thisy) \
                                             & (h.index.dayofyear < md1) \
                                             & (h.iloc[:, 6] == 1)] \
                    .index.max().dayofyear
                # find the year (immediately before this year) 
                # that did not have complete ice cover
                # if not found, then record icedu = 365
                nonfully = None # the yi for the year that does not have complete ice-cover
                                # defaults to 1, which is 2nd -- this is used 
                                # only if icecovered from the beginning year
                for yii in range(yi-1, 0, -1):
                    if not np.isnan(maxdoy[yii, 0]):
                        nonfully = yii
                        break
                if nonfully is None:
                    icedu[yi, 0] = 365
                else: 
                    icedu[yi, 0] = min(365, (365 - iceon[nonfully, 0]) + 1 + thisiceof)
                clongice[yi, 0] = True
                continue
                
            # IV. if both md0 and md1 are available, 
            #     calculate iceon like II.
            #     calculate iceof like III
            #     and also calculate ndtha and ndtha
            else:
                # all days with ice (may be intermittent)
                icx = h[(((h.index.year == (thisy - 1)) \
                         & (h.index.dayofyear > md0)) \
                        | ((h.index.year == thisy) \
                           & (h.index.dayofyear < md1))) \
                       & (h.iloc[:, 6] == 1)]
                if icx.shape[0] == 0:
                    # this means no ice
                    continue

                iceon[yi, 0] = (h[(((h.index.year == (thisy - 1)) \
                                    & (h.index.dayofyear > md0)) \
                                   | (h.index.year == thisy))\
                                  & (h.iloc[:, 6] == 1)] \
                                .index.min().start_time.to_pydatetime().date() \
                                - datetime.date(thisy - 1, 12, 31)).days
                thisiceon = iceon[yi, 0]
                # either last year days since maxdoy or this year, 
                # AND ice-covered --- take the date and count how many days
                # from the New Year's Eve the year before

                iceof[yi, 0] = h[(h.index.year == thisy) \
                                 & (h.index.dayofyear < md1) \
                                 & (h.iloc[:, 6] == 1)] \
                    .index.max().dayofyear
                thisiceof = iceof[yi, 0]
                # (EITHER last year since maxdoy OR this year until maxdoy) 
                # AND ice-covered
                
                ic = h[icx.index.min():icx.index.max()] # works charming
                icedu[yi, 0] = ic.shape[0]
                ic = ic.iloc[:, 6]
                ic = (ic == 1).values

                # get number of days and events of mixing
                thisndtha = 0 # overwrite on np.nan
                thisnetha = 0 # overwrite on np.nan
                thisndtha = np.logical_not(ic).sum()
                # for the number of events only count the number of first days
                if thisndtha > 1:
                    previous = False 
                    for d in np.logical_not(ic):
                        if previous: # if not ice-covered on previous day do not accrue 
                            if not d:
                                previous = False # open-water ended on this day
                        elif d:
                            thisnetha += 1
                            previous = True # open-water started on this day
                ndtha[yi, 0] = thisndtha
                netha[yi, 0] = thisnetha            
        
        ######
        # other variables
        ######

        ## Frode's variables
        ## reference fr15t.loc[month]
        ## target t.reample('M').iloc[13, :]
        fr15tall = np.nan * fr15troriginal
        fr15tall.index = t.resample('M').index
        for monthi in range(fr15tall.shape[0]):
            m = fr15tall.index.month[monthi]
            yt = fr15tr.loc[m]
            if np.any(np.isnan(yt)):
                continue
            if yt < 3.98:
                continue
            y = t.resample('M').iloc[monthi, :]
            x = np.arange(t0.shape[1]) + 0.5
            if np.all((y - yt) > 0):
                continue
            if np.all((y - yt) < 0):
                fr15tall[monthi] = 0
                continue
            a = max(x[((y - yt) > 0).values])
            b = min(x[((y - yt) < 0).values])
            f = scipy.interpolate.interp1d(x, y - yt)
            fr15tall[monthi] = scipy.optimize.bisect(f, a, b, xtol=0.001)
        # tttt = fr15tall.groupby(fr15tall.index.month).mean()
        # print(tttt[tttt.notnull()])
        fr15t = fr15tall.values.reshape((10, 12))

        fr05tall = np.nan * fr05troriginal
        fr05tall.index = t.resample('M').index
        for monthi in range(fr05tall.shape[0]):
            m = fr05tall.index.month[monthi]
            yt = fr05tr.loc[m]
            if np.any(np.isnan(yt)):
                continue
            if yt < 3.98:
                continue
            y = t.resample('M').iloc[monthi, :]
            x = np.arange(t0.shape[1]) + 0.5
            if np.all((y - yt) > 0):
                continue
            if np.all((y - yt) < 0):
                fr05tall[monthi] = 0
                continue
            a = max(x[((y - yt) > 0).values])
            b = min(x[((y - yt) < 0).values])
            f = scipy.interpolate.interp1d(x, y - yt)
            fr05tall[monthi] = scipy.optimize.bisect(f, a, b, xtol=0.001)
        # tttt = fr05tall.groupby(fr05tall.index.month).mean()
        # print(tttt[tttt.notnull()])       
        fr05t = fr05tall.values.reshape((10, 12))

        icesnowatt = np.exp(-5 * h.iloc[:, 0]) * np.exp(-15 * h.iloc[:, 1])
        icesnowatt.iloc[np.logical_not(h.iloc[:, 6] == 1).values] = 1
        incomingsw = q.iloc[:, 0] * icesnowatt * 0.45
        incomingsw.index = period
        fr15pall = np.nan * fr05troriginal        
        fr15pall.index = t.resample('M').index
        fr05pall = np.nan * fr05troriginal
        fr05pall.index = t.resample('M').index
        for monthi in range(fr05tall.shape[0]):
            m = fr05tall.index.month[monthi]
            year = fr05tall.index.year[monthi]
            refsw15 = fr15pr.loc[m]
            refsw05 = fr05pr.loc[m]
            sub = incomingsw[(incomingsw.index.year == year) & 
                             (incomingsw.index.month == m)]
            fr15pall[monthi] = np.mean(-1 * np.log(np.exp(refsw15) / sub))
            fr05pall[monthi] = np.mean(-1 * np.log(np.exp(refsw05) / sub))
        fr15p = fr15pall.values.reshape((10, 12))
        fr05p = fr05pall.values.reshape((10, 12))
        fr15p[np.isinf(fr15p)] = np.nan
        fr15p[fr15p > t.shape[1]] = np.nan
        fr05p[np.isinf(fr05p)] = np.nan
        fr05p[fr15p > t.shape[1]] = np.nan

        # form into shape
        a = np.concatenate((tsw00, tsw, pcd00, pcd, pcn00, pcn, ndice00, ndice, 
                            csomeice, clongstrat, clongice, 
                            maxdoy, mixspr0, mixspr1, mixaut0, mixaut1,
                            ndmix, nemix, category, 
                            icedu, iceon, iceof, ndtha, netha, 
                            fr15t, fr05t, fr15p, fr05p), axis=1)
        dim0 = 10
        # dim0 = 10 + 4
        # dim1 = a.shape[1]
        # a100 = a.mean(axis=0).reshape((1, dim1))
        # a101 = np.median(a, axis=0).reshape((1, dim1))
        # a102 = a.min(axis=0).reshape((1, dim1))
        # a103 = a.max(axis=0).reshape((1, dim1))
        # y = np.array(range(10) + [100, 101, 102, 103]).reshape((dim0, 1))
        # b = np.concatenate((a, a100, a101, a102, a103), axis=0)
        y = np.array(np.arange(10).reshape((dim0, 1)))
        b = a
        c = np.concatenate((np.array([[sim]]).repeat(dim0, axis=0), y, b), 
                           axis=1)
        if first_time:
            result = c
            first_time = False
        else:
            result = np.concatenate((result, c), axis=0)


cols = ['sim_id', 'y',
        'tws00', 'tws01', 'tws02', 'tws03', 'tws04', 'tws05', 'tws06', 
        'tws07', 'tws08', 'tws09', 'tws10', 'tws11', 'tws12',  
        'pcd00', 'pcd01', 'pcd02', 'pcd03', 'pcd04', 'pcd05', 'pcd06',  
        'pcd07', 'pcd08', 'pcd09', 'pcd10', 'pcd11', 'pcd12',  
        'pcn00', 'pcn01', 'pcn02', 'pcn03', 'pcn04', 'pcn05', 'pcn06',  
        'pcn07', 'pcn08', 'pcn09', 'pcn10', 'pcn11', 'pcn12',
        'ice00', 'ice01', 'ice02', 'ice03', 'ice04', 'ice05', 'ice06',  
        'ice07', 'ice08', 'ice09', 'ice10', 'ice11', 'ice12', 
        'csomeice', 'clongstrat', 'clongice', 
        'maxdoy', 'mixspr0', 'mixspr1', 'mixaut0', 'mixaut1', 
        'ndmix', 'nemix', 'category', 
        'icedu', 'iceon', 'iceof', 'ndtha', 'netha', 
        'f15t01', 'f15t02', 'f15t03', 'f15t04', 'f15t05', 'f15t06', 
        'f15t07', 'f15t08', 'f15t09', 'f15t10', 'f15t11', 'f15t12', 
        'f05t01', 'f05t02', 'f05t03', 'f05t04', 'f05t05', 'f05t06', 
        'f05t07', 'f05t08', 'f05t09', 'f05t10', 'f05t11', 'f05t12', 
        'f15p01', 'f15p02', 'f15p03', 'f15p04', 'f15p05', 'f15p06', 
        'f15p07', 'f15p08', 'f15p09', 'f15p10', 'f15p11', 'f15p12', 
        'f05p01', 'f05p02', 'f05p03', 'f05p04', 'f05p05', 'f05p06', 
        'f05p07', 'f05p08', 'f05p09', 'f05p10', 'f05p11', 'f05p12']

result = pd.DataFrame(result, columns=cols)
                                        
## still need to do something with the iceon after 31st December

## comsat
# In [63]: result22 = result
# In [64]: result22['ebint'] = result.sim_id - 440000000
# In [65]: agg = result22.groupby('sim_id').mean()
# In [66]: agg.to_csv('comsat_DMI_mean-2001-2010.csv')
# In [67]: m = agg.merge(comsat)

result['csomeice'] = result['csomeice'].astype(bool)
result['clongstrat'] = result['clongstrat'].astype(bool)
result['clongice'] = result['clongice'].astype(bool)
result.to_csv('test20151110a.csv')
result.to_sql(name='sim3', con=engine, schema='mylake',
              if_exists='append', index=False)



