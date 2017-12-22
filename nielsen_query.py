import pandas as pd
import sqlalchemy as alch
import pystan
import urllib.parse
import itertools
import os
import numpy as np
from google.cloud import storage as gstore

# ### 1. Set up database connection
host = 'localhost'
port = '3306'
user = 'thg_user'
password = urllib.parse.quote_plus('Thg@1234')
db = 'nielsen_recode'


def query_server(logger, writer, host, port, user, password, db, categories,
                 category_regroup=None, min_obs_dollars=0, min_obs_units=0,
                 min_acv=0, min_upc_dollars=0, min_upc_units=0, min_periods=0,
                 eval_period_start=0, eval_period_end=0,
                 min_brand_dollars_in_period=0,
                 excluded_brands=0):
    query_string = f"mysql+pymysql://{user}@{host}:{port}/{db}?password={password}" 
    logger.log('Begin querying server with URI {}'.format(query_string))
    engine = alch.create_engine(query_string)
    logger.log('Engine created')

    base_sql = ("SELECT * FROM (`obs_table` INNER JOIN `upc_table` ON upc_table.`upc_id`=obs_table.upc_id "
                "JOIN brand_table ON upc_table.brand_id=brand_table.brand_id) "
                "WHERE `thg_category` IN ")
    category_list = ('(' + ' '.join(["'" + cat + "'" + ',' for cat in categories[:-1]]) + 
                     "'" + categories[-1] + "'" + ')')
    query = base_sql + category_list
    logger.log('Generated query: "{}"'.format(base_sql))

    logger.log('Starting pull...')
    try:
        full_table = pd.read_sql_query(query, engine)
    except MemoryError as e:
        logger.log('Query results too large to fit in memory!')
        raise e
    logger.log('Pull complete')


    ## Includes duplicated columns from the SQL join, need to drop them
    full_table = full_table.loc[:, ~full_table.columns.duplicated()]

    def count_table(table):
        """Print out number of unique observations, upcs, brands."""
        logger.log('Number of observations: {}'.format(len(table)))
        logger.log('Number of UPCs: {}'.format(len(table['upc_id'].unique())))
        logger.log('Number of Brands: {}'.format(len(table['brand_id'].unique())))
    count_table(full_table)
    # This is the maximum universe that we are within. In actuality, some of the
     #observations are spurious, meaning zero or almost zero units sold, some of the
     # upcs were around for only a few periods and so cannot be analyzed, and some 
     #of the brands have so little sales that it is not worth our time to 
     #investigate if they are experience organic growth. So, we need to do some filtering!
    ## Get a list of passing obs_id
    pass_idx = ((full_table['dollars'] > min_obs_dollars) &
                (full_table['units'] > min_obs_units) &
                (full_table['acv'] > min_acv))
    passing_obs = full_table.loc[pass_idx, 'obs_id']
    logger.log('Fraction of Observations which pass minimums: {:0.02f}'.format(len(passing_obs)/len(pass_idx)))

    full_table = full_table.loc[pass_idx]
    ## Weed out observations which are not part of the longest run
    def in_longest_run(tseries):
        """Finds longest consequtive sequence"""
        tdf = pd.DataFrame({'orig' : tseries, 'prev' : tseries})
        tdf['prev'] = tdf['prev'].shift(1) 
        tdf.iloc[0, 1] = tdf.iloc[0, 0] - 1 # First value in series automatically counts as consequtive
        tdf['breaks'] = ~(tdf['orig'] == tdf['prev'] + 1)
        tdf['subseq_num'] = tdf['breaks'].cumsum()
        longest_run = tdf['subseq_num'].mode()[0] 
        # Mode always returns a series, and we want first longest run if equal length.
        return tdf['subseq_num'] == longest_run
    full_table = full_table.sort_values(['upc_id', 't'])
    conseq = full_table.groupby('upc_id', sort=False)['t'].transform(in_longest_run)
    full_table['conseq']  = conseq
    passing_obs_conseq = full_table.loc[conseq, 'obs_id']
    logger.log('Fraction of Observations which pass longest run {:0.02f} '.format(
        len(passing_obs_conseq)/len(conseq)))
    full_table = full_table.loc[conseq]

    upc_check_funcs = {'dollars' : (lambda ds: ds.sum() > min_upc_dollars),
                  'units' : (lambda us: us.sum() > min_upc_units),
                  't' : (lambda ps: len(ps) >= min_periods)
                 }
    # Note that the bool values are coerced to floats b/c of dtype
    upc_checks = full_table.groupby('upc_id').agg(upc_check_funcs) 
    # 1.0 if passing all checks, 0.0 if any fail
    upc_checks = upc_checks.prod(axis=1)

    passing_upc_ids = upc_checks.index[upc_checks > 0]
    print("Fraction of UPCs which pass minimums: {:0.02f}".format(len(passing_upc_ids)/len(upc_checks)))
    pass_idx_upc = full_table['upc_id'].isin(passing_upc_ids)

    # Reduce to passing upcs only
    full_table = full_table.loc[pass_idx_upc]

    ## Filter brands based on metrics
    ## Brand inclusion rules are often based on performance in the last year,
    ## so we set up so that can be done easily.

    ## Potential slight bug here -> even if 
    ## setting min_brand_dollars_in_period to 0, brand still must have at least 1 UPC in period
    ## to be included in query. 

    brand_check_table = full_table.loc[
        (full_table['t'] >= eval_period_start) & (full_table['t'] <= eval_period_end)
    ]
    brand_check_funcs = {
        'dollars' : lambda ds: ds.sum() > min_brand_dollars_in_period, 
        'brandhigh' : lambda b: b.iloc[0] not in excluded_brands
    }
    brand_check = brand_check_table.groupby('brand_id').agg(brand_check_funcs)
    brand_check = brand_check.prod(axis = 1)
    passing_brand_ids = brand_check.index[brand_check > 0]
    pass_idx_brand = full_table['brand_id'].isin(passing_brand_ids)
    ## Reduce to passing brands
    full_table = full_table.loc[pass_idx_brand]
    count_table(full_table)
    for cat, replace_cat in category_regroup.items():
        logger.log('Replacing {} with {} in thg_category'.format(cat, replace_cat))
        full_table['thg_category'].replace(cat, replace_cat, inplace=True)
    fin_cats = list(full_table['thg_category'].unique())
    logger.log('Final category list = {}'.format(fin_cats))
    writer.write(fin_cats, 'util', 'category_list.txt')

    ## Push up to google cloud
    for cat in fin_cats:
        writer.makedir(cat)
    ## Outputing into Stan format. 
    full_table = full_table.sort_values(['brand_id', 'upc_id', 't'], kind = 'mergesort')
    for cat in fin_cats:
        logger.log('Starting upload for {}'.format(cat))
        cat_table = full_table.loc[full_table['thg_category'] == cat]
        brand_tables = cat_table.groupby('brand_id', sort = False)
        for brand_id, table in brand_tables:
            start_t = table.groupby('upc_id')['t'].transform('min')
            time_since_start = table['t'] - start_t
            def upc_in_brand():
                #Small closure function to count unique upcs in brand.
                n = 0
                def count_upc():
                    nonlocal n
                    n = n + 1
                    return n
                return count_upc
            count = upc_in_brand()
            upc_in_brand = table.groupby('upc_id')['upc_id'].transform(lambda s: count())
            obs_per_upc = upc_in_brand.value_counts(sort=False)
            stan_dict = {
                't' : np.array(table['t']),
                'time_since_start' : np.array(time_since_start),
                'dollars' : np.array(table['dollars']),
                'units' : np.array(table['units']),
                'acv' : np.array(table['acv']),
                'obs_id' : np.array(table['obs_id']),
                'upc_id' : np.array(table['upc_id']),
                'brand_id' : np.array(table['brand_id']),
                'avgunitprice' : np.array(table['avgunitprice']),
                'upc_in_brand' : np.array(upc_in_brand),
                'N_upc' : upc_in_brand.max(),
                'N_obs' : len(table),
                'obs_per_upc' : np.array(obs_per_upc)
            }
            outfile_name = '{:0.0f}'.format(brand_id) + '_'  + table['brandhigh'].mode()[0] + '.R'
            outfile_name = outfile_name.replace(' ', '_') # Remove spaces in brandnames for easier file manip
            pystan.misc.stan_rdump(stan_dict, 'working_pystan_file.R')
            writer.write(directory=cat, filename=outfile_name,
                         upload_file='working_pystan_file.R')

    full_table.to_csv('working_table_file.csv')
    writer.write(directory='util', filename='fulltable.csv',
                 upload_file='working_table_file.csv')
