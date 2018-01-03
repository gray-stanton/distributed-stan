#!/usr/bin/python3
import pandas as pd
import os
import csv
import shlex
import subprocess as sp
import sys
import string
import pystan
from collections import OrderedDict

def combine_bucket(bucket_path, cat):
    ## Download all csv files in bucket path
    CATEGORY = cat
    upc_columns = None
    brand_columns = None
    DL_CMD = 'gsutil -m cp  "{}/*" .'.format(bucket_path)
    oldfiles = os.listdir()
    if len(oldfiles) < 5:
        sp.call(DL_CMD, shell=True)
    files = os.listdir()
    csvfiles = [f for f in files if f.endswith('.csv')]
    nfiles = len(csvfiles)
    brand_rows = []
    upc_rows = []
    for i, c in enumerate(csvfiles):
        ## Match up sample file with input data
        filestem = os.path.splitext(c)[0]
        print("{}/{} Opening {}".format(i, nfiles, c))
        try:
            samples = pd.read_csv(c)
        except FileNotFoundException as e:
            print("Can't load csv {}".format(c))
            raise e
        try:
            model_info = pystan.read_rdump(filestem + '.R')
        except FileNotFoundException as e:
            print('Can not find corresponding R dump file')
            raise e
        upc_ids = model_info['upc_id']
        brand_id = model_info['brand_id'][0] # Brand id is constant.
        obs_ids = model_info['obs_id']
        u_upc_ids = pd.Series(upc_ids).unique()
        brand_params = [p for p in samples.columns if p.startswith('brand')]
        upc_params = [u.split('.')[0] for u in list(samples.columns) if
                      u.startswith('upc')]
        #Order preserving remove list duplicates
        upc_params = OrderedDict((x, True) for x in upc_params).keys()

        brand_samples = samples[brand_params]
        
        ## Helper functions to compute 10% - 90% range for posterior
        def low_quantile(x):
            return x.quantile(0.1)
        def high_quantile(x):
            return x.quantile(0.9)
        print('Summarizing brand...')
        brand_summary = brand_samples.agg([ low_quantile, 'mean',  high_quantile])
        print('Brand summary created!')
        brand_summary_list = brand_summary.values.flatten(order = 'F').tolist()
        print('Adding brand row')
        brand_row = [brand_id, CATEGORY, *brand_summary_list]
        if brand_columns == None:
            param_columns = ['brand_id', 'thg_category']
            for p in brand_params:
                param_columns.append('{}_low'.format(p))
                param_columns.append('{}_mean'.format(p))
                param_columns.append('{}_high'.format(p))
            brand_columns = param_columns
        brand_rows.append(brand_row)
        ##Build upc row per upc
        print('Adding upc rows')
        for i, u in enumerate(u_upc_ids):
            this_upc_params = ['{}.{}'.format(p, i + 1) for p in upc_params]
            print('this upc_params: {}'.format(str(this_upc_params)))
            upc_samples = samples[this_upc_params]
            print('Summarizing upc')
            upc_summary = upc_samples.agg(
                [low_quantile, 'mean', high_quantile])
            print('Upc {} summarized'.format(u))
            upc_summary_list = upc_summary.values.flatten(order = 'F').tolist()
            upc_row = [u, CATEGORY, *upc_summary_list]
            if upc_columns == None:
                cols = ['upc_id', 'thg_category']
                for p in upc_params:
                    cols.append('{}_low'.format(p))
                    cols.append('{}_mean'.format(p))
                    cols.append('{}_high'.format(p))
                upc_columns = cols
            upc_rows.append(upc_row)
    ## Roll into dataframe and write out
    print('Combining rows...')
    brands_df  = pd.DataFrame(brand_rows, columns=brand_columns)
    upcs_df = pd.DataFrame(upc_rows, columns=upc_columns)
    print('Writing out...')
    brands_df.to_csv('{}_brands.csv'.format(CATEGORY))
    upcs_df.to_csv('{}_upcs.csv'.format(CATEGORY))
    print('All complete!')

if __name__== '__main__':
    print (sys.argv[1:])
    wdpath, bucket_path, category = sys.argv[1:]
    os.chdir(wdpath)
    print('Starting to compile samples')
    combine_bucket(bucket_path, category)






        











