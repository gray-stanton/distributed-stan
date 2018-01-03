import os
import sys
import csv
import subprocess as sp
import shlex
import time
import random

DATAFILE_LIST = "data_aa"
STANFILE = "/home/gray/stan/fresh_pov_model"
WORK_DIR = "/home/gray/work/"
NCHAINS = 8
SEED = 561
## Instantiate logger 
def log(x):
    with open('cmdstan.log', "a") as f:
        print(x, file=f)
    print(x)

def has_closed(m):
    """Check if m a Popen object has finished"""
    status = m.poll()
    if status is None:
        return False
    else: 
        return True

def hms_string(sec_elapsed):
    h = int(sec_elapsed / (60 * 60))
    m = int((sec_elapsed % (60 * 60)) / 60)
    s = sec_elapsed % 60.
    return "{}:{:>02}:{:>05.2f}".format(h, m, s)

def run_processes(SEED):
    status_files = []
    processes = []
    for i in range(1, NCHAINS+1):
        # Can't use context manager as chains are launched async
        status_file = open(WORK_DIR + 'status_{}.txt'.format(i), 'w')
        status_files.append(status_file)
        cmd_str = RUN_CMDSTAN.format(
            stan=STANFILE, 
            input= WORK_DIR + filename,
            seed = SEED,
            id=i,
            output= (WORK_DIR + filestem)
        )
        args = shlex.split(cmd_str)
        #print(cmd_str)
        #print(args)
        ## Launch cmdstan
        proc = sp.Popen(args, stdout = status_file, stderr = sp.STDOUT)
        processes.append(proc)

    ## Check process heartbeats, kill if necessary
    start_time = time.time()
    majority_time = 0
    while True:
        statuses = [has_closed(p) for p in processes]
        ncomplete = sum(statuses)
        curtime = time.time()
        too_long = (curtime - majority_time) > 1.5 * (majority_time - start_time)
        if ncomplete == NCHAINS:
            complete_time = time.time()
            log("All complete! Runtime: {}".format(
                    hms_string(complete_time - start_time)))
            [f.close() for f in status_files]
            return True
        elif ncomplete >= (NCHAINS * 0.5) and majority_time == 0:
                majority_time = time.time()
                log('Majority finished. Runtime: {}'.format(
                    hms_string(majority_time - start_time)))
        ## If there are only a few chains left, but the majority finished 
        ## long ago, should reap and restart.
        elif ncomplete >= (0.75 * NCHAINS) and too_long:
            log("Reaped! Only {} complete, ".format(ncomplete))
            [f.close() for f in status_files]
            for p in processes:
                try:
                    p.kill()
                except Exception as e:
                    pass
            return False
        time.sleep(5)


    

## Pull down list of brand data files to process
GET_ASSIGNED = 'gsutil cat gs://fresh_nielsen/fresh_pov/' +DATAFILE_LIST 
try:
    bucketfiles_str = sp.check_output(GET_ASSIGNED, shell=True)
except CalledProcessError as e:
    log("Can't get assigned brand files")
    raise e

bucketfiles = bucketfiles_str.split(b"\n")
bucketfiles = [b.decode('utf-8') for b in bucketfiles if len(b) > 2] # Remove spurious '"'

nfiles = len(bucketfiles)
for n, brandfile in enumerate(bucketfiles):
    #starttime =  
    filename = os.path.basename(brandfile)
    filestem = os.path.splitext(filename)[0]
    dirname = os.path.dirname(brandfile)
    log(filename)
    log("{}/{}".format(n + 1, nfiles))
    ##Pull down input file
    GET_INPUT = 'gsutil cp "{f}" {d}'.format(f=brandfile, d=WORK_DIR)
    try:
        sp.call(GET_INPUT, shell = True)
    except (CalledProcessError, CommandException) as e:
        log("Can't get input file: {}".format(brandfile))
        break
    ## Compose cmdstan command
    RUN_CMDSTAN = ('{stan} sample '
                   'num_samples=500 num_warmup=500 '
                   'algorithm=hmc engine=nuts max_depth=13 ' 
                   'data file="{input}" '
                   'random seed={seed} '
                   'id={id} '
                   'output file="{output}_{id}.csv" '
                   'refresh=50')
    ## Run cmdstan processess, reap if necessary
    successful = run_processes(561)
    ## Restart w different seed if reaped
    while not successful:
        ## If doesn't work with that seed, 
        new_seed = random.randint()
        log('Trying again, seed: {}'.format(new_seed))
        successful = run_processes(new_seed)
    log('Terminated successfully')
    ## Post-process chain output csv
    ## Combining commands from CmdStan manual
    output_file = WORK_DIR + filestem
    COMBINE_CMD1 = 'grep lp__ "{f}_1.csv" > "{dir}combined.csv"'.format(
        f=output_file, dir=WORK_DIR)
    COMBINE_CMD2 = 'sed "/^[#l]/d" "{f}_{n}.csv" >> "{dir}combined.csv"'    
    sp.call(COMBINE_CMD1, shell=True)
    for i in range(1, NCHAINS+1):
        sp.call(COMBINE_CMD2.format(f=output_file, n=i, dir=WORK_DIR), shell=True)


    ## Upload result to gcp bucket, clean workspace.
    upload_file = dirname + '/' +  filestem + '.csv'
    os.chdir('/home/gray/work/')
    UPLOAD_CMD = 'gsutil cp {f} "{bucket}"'.format(
            bucket=upload_file, f='combined.csv')

    print(UPLOAD_CMD)
    sp.call(UPLOAD_CMD, shell=True)
    log("Uploaded {} to server".format(upload_file))
    sp.call('rm {d}status_* {d}*.csv'.format(d=WORK_DIR), shell=True)

log('ALL BRANDS COMPLETE')


