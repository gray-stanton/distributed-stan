#! /bin/bash
# Assumes Ubuntu v 17.04 as base image

mkdir /home/gray/
# Update base stuff
apt-get -y  upgrade
apt-get -y  update
apt-get -y install build-essential

# Copy over control files
git clone https://github.com/gray-stanton/distributed-stan/ /home/gray/stan/

# Setup miniconda environment
curl  https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /home/gray/miniconda.sh
bash /home/gray/miniconda.sh -b -p /home/gray/miniconda
export PATH="/home/gray/miniconda/bin:$PATH"
conda env create -f stan.yml 
source activate stan

# Setup cmdstan
git clone https://github.com/stan-dev/cmdstan.git --recursive /home/gray/cmdstan
cd /home/gray/cmdstan
make build
make /home/gray/stan/model

# Find out assigned machine id
MACHINE_ID=`curl "http://metadata.google.internal/computeMetadata/v1/instance/attributes/machine_id" \
-H "Metadata-Flavor: Google"`

