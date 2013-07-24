#!/bin/bash

DEPLOY_KEY="/home/erosen/src/geowiki/deploy_key"
DATA_REPO_PATH="/home/erosen/src/geowiki-data"

# update geowiki db
python /home/erosen/src/geowiki/geowiki/process_data.py\
                -o /home/erosen/data/editor-geocoding/\
                --wpfiles /home/erosen/src/geowiki/geowiki/data/all_ids.tsv --daily\
                --start=`date --date='-1 day' +\%Y-\%m-\%d`\
                --end=`date --date='1 day' +\%Y-\%m-\%d`

# make dashboard files
python /home/erosen/src/geowiki/scripts/make_limn_files.py\
                -d=$DATA_REPO_PATH

# setup limn_deploy ssh identity
eval `ssh-agent`
ssh-add $DEPLOY_KEY

# update data repo
cd $DATA_REPO_PATH
git pull
git add -A
git commit -m 'automatic commit using deploy_dashboard.sh script'
git push

# remove deploy key
ssh-add -d $DEPLOY_KEY
