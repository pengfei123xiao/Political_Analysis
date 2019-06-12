#!/bin/sh
DUMP=mongodump
OUT_DIR=~/data/backup/mongod/tmp
TAR_DIR=../
DATE=`date +%Y_%m_%d_%H_%M_%S`
DB_USER="admin"
DB_PASS="123"
DB_NAME="capstone"
AUT_DB_NAME="admin"
DAYS=5
TAR_BAK="mongod_tweets_$DATE.tar.gz"

cd $OUT_DIR
rm -rf $OUT_DIR/*
mkdir -p $OUT_DIR/$DATE
# dump for backup
$DUMP -d $DB_NAME --username $DB_USER --password $DB_PASS --authenticationDatabase $AUT_DB_NAME -o $DATE
# $DUMP -u $DB_USER -p $DB_PASS -db $DB_NAME -o $OUT_DIR/$DATE  // backup

# move to the targer
tar -zcvf $TAR_DIR/$TAR_BAK $DATE
# delete for more than 5 days
find $TAR_DIR/ -mtime +$DAYS -delete
