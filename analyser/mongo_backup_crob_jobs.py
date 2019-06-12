#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 15/05/2019 10:00 AM
# @Author  : Zhihui Cheng
# @FileName: mongo_backup_cron_jobs.py
# @Software: PyCharm

"""This file is used to set mongo backup job."""

from crontab import CronTab

cron = CronTab(user="ubuntu")
# submit the mongodb backup job at 17:05 UTC time(Melbourne time 3:05)
job = cron.new(command='sh mongodb_backup.sh > /home/ubuntu/cron_error.log 2>&1', comment='mongodb backup')
job.minute.on(5)
job.hour.on(17)

cron.write()

for item in cron:
    print(item)

print(job.is_valid())
