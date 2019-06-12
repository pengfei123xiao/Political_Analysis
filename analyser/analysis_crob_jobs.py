#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 15/05/2019 10:00 AM
# @Author  : Zhihui Cheng
# @FileName: analysis_cron_jobs.py
# @Software: PyCharm

"""This file is used to set daily jobs."""

from crontab import CronTab

cron = CronTab(user="ubuntu")
# submit the daily analysis job at 14:05 UTC time(Melbourne time 0:05)
job = cron.new(command='sh political_analysis.sh > /home/ubuntu/cron_error.log 2>&1', comment='daily job for politician analysis')
job.minute.on(5)
job.hour.on(14)

cron.write()

for item in cron:
    print(item)

print(job.is_valid())
