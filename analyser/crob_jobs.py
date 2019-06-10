#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 15/05/2019 10:00 AM
# @Author  : Zhihui Cheng
# @FileName: cron_jobs.py
# @Software: PyCharm

# !/usr/bin/ python3
# /Users/czh/anaconda3/bin/python3
# /home/ubuntu/Political_Analysis
from crontab import CronTab


cron = CronTab(user="ubuntu")
# submit the daily analysis job at 14:05 UTC time(Melbourne time 0:005)
job = cron.new(command='sh political_analysis.sh > /home/ubuntu/cron_error.log 2>&1', comment='daily job for politician analysis')
job.minute.on(5)
job.hour.on(14)

# cron.remove_all()
cron.write()

for item in cron:
    print(item)

# job.enable()
print(job.is_valid())
