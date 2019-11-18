import pandas as pd
import numpy
from datetime import timedelta
import csv
import os

os.chdir('data')
columns = [
        'Time',
        'Nonheap Memory Usage',
        'Process Cpu Load',
        'Buffer Pool Direct Memory Used',
        'Heap Memory Usage',
        'Check Mail Scheduler',
        'System Cpu Load',
        'Buffer Pool Direct Count',
        'Rest Operations Count (total)',
        'JMS Queue Advimport Count',
        'HTTP Status 0/1 (proxy)',
        'HTTP Status 0/1',
        'HTTP availability 0/1',
        'Thread Count Peak',
        'JMS Queue EventAction Added (total)',
        'JMS Queue DocumentIndexer Added (total)',
        'JMS Queue EventAction Count',
        'JMS Queue Advlist.Export Added (total)',
        'JMS Queue DocumentIndexer Count',
        'HTTP availability service mode 0/1',
        'JMS Queue Advimport Added (total)',
        'JMS Queue Advlist.Export Count',
        'HTTP user licenses 0/1',
        'Class Loaded Count',
        'JMS Queue EventAction.Pushes Count',
        'JMS Queue EventAction.Pushes Added (total)',
        'JMS Queue EventAction.Notifications Count',
        'JMS Queue EventAction.Notifications Added (total)',
        'JMS Queue External.EventAction Count',
        'JMS Queue External.EventAction Added (total)',
        'JMS Queue Reports.Export Count',
        'JMS Queue Reports.Export Added (total)',
        'JMS Queue Reports.BuildReport Count',
        'JMS Queue Reports.BuildReport Added (total)',
        'Memory Code Cache Max',
        'Dispatch Input Traffic (total)',
        'Heap Memory Max',
        'Dispatch Count (total)',
        'Dispatch Output Traffic (total)',
        'Dispatch SQL Count (total)',
        'Memory Code Cache Used',
        'Memory Code Cache Committed',
        'Memory Metaspace Committed',
        'Memory Metaspace Max',
        'Memory Metaspace Used',
        'Is Reachable 0-1',
        'Main Pool Connections Busy',
        'Open Files Count',
        'Sequence Pool Connections Max',
        'Minor Duration (total)',
        'Full Duration (total)',
        'Main Pool Connections Max',
        'Main Pool Connections Count',
        'Sequence Pool Connections Count',
        'Sequence Pool Connections Busy',
        'Tomcat Threads Max',
        'Tomcat Uptime',
        'Icmp Get Time',
        'Tomcat Threads Count',
        'Thread Count',
        'Tomcat Threads Busy',
        'Tomcat Connections Count',
        'Tomcat Connections Max',
        'System Load Average 0-1',
        'GC Minor Count (total)',
        'GC Full Count (total)'
    ]

with open('test1.csv', 'w') as csvfile:
    metrics = []
    col = 1
    os.chdir('metrics')
    for filename in os.listdir():
        data = pd.read_csv(filename, sep=',', usecols=['time', 'value'])

        metric_time = []
        for t in data['time']:
            date = pd.to_datetime(t, unit='ns')
            metric_time.append(date)
        metric_vals = numpy.array(data['value'])

        need_averaging = columns[col].find('0/1') == -1
        metrics.append({'averaging' : need_averaging, 'time' : metric_time, 'value' : metric_vals})
        col += 1

    min_dates = []
    max_dates = []
    for m in metrics:
        min_dates.append(min(m['time']))
        max_dates.append(max(m['time']))

    start_date = min(min_dates)
    last_date = max(max_dates)
    delta = timedelta(minutes=30)
    temp_date = start_date
    time_column = []
    while temp_date < last_date:
        time_column.append(temp_date)
        temp_date += delta
    time_column.append(last_date)

    all_value_columns = []
    for m in metrics:
        value_column = []
        j = 0
        m_size = len(m['value'])
        if m['averaging'] == True:
            for time in time_column:
                temp_vals = []
                for i in range(j, m_size):
                    if m['time'][i] <= time:
                        temp_vals.append(m['value'][i])
                        if i == m_size - 1:
                            value_column.append(numpy.mean(temp_vals))
                    else:
                        if len(temp_vals) != 0:
                            value_column.append(numpy.mean(temp_vals))
                        else:
                            value_column.append(None)
                        j = i
                        break
        else:
            for time in time_column:
                val = None
                for i in range(j, m_size):
                    if m['time'][i] <= time:
                        if m['value'][i] != 0 and val != 0:
                            val = 1
                        else:
                            val = 0
                        if i == m_size - 1:
                            value_column.append(val)
                    else:
                        j = i
                        value_column.append(val)
                        break
        while len(value_column) < len(time_column):
            value_column.append(None)
        all_value_columns.append(value_column)

    filewriter = csv.DictWriter(csvfile, fieldnames=columns, delimiter=',')
    filewriter.writeheader()
    row = {}
    for index in range(0, len(time_column)):
        row[columns[0]] = time_column[index]
        for col in range(1, len(columns)):
            row[columns[col]] = all_value_columns[col-1][index]
        filewriter.writerow(row)