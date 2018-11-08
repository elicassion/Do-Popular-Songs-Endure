import billboard
import pandas as pd
import os, codecs

CHART_NAMES = ['youtube']


def init_chart_dict(chart_name):
    if chart_name == 'hot-100':
        b_chart_d = {}
        b_chart_d['title'] = []
        b_chart_d['chart_date'] = []
        b_chart_d['artist_name'] = []
        b_chart_d['peak_rank'] = []
        b_chart_d['last_rank'] = []
        b_chart_d['on_chart_weeks'] = []
        b_chart_d['current_rank'] = []
        b_chart_d['is_new'] = []
        return b_chart_d
    elif chart_name == 'youtube':
        b_chart_d = {}
        b_chart_d['title'] = []
        b_chart_d['chart_date'] = []
        b_chart_d['artist_name'] = []
        b_chart_d['peak_rank'] = []
        b_chart_d['last_rank'] = []
        b_chart_d['on_chart_weeks'] = []
        b_chart_d['current_rank'] = []
        b_chart_d['is_new'] = []
        return b_chart_d


def store_chart(chart):
    for song in chart:
        b_chart_d['title'].append(song.title)
        b_chart_d['chart_date'].append(chart.date)
        b_chart_d['artist_name'].append(song.artist)
        b_chart_d['peak_rank'].append(song.peakPos)
        b_chart_d['last_rank'].append(song.lastPos)
        b_chart_d['on_chart_weeks'].append(song.weeks)
        b_chart_d['current_rank'].append(song.rank)
        b_chart_d['is_new'].append(song.isNew)

def store_checkpoint(chart, d, chart_name):
    file_name = '..\\data\\raw\\{}.csv'.format(chart_name)
    df = pd.DataFrame.from_dict(d)
    print ("####### Storing checkpoint {} #######".format(chart.date))
    if os.path.exists(file_name):
        df.to_csv(file_name, mode='a', header=False, index=False)
    else:
        f = codecs.open(file_name, 'w')
        f.close()
        df.to_csv(file_name, mode='a', index=False)
    # if os.path.exists(checkpoint_filename):
    cf = codecs.open(checkpoint_filename, 'w')
    cf.write(chart.date.strip())
    cf.close()

for chart_name in CHART_NAMES:
    checkpoint_filename = '{}_checkpoint'.format(chart_name)
    if os.path.exists(checkpoint_filename):
        cf = open(checkpoint_filename, 'r')
        date_string = cf.read().strip()
        cf.close()
        chart = billboard.ChartData(chart_name, date = date_string, fetch=True)
    else:
        chart = billboard.ChartData(chart_name, fetch=True)
    b_chart_d = init_chart_dict(chart_name)
    count = 0
    while chart.previousDate:
        print ('Processing Chart from {}'.format(chart.date))
        store_chart(chart)
        count += 1
        if count % 52 == 0:
            store_checkpoint(chart, b_chart_d, chart_name)
            b_chart_d = init_chart_dict(chart_name)
        chart = billboard.ChartData(chart_name, chart.previousDate, fetch=True)
    store_checkpoint(chart, b_chart_d, chart_name)


    