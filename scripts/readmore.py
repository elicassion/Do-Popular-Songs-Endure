import h5py, os, codecs, time, glob, csv
import pandas as pd


def naive_get_all_titles(basedir, ext='.h5'):
    result_dict = {}
    result_dict['file_id'] = []
    result_dict['song_id'] = []
    result_dict['post_year'] = []
    # result_dict['song_age'] = []
    ### result_dict['month'] = []
    # result_dict['decade'] = []
    # result_dict['5_year_id'] = []
    result_dict['artist_id'] = []
    result_dict['artist_hotness'] = []
    result_dict['artist_name'] = []
    result_dict['danceability'] = []
    result_dict['duration'] = []
    result_dict['end_of_fade_in'] = []
    result_dict['energy'] = []
    result_dict['loudness'] = []
    result_dict['song_hotness'] = []
    result_dict['start_of_fade_out'] = []
    result_dict['tempo'] = []
    result_dict['title'] = []
    count = 0
    for root, dirs, files in os.walk(basedir):
        files = glob.glob(os.path.join(root, '*'+ext))
        # count += len(files)
        for f in files:
            h5 = h5py.File(f, 'r')
            result_dict['file_id'].append(f.split('\\')[-1].split('.')[0])
            result_dict['song_id'].append(h5['metadata']['songs']['song_id'][0].decode('utf8'))
            result_dict['post_year'].append(h5['musicbrainz']['songs']['year'][0])
            result_dict['artist_id'].append(h5['metadata']['songs']['artist_id'][0].decode('utf8'))
            result_dict['artist_hotness'].append(h5['metadata']['songs']['artist_hotttnesss'][0])
            result_dict['artist_name'].append(h5['metadata']['songs']['artist_name'][0].decode('utf8'))
            result_dict['danceability'].append(h5['analysis']['songs']['danceability'][0])
            result_dict['duration'].append(h5['analysis']['songs']['duration'][0])
            result_dict['end_of_fade_in'].append(h5['analysis']['songs']['end_of_fade_in'][0])
            result_dict['energy'].append(h5['analysis']['songs']['energy'][0])
            result_dict['loudness'].append(h5['analysis']['songs']['loudness'][0])
            result_dict['song_hotness'].append(h5['metadata']['songs']['song_hotttnesss'][0])
            result_dict['start_of_fade_out'].append(h5['analysis']['songs']['start_of_fade_out'][0])
            result_dict['tempo'].append(h5['analysis']['songs']['tempo'][0])
            result_dict['title'].append(h5['metadata']['songs']['title'][0].decode('utf8'))
            h5.close()
            count += 1
            if count % 100 == 0:
                print (count, )
                # return result_dict
    # print (count)
    return result_dict


st = time.time()
feature_dict = naive_get_all_titles('..\\data\\raw\\MillionSongSubset')
df = pd.DataFrame.from_dict(feature_dict)
df.to_csv('..\\data\\extracted\\million_song_subset.csv', index=False)
# with open(, 'w') as f:  # Just use 'w' mode in 3.x
#     w = csv.DictWriter(f, feature_dict.keys())
#     w.writeheader()
#     w.writerow(feature_dict)
print ('Using naive pure h5py: {}'.format(time.time() - st))