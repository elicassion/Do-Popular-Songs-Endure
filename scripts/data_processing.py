import sys, os, codecs, tarfile, shutil, h5py, time, csv
import multiprocessing
# from multiprocessing import Pool
import pandas as pd



def initial_feature_dict():
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
    return result_dict

def fill_reature_dict(h5, filename, f_dict):
    f_dict['file_id'].append(filename.split('\\')[-1].split('.')[0])
    f_dict['song_id'].append(h5['metadata']['songs']['song_id'][0].decode('utf8'))
    f_dict['post_year'].append(h5['musicbrainz']['songs']['year'][0])
    f_dict['artist_id'].append(h5['metadata']['songs']['artist_id'][0].decode('utf8'))
    f_dict['artist_hotness'].append(h5['metadata']['songs']['artist_hotttnesss'][0])
    f_dict['artist_name'].append(h5['metadata']['songs']['artist_name'][0].decode('utf8'))
    f_dict['danceability'].append(h5['analysis']['songs']['danceability'][0])
    f_dict['duration'].append(h5['analysis']['songs']['duration'][0])
    f_dict['end_of_fade_in'].append(h5['analysis']['songs']['end_of_fade_in'][0])
    f_dict['energy'].append(h5['analysis']['songs']['energy'][0])
    f_dict['loudness'].append(h5['analysis']['songs']['loudness'][0])
    f_dict['song_hotness'].append(h5['metadata']['songs']['song_hotttnesss'][0])
    f_dict['start_of_fade_out'].append(h5['analysis']['songs']['start_of_fade_out'][0])
    f_dict['tempo'].append(h5['analysis']['songs']['tempo'][0])
    f_dict['title'].append(h5['metadata']['songs']['title'][0].decode('utf8'))


def naive_get_all_features_from_gzip(basedir, filename, ext='.h5'):
    print ('Processing: {}'.format(filename))
    feature_dict = initial_feature_dict()
    f = tarfile.open(os.path.join(basedir, filename+'.tar.gz'), 'r:gz')
    temp_dir = "tmp_dir_{}".format(filename.split('.')[0])
    f.extractall(path=temp_dir)
    files = [os.path.join(temp_dir, fname) for fname in f.getnames() if fname.endswith(".h5")]
    count = 0
    for file in files:
        h5 = h5py.File(file, 'r')
        fill_reature_dict(h5, file, feature_dict)
        h5.close()
        count += 1
        if count % 10000 == 0:
            print ("{}-{}".format(filename, count))
    df = pd.DataFrame.from_dict(feature_dict)
    df.to_csv('..\\data\\extracted\\{}.csv'.format(filename), index=False)
    shutil.rmtree(temp_dir)


if __name__ == '__main__':
    st = time.time()
    basedir = '..\\data\\raw'
    # jobs = []
    # for name in 'ABCD':  #EFGHIJKLMNOPQRSTUVWXYZ
    #     p = multiprocessing.Process(target=naive_get_all_features_from_gzip, args=(basedir, name, ))
    #     jobs.append(p)
    #     p.start()
        
    # for p in jobs:
    #     p.join()

    pool = multiprocessing.Pool(processes = 8)
    for name in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
        pool.apply_async(naive_get_all_features_from_gzip, (basedir, name, ))
    pool.close()
    pool.join()
    print (time.time() - st)