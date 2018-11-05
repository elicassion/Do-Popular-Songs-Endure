import h5py, os, codecs, hdf5_getters, time, glob

# directory = 'MillionSongSubset\\A\\B\\B'

# filename = 'TRABBAM128F429D223.h5'

# f = h5py.File(os.path.join(directory, filename), 'r')

# print (list(f.keys()))

# print (f['metadata']['songs']['song_id'][0].decode('utf8'))

# h5 = hdf5_getters.open_h5_file_read(os.path.join(directory, filename))

# print (hdf5_getters.get_song_id(h5))

def get_all_titles(basedir, ext='.h5') :
    titles = []
    count = 0
    for root, dirs, files in os.walk(basedir):
        files = glob.glob(os.path.join(root, '*'+ext))
        for f in files:
            h5 = hdf5_getters.open_h5_file_read(f)
            titles.append( hdf5_getters.get_title(h5) )
            h5.close()
            count += 1
            if count % 100 == 0:
                print (count, )
            if count == 1000:
                return titles
    return titles


def naive_get_all_titles(basedir, ext='.h5'):
    titles = []
    count = 0
    for root, dirs, files in os.walk(basedir):
        files = glob.glob(os.path.join(root, '*'+ext))
        for f in files:
            h5 = h5py.File(f, 'r')
            titles.append(h5['metadata']['songs']['title'][0])
            h5.close()
            count += 1
            if count % 100 == 0:
                print (count, )
            if count == 1000:
                return titles
    return titles


st = time.time()
titles = naive_get_all_titles('MillionSongSubset')
print ('Using naive pure h5py: {}'.format(time.time() - st))


st = time.time()
titles = get_all_titles('MillionSongSubset')
print ('Using hdf5_getters: {}'.format(time.time() - st))