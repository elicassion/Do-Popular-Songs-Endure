import gzip
import tarfile
import time
import h5py
import shutil
import os
st = time.time()
# tar = tarfile.open('..\\data\\raw\\millionsongsubset.tar.gz', 'r:gz')
# # print (tar.getmembers())
# for t in tar.getmembers():
#   # print (t)
#   if not t.isfile():
#       continue
#   f = tar.extractfile(t)
#   # print (type(f))
#   k = f.read('rb')
#   h5 = h5py.File(k)
#   print (h5['metadata']['songs']['title'][0].decode('utf8'))
#   # print (type(f.read()))
#   f.close()
#   break
    

titlelist = []
f = tarfile.open('..\\data\\raw\\millionsongsubset.tar.gz', 'r:gz')
f.extractall(path="tmp_dir")
files = [os.path.join("tmp_dir", fname) for fname in f.getnames() if fname.endswith(".h5")]
for file in files:
     h5 = h5py.File(file, 'r')
     ## do stuff with g
     titlelist.append(h5['metadata']['songs']['title'][0].decode('utf8'))
     h5.close()
shutil.rmtree("tmp_dir")

# titlelist = []
# f = tarfile.open('..\\data\\raw\\millionsongsubset.tar.gz', 'r:gz')
# for tarinfo in f:
#     if os.path.splitext(tarinfo.name)[-1] == ".h5":
#         file = os.path.join("tmp_dir", tarinfo.name)
#         f.extract(tarinfo, path="tmp_dir")
#         # g = h5py.File(file, 'r')
#         ## do stuff with g
#         titlelist.append(g['metadata']['songs']['title'][0].decode('utf8'))
#         os.remove(file)
# shutil.rmtree("tmp_dir")
print (titlelist)
print (time.time() - st)