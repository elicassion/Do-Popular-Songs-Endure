import sys, os, codecs, tarfile, shutil, h5py, time, csv
import multiprocessing
# from multiprocessing import Pool
import pandas as pd


subsets = []
for name in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
	subsets.append(pd.read_csv("..\\data\\extracted\\{}.csv".format(name)))

df = pd.concat(subsets, axis=0)

df.to_csv("..\\data\\extracted\\MSDSet.csv", index=False)