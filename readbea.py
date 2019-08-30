import numpy as np
import pandas as pd
import json
class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)
def read_in_bea(file):
    f = open(file, "r")

    beat = []
    label = []
    rhythm = []

    for row in f:

        beats = row.split(' ')

        beat.append(int(beats[0]))
        label.append(beats[2])
        rhythm.append(beats[len(beats)-1].replace('\n',''))
    f.close()
    return(np.asarray(beat),np.asarray(label),np.asarray(rhythm))

def strmatch(beat,labels):
    matches = []
    for x in range(len(labels)):
        if beat in labels[x]:
            matches.append(x)
    return(matches)

def readbea(file):
    rr=[]
    rrt=[]
    rlab=[]
    beat=[]
    lab=[]
    label=[]
    rhythm=[]
    rtime=[]

    beat, label, rhythm = read_in_bea(file)

    n = len(beat)

    if n == 0:

        return(rr,rrt,rlab,beat,lab,label,rhythm,rtime)

    lab = np.asarray([9]*n)

    beats=['NORMAL','PVC','APC','AESC','VESC','PACE','PFUS',\
    'UNKNOWN','UNCLASS','RHYTHM','AUX','SUB','ARFCT',\
    'VFON','FLWAV','VFOFF']

    bnum = [0,2,3,4,5,6,7,8,8,10,11,12,13,20,21,22]

    label0 = label

    k = np.arange(n)

    nb = len(beats)

    for i in range(nb):

        j = strmatch(beats[i],label0)

        label0 = np.delete(label0,j)

        lab[k[j]] = bnum[i]

        if len(label0) == 0:
            break

        k = np.delete(k,j)

    good = np.where(lab < 10)

    if len(good[0]) >= 2:

        rr = np.diff(beat[good[0]])

        rrt = beat[good[0][0]] + np.cumsum(rr)

    n = len(rr)

    rlab =-1 * np.ones(n)

    j = np.where(lab == 11)

    rhythm = rhythm[j]

    rtime = beat[j]

    nr = len(rhythm)

    if nr == 0:
        return(rr,rrt,rlab,beat,lab,label,rhythm,rtime)

    rhythms=['N','AFIB','AB','AFL','B','BII','IVR','NOD',\
        'P','PREX','SBR','SVTA','T','VFL','VT','J',\
        'PAT','AT','VTS','AIVRS','IVRS','AIVR']

    rnum = np.arange(len(rhythms))

    for i in range(nr):

        string = rhythm[i]
        num = -1

        if '(' in string:

            string = string.replace('(','')

            k = np.where(np.asarray(rhythms) == string)

            if len(k) == 1:
                num = rnum[k]

        time1 = rtime[i]
        k = rrt >= time1

        if i < nr - 1:

            time2 = rtime[i+1]

            k = np.logical_and(k,rrt<time2)

        rlab[k] = num
    result = {'rr':rr,"rlab":rlab}#,"beat":beat,'lab':lab,'label':label,'rhythm':rhythm,'rtime':rtime}
    return(result)
def sample_statistics(result):
    rr = result['rr']
    rr_clean =  rr[np.where(rr < 3000)]
    rlab = result['rlab']
    return({'Mean RR':rr.mean(),'STD RR': rr.std(),"RR Clean Mean":rr_clean.mean(),\
    "RR Clean STD":rr_clean.std(),'RLab':sum(rlab > 0)
    })
def both(i):
    file_path = 'houlter data/BEA/UVA'
    file = file_path + str(i).zfill(4) + '.bea'
    result = readbea(file)
    stats = sample_statistics(result)
    stats['ID'] = i

    return(stats)

import time
start_time = time.time()



import multiprocessing as mp
#print("Number of processors: ", mp.cpu_count())

#Step 1: Init multiprocessing.Pool()

pool = mp.Pool(14)

# Step 2: `pool.apply` the `howmany_within_range()`
results = pool.map(both, [i for i in range(1,2894)])
#results = [pool.apply(both, args=(i)) for i in range(100)]

# Step 3: Don't forget to close
pool.close()

df = pd.DataFrame(columns=['ID','Mean RR', 'STD RR',"RR Clean Mean","RR Clean STD", 'RLab'])
for i in range(0,2893):
    stats = results[i]
    df = df.append(stats,ignore_index=True)
df.to_csv('Cleaned Data/cleaned_corrected.csv',index=False)
print("--- %s seconds ---" % (time.time() - start_time))
