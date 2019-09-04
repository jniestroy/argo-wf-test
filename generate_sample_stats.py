import numpy as np
import pandas as pd
import json
import sys
import os
import subprocess

def sample_statistics(result):
    rr = result['rr']
    rr_clean =  rr[np.where(rr < 3000)]
    rlab = result['rlab']
    return({'Mean RR':rr.mean(),'STD RR': rr.std(),"RR Clean Mean":rr_clean.mean(),\
    "RR Clean STD":rr_clean.std(),'RLab':sum(rlab > 0)
    })


path = sys.argv[1]

os.chdir(path)

test = ''

files = [f for f in os.listdir('.') if os.path.isfile(f)]

df = pd.DataFrame(columns=['ID', 'Mean', 'STD'])

for f in files:

    with open(f) as json_file:
        data = json.load(json_file)

    rr = data['rr']

    rr = np.asarray(rr)

    id = f.replace('.json','')

    stats = {'ID':id,'Mean':rr.mean(),'STD':rr.std()}

    df = df.append(stats , ignore_index=True)

df.to_csv('test.csv',index=False)
