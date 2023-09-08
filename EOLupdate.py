import csv
import os
import re
import pandas as pd
from tqdm import tqdm

#os.rename('AE_output_1_EOLtest.csv', 'AE_output_1_EOLtest.csv.bak')
#with open('AE_output_1_EOLtest.csv', 'r') as infile, open('AE_output_1_EOLtest_test.csv', 'w') as outfile:
#    for line in infile:
#        line = line.strip().replace('\r','')
#        
#        #line = line + ',\n'
#        # print(type(line))
#        outfile.write(line)
# os.remove('AE_output_1_EOLtest.csv.bak')

import csv
with open("Final_AE_data_new_collated.csv", "r", encoding='latin-1') as input, open("Final_AE_data_new_collated_final.csv", "w", newline='',encoding='utf-8') as output:
    w = csv.writer(output)
    for record in tqdm(csv.reader(input)):
        w.writerow(tuple(' '.join(s.strip().replace("\n",'').replace('\u200e','').split()) for s in record))

# df = pd.read_csv('AE_output_1_EOLtest.csv',converters={'Product_Condition':lambda x:x.replace('/n','')})

# df.to_csv('testEOL.csv', index=False)
		