import pandas as pd
import glob
from datetime import datetime

date_frame = pd.read_csv('Sample_metadata.csv', usecols=['Sample_name', 'Sample_date'])
date_frame.set_index('Sample_name', inplace=True)
date_dict = date_frame['Sample_date'].to_dict()
#print(date_dict)

#edge_frame = pd.read_csv('omi_abctl_hosp_mafftdeduplicated_edges_set6e-5_phys_dist.csv', usecols=['Source','Target','Distance','phys_dist'])
edge_frame = pd.read_csv('Sample_phys_distDEDUP.csv')

#print(date_dict.get('hCoV-19/USA/WA-ASU65597/2022|EPI_ISL_12432791|2022-01-04'))
drop_list = []
final_count = len(edge_frame)
progress_counter = 1
source_list = []
target_list = []
distance_list = []
phys_dist_list = []
days_diff_list = []
for index, row in edge_frame.iterrows():
    range_test = int(date_dict.get(row['Source'])) - int(date_dict.get(row['Target']))
    #rint(row['Source'])
    #if range_test >=-2 and range_test <=2:
    if range_test < 0:
        source_list.append(row['Source'])
        target_list.append(row['Target'])
        distance_list.append(row['Distance'])
        phys_dist_list.append(row['phys_dist'])
        days_diff_list.append(range_test * -1)
    else:
        source_list.append(row['Target'])
        target_list.append(row['Source'])
        distance_list.append(row['Distance'])
        phys_dist_list.append(row['phys_dist'])
        days_diff_list.append(range_test)
    progress_counter +=1
    if progress_counter % 100 == 0:
        print("%s of %s completed" % (progress_counter, final_count))

#print(drop_list)
#clean_frame = edge_frame.drop(drop_list, axis=0)
clean_frame = pd.DataFrame()
clean_frame['Source'] = source_list
clean_frame['Target'] = target_list
clean_frame['Distance'] = distance_list
clean_frame['phys_dist'] = phys_dist_list
clean_frame['days_diff'] = days_diff_list

#Only keep connections <10 days old
clean_frame = clean_frame[clean_frame['days_diff']<=10]

#Deduplicating edges

clean_frame.drop_duplicates(inplace=True)

clean_frame.to_csv('Sample_directedDEDUP.csv', index=False)