import pandas as pd
import glob
from datetime import datetime

edge_frame = pd.read_csv('Sample_directedDEDUP.csv')
#edge_frame = pd.read_csv('2_10day_sorted_test.csv')

print("Sorting")
sorted_frame = edge_frame.sort_values(by=['Target','days_diff', 'phys_dist'])
print("Saving")
#sorted_frame.to_csv('2_10day_sorted.csv', index=False)

source_list = []
target_list = []
dist_list = []
phys_dist_list = []
days_diff_list = []


completed_set = set()

date_dict = {}
phys_dist_dict = {}
drop_list = []
keep_list =[]
#Create a set of all Targets found
target_set = set(sorted_frame['Target'].to_list())


#Create a dataframe for each item in the target set, composed of all instances of that sample being a target.
# Find mininum days between samples. Create a data frame from samples with that date difference
# Find the smallest physical difference between zip codes and create a dataframe from entries with that distance
# Create or concatenate this final filter into the concatentation frame.

progress_counter = 0
total_progress = len(target_set)
for each_target in target_set:
    query_frame = sorted_frame[sorted_frame['Target']== each_target]
    min_days_diff = query_frame['days_diff'].min()
    min_days_frame = query_frame[query_frame['days_diff']==min_days_diff]
    min_phys_dist = min_days_frame['phys_dist'].min()
    most_likely_frame = min_days_frame[min_days_frame['phys_dist'] == min_phys_dist]
    # most_likely_indexes = most_likely_frame.index
    # keep_list.extend(most_likely_indexes)
    if progress_counter == 0:
        concat_frame = most_likely_frame.copy()
    else:
        concat_frame = pd.concat([concat_frame,most_likely_frame])
    
    progress_counter +=1
    if progress_counter % 10 == 0:
        print("%s of %s completed" % (progress_counter, total_progress))

concat_frame.to_csv('Sample_final_networkDEDUP.csv', index=False)




