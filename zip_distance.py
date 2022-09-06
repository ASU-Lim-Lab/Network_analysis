from uszipcode import SearchEngine
from haversine import haversine, Unit
import pandas as pd



search = SearchEngine(simple_or_comprehensive=SearchEngine.SimpleOrComprehensiveArgEnum.simple)

print("Opening metadata")
zip_frame = pd.read_csv('Sample_metadata.csv')
zip_frame.set_index('Sample_name', inplace=True)
zip_dict = zip_frame['Sample_zip'].to_dict()

#print(zip_dict)

print("Opening edge list")
edge_frame = pd.read_csv('Sample_mafftdeduplicated_edges_set_6e-5.csv')
print("Edge list opened")


progress_counter = 1
total_progress = len(edge_frame.index)
distance_list = []

for index, row in edge_frame.iterrows():
    source_zip = zip_dict[row['Source']]
    target_zip = zip_dict[row['Target']]
        #print(source_zip, target_zip)
    try:
        source_data = search.by_zipcode(source_zip)
        source_coord = (source_data.lat, source_data.lng)

        target_data = search.by_zipcode(target_zip)
        target_coord = (target_data.lat, target_data.lng)

        distance_meas = haversine(source_coord, target_coord, unit=Unit.MILES)
        distance_list.append(distance_meas)
        #print(row['phys_dist'])
    except:
        distance_meas = 99999
        distance_list.append(distance_meas)
        pass
    progress_counter += 1
    if progress_counter % 1000 == 0:
        print("%s of %s completed" % (progress_counter, total_progress))

edge_frame['phys_dist'] = distance_list

print("Saving")
edge_frame.to_csv('Sample_phys_distDEDUP.csv', index=False)