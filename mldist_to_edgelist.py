import glob
import pandas as pd
import time
import numpy as np


def clean_mldist(mldist_file): #Open the mldist file
    mldist_filename = mldist_file + '.mldist'

    with open(mldist_filename) as f: #read lines of mldist file
        lines = f.readlines()

    f.close()

    print("File loaded into memory")


    total_length = len(lines)
    progress_counter = 0
    print("Total line length is %s" % (total_length))

    with open('mldist_intermediate.csv','w') as g: #write mldist to csv file, replacing spaces with commas, also removing repeating commas
        for eachLine in lines:
            newLine = eachLine.replace(' ',',')
            while ',,' in newLine:
                newLine = newLine.replace(',,',',')
            #print(newLine)
            g.write(newLine)
            progress_counter +=1
            if progress_counter % 500 == 0:
                print("Completed converting %s lines of %s to csv" % (progress_counter, total_length))

    g.close()


    with open('mldist_intermediate.csv', 'r') as input_file: #removes the first line, which contains unneeded information
        lines = input_file.readlines()
    input_file.close()
    print(lines.pop(0))
    print("Lines loaded, LINE POPPED")

    progress_counter = 0
    with open('intermediate.csv', 'w') as intermediate_file:
        for each_line in lines:
            intermediate_file.write(each_line)
            progress_counter +=1
            if progress_counter % 500 == 0:
                print("Completed rewriting %s lines of %s" % (progress_counter, total_length))
    intermediate_file.close()
    print("Intermediate created")
    dist_matrix = pd.read_csv('intermediate.csv', header=None, engine='python')


    #print(dist_matrix)
    dist_matrix.rename(columns={dist_matrix.columns[0]:"Name"}, inplace=True)
    target_names = dist_matrix['Name'].to_list()

    name_counter = 1
    for each_name in target_names:
        dist_matrix.rename(columns={dist_matrix.columns[name_counter]:each_name}, inplace=True)
        name_counter +=1
        if name_counter % 1000 == 0:
            print("%s columns complete" % (name_counter))

    #print(dist_matrix)
    
    #keep_frame = network_frame[network_frame['Distance'] < threshold_value]
    dist_matrix.to_csv(mldist_file + '.csv', index=False)

def nearest_neighbor(input_filename):
    inFile = input_filename + '.csv'
    outFile = input_filename + 'neighbored.csv'
    neighborVariance = 0.00 #keep decimal to ensure float

    #Change to read_csv
    matrixFile = pd.read_csv(inFile)
    #matrixFile = pd.read_excel(inFile)
    print("File read")
    matrixFile.set_index(matrixFile.columns[0], inplace=True)
    #print(matrixFile.index)
    print("File reindexed")

    #Remove 0s
    matrixFile.replace(0, np.nan, inplace=True)

    neighborDict = {}
    for eachColumn in matrixFile.columns:
        #print(matrixFile[eachColumn].min())
        neighborDict[eachColumn] = matrixFile[eachColumn].min()

    #print("Neighbor dictionary created")

    neighborList = []
    sourceList = []
    targetList = []
    distanceList = []
    counter = 0
    totalCols = len(matrixFile.columns)
    
    column_list = matrixFile.columns.to_list()
    
    
    
    for each_column in column_list:
        #print(each_column)
        distanceMin = neighborDict[each_column] - (neighborDict[each_column] * neighborVariance)
        #print(distanceMin)
        #distanceMax = neighborDict[eachIndex] + (neighborDict[eachIndex] * neighborVariance)
        distanceMax = 0.00006
        #print(matrixFile)
        neighbor_frame = matrixFile[(matrixFile[each_column]==distanceMin) & (matrixFile[each_column]<= distanceMax)]

                
        for index, row in neighbor_frame.iterrows():
            try:
                distanceValue = matrixFile.at[index, each_column]
                sourceList.append(index)
                targetList.append(each_column)
                distanceList.append(distanceValue)
            except:
                pass

        counter +=1
        if counter % 100 == 0:
            print("Completed neighboring columns " + str(counter) + " of " + str(totalCols))
        
    #print(len(sourceList))
    #print(len(targetList))
    #print(len(distanceList))
    edgeFile = pd.DataFrame()

    edgeFile['Source'] = sourceList
    edgeFile['Target'] = targetList
    edgeFile['Distance'] = distanceList

    edgeFile.to_csv(outFile, index=False)

def edge_deduper(input_filename):
    
    edge_file = input_filename + "neighbored.csv"

    edge_frame = pd.read_csv(edge_file)

    edge_frame['super_nearest'] = False
    #print(edge_frame)

    forward_dictionary = {}
    forward_list = []
    reverse_list = []
    drop_set = set()
    super_list = []
    progress_counter = 0
    total_rows = len(edge_frame.index)

    for index, row in edge_frame.iterrows():
        list_item = [row['Source'], row['Target'], row['Distance']]
        #list_item.append([row['Source'], row['Target'], row['Distance']])
        forward_list.append(list_item)
        forward_dictionary[str(list_item)] = index
        progress_counter += 1
        if progress_counter % 1000 == 0:
            print("%s of %s dictionaried" % (progress_counter, total_rows))

    print("Dictionary created")

    progress_counter = 0
    for index, row in edge_frame.iterrows():
        reverse_item = [row['Target'], row['Source'], row['Distance']]
        if str(reverse_item) in forward_dictionary:
            row['super_nearest'] = True
            edge_frame.at[forward_dictionary[str(reverse_item)], 'super_nearest'] = True
            if index not in drop_set:
                drop_set.add(forward_dictionary[str(reverse_item)])
        progress_counter += 1
        if progress_counter % 1000 == 0:
            print("%s of %s dictionaried (set)" % (progress_counter, total_rows))

    print("Drop index created")

    #print(drop_set)
    #print(len(drop_set))

    edge_frame.drop(drop_set, axis=0, inplace=True)

    print("Saving")
    edge_frame.to_csv(file_name + 'deduplicated_edges_set_6e-5.csv', index=False)

if __name__ == '__main__':
    found_mldist_files = glob.glob('*mldist')
    for each_file in found_mldist_files:
        file_name = each_file.split('.')[0]
        print("Cleaning %s" % (file_name))
        clean_mldist(file_name)
        
        nearest_neighbor(file_name)

        print("Deduping %s" % (file_name))
        edge_deduper(file_name)
        print("%s Done" % (file_name))
