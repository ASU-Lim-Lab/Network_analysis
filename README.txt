Workflow
1. Combine fasta-formatted sequences into a single file.
2. Run MAFFT multiple sequence alignment using Wuhan1 as reference file
3. Run IQtree analysis on alignment file
4. Convert distance matrix to edge list: mldist_to_edgelist.py
5. Calculate haversine distance and add to edge list: zip_distance.py
6. Determine most recent, nearest source for each target: find_recent_neighbors.py

Non-standard required python packages
- Pandas
- Numpy
- uszipcode
- haversine

