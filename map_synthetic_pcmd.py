#Created by Supratim Deb on March 20, 2014
"""
This is a pyspark code for generating cell statistics from PCMD data
"""

from pyspark import SparkContext
from collections import defaultdict
import sys, operator, math

#Constants for synthetic pcmd string
STRING_INDEX_FIRST_CELL = 4
STRING_INDEX_NUM_CELLS = 3
NUM_MEAS_STATISTICS = 6



"""
The following map function creates (key, value) pairs where 
key = (serving_cell, neighbor_cell) which is 2-tuple
value = (server_RSRP, nghbr_RSRP, server_RSRP^2, server_RSRP*nghbr_RSRP, nghbr_RSRP^2, 1)
which is a 5-tuple.
This representation helps us aggregate the values in the reduce function.
"""
def mapSyntheticPCMDString(pcmdString):
	elementsOfPCMDString = pcmdString.split(",")
	servingCell = elementsOfPCMDString[0]
	serverRSRP = float(elementsOfPCMDString[STRING_INDEX_FIRST_CELL + 1])
	listOfMeasurements = []
	for count in range(0,int(elementsOfPCMDString[STRING_INDEX_NUM_CELLS])):
		nghbrCell = elementsOfPCMDString[STRING_INDEX_FIRST_CELL + 2*count]
		nghbrRSRP = float(elementsOfPCMDString[STRING_INDEX_FIRST_CELL + 2*count + 1])
		listOfMeasurements.append((
			(servingCell, nghbrCell),\
			(serverRSRP, nghbrRSRP, serverRSRP*serverRSRP,\
			 serverRSRP*nghbrRSRP, nghbrRSRP*nghbrRSRP, 1)\
			))

	return listOfMeasurements



