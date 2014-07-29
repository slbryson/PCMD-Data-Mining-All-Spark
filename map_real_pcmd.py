#Created by Supratim Deb on March 20, 2014
"""
This is a pyspark code for generating cell statistics from PCMD data
"""

from collections import defaultdict
import sys, operator, math
import os.path

#Constants for synthetic pcmd string
STRING_INDEX_FIRST_CELL = 4
STRING_INDEX_NUM_CELLS = 3
NUM_MEAS_STATISTICS = 6

#Constants for real pcmd string
FIELD_SERVING_CELL_ID_PRIMARY = 3 #21
START_CHAR_MEAS = '['
END_CHAR_PCMD = '|'
LENGTH_MEAS_RECORD = 4
FIELD_CELL_ID_MEAS = 2
FIELD_RSRP_MEAS = 3


"""
The following map function creates (key, value) pairs where 
key = (serving_cell, neighbor_cell) which is 2-tuple
value = (server_RSRP, nghbr_RSRP, server_RSRP^2, server_RSRP*nghbr_RSRP, nghbr_RSRP^2, 1)
which is a 5-tuple.
This representation helps us aggregate the values in the reduce function.
"""
def mapRealPCMDString(pcmdString):
	servingCell, rsrpDict = retrieveRSRPDictFromPCMDString(pcmdString)
	if not rsrpDict.keys():
		return []

	serverRSRP = rsrpDict[servingCell]
	listOfMeasurements = []
	for cell in rsrpDict.keys():
		cellRSRP = rsrpDict[cell]
		listOfMeasurements.append((
			(servingCell, cell),\
			(serverRSRP, cellRSRP, serverRSRP*serverRSRP,\
			 serverRSRP*cellRSRP, cellRSRP*cellRSRP, 1)\
			))

	return listOfMeasurements


"""
The following function reads a pcmd string and retrieves the values in
a dictionary format with keys being cellids and values being RSRPs 
"""
def retrieveRSRPDictFromPCMDString(pcmdString):
	if START_CHAR_MEAS not in pcmdString:
		return str(), {}

	elementsOfPCMDString = pcmdString.split(";")
	servingCell = elementsOfPCMDString[FIELD_SERVING_CELL_ID_PRIMARY - 1]
	startOfMeasRecords = pcmdString.index(START_CHAR_MEAS) + 1
	endOfMeasRecords = pcmdString.index(END_CHAR_PCMD)
	allMeasRecords = (pcmdString[startOfMeasRecords : endOfMeasRecords]).split(";")
	numMeasRecords = int(allMeasRecords[0])
	""" create a dictionary with rsrp measurements in this record """
	index = 0
	rsrpMeas = defaultdict(int)
	for count in range(0, numMeasRecords):
		thisMeasRecord = allMeasRecords[count*LENGTH_MEAS_RECORD : (count + 1)*LENGTH_MEAS_RECORD]
		rsrpMeas[thisMeasRecord[FIELD_CELL_ID_MEAS]] = int(thisMeasRecord[FIELD_RSRP_MEAS])
	return servingCell, rsrpMeas




