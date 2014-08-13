#Created by Supratim Deb on March 20, 2014
"""
This is a pyspark code for generating cell statistics from PCMD data
"""

from pyspark import SparkContext, SparkConf
from collections import defaultdict
import sys, operator, math, imp, os, uuid, ConfigParser, pyodbc
import datetime as dt
from multiprocessing import Pool
import itertools as it
import threading
import networkx as nx
import matplotlib . pyplot as plt

#Max cores per worker
SPARK_CORES_MAX = 3


#Constants for synthetic pcmd string
STRING_INDEX_FIRST_CELL = 4
STRING_INDEX_NUM_CELLS = 3
NUM_MEAS_STATISTICS = 6

#Constants for real pcmd string
CONFIG_FILE = 'config.ini'
FIELD_SERVING_CELL_ID_PRIMARY = 21
FIELD_UE_LOCATION_CAPABILITY = 158
FIELD_CELL_ID = 131
START_CHAR_MEAS = '['
END_CHAR_PCMD = '|'
LENGTH_MEAS_DATA = 4
FIELD_CELL_ID_MEAS = 2
FIELD_RSRP_MEAS = 3
RSRP_MIN = -140.0
DATETIME_FORMAT_PCMD_FILENAME = '%Y-%m-%d.%H_%M'
INT_THRESHOLD = 0.09


#Constant strings for outpul file string
MEAS_STRINGS = ['MeanServer', 'MeanNeighbor', 'VarianceServer', 'CovarianceNeighbor', 'VarianceNeighbor']
STRING_AVG_SERVER_RSRP = 'MeanServer'
STRING_VAR_SERVER_RSRP = 'VarianceServer'
STRING_AVG_NGHBR_RSRP = 'MeanNeighbor'
STRING_VAR_NGHBR_RSRP = 'VarianceNeighbor'
STRING_COVAR_RSRP = 'CovarianceNeighbor'


#def computeCellStatsForManyMeasFiles(startTime, timeWindowInMinutes, isRealPCMD, sparkMaster, measDir):
def parallelyComputeCellStatsForManyMeasFiles(startTime, timeWindowInMinutes, isRealPCMD, sparkMaster, measDir):
	
	timelyFiles = getTimelyFilesFromHDFS(startTime, timeWindowInMinutes, measDir)
	listCellStats = []

	pool = Pool()
	fileCellStats = pool.map(computeCellStatsForOneMeasFile_star, \
							 it.izip(it.repeat(sparkMaster), it.repeat(isRealPCMD), timelyFiles))
	pool.close()
	pool.join()
	listCellStats = list(it.chain(*fileCellStats))

	sparkAppName = 'PCMDMiner' 
	sc = SparkContext(sparkMaster, sparkAppName)
	rddCellStats = sc.parallelize(listCellStats)
	cellStats = rddCellStats \
				.reduceByKey(lambda x, y: reduceToSingleStat(x, y)) \
	            .map(lambda x: (x[0], toCovarMatrix(x[1]))) \
	            .sortByKey() \
	            .collect()
	sc.stop()

	return cellStats


def computeCellStatsForManyMeasFiles(startTime, timeWindowInMinutes, isRealPCMD, sparkMaster, measDir):
	timelyFiles = getTimelyFilesFromHDFS(startTime, timeWindowInMinutes, measDir)
	listCellStats = []
	for fn in timelyFiles:
		cs = computeCellStatsForOneMeasFile(sparkMaster, isRealPCMD, fn)
		listCellStats.extend(cs)
	sparkAppName = 'PCMDMiner' 
	sc = SparkContext(sparkMaster, sparkAppName)
	rddCellStats = sc.parallelize(listCellStats)
	cellStats = rddCellStats \
			.reduceByKey(lambda x, y: reduceToSingleStat(x, y)) \
			.sortByKey() \
			.collect()
	sc.stop()

	return cellStats


"""
the following is a wrapper around computeCellStatsForOneMeasFile with all arguments
put in a single list. this is required for parrallel processing of multiple files.
"""
def computeCellStatsForOneMeasFile_star(params):
	return(computeCellStatsForOneMeasFile(*params))


def computeCellStatsForOneMeasFile(sparkMaster, isRealPCMD, measFileName):
	sparkAppName = 'PCMDMiner' + uuid.uuid1().hex
	sparkConf = (SparkConf()
	       .setMaster(sparkMaster)
    	       .setAppName(sparkAppName)
    	       .set('spark.scheduler.mode', 'FAIR'))

	sc = SparkContext(conf = sparkConf)
	pcmdStrings = sc.textFile(measFileName)
#	cellStats = pcmdStrings\
#	            .flatMap(lambda x: mapPCMDString(isRealPCMD, x)) \
#	            .reduceByKey(lambda x, y: reduceToSingleStat(x, y)) \
#	            .collect()
	cellStats = pcmdStrings\
		.flatMap(lambda x: mapPCMDString(isRealPCMD, x)) \
	        .reduceByKey(lambda x, y: reduceToSingleStat(x, y)) \
		.collect()
	sc.stop()
	#debug
	            
	return cellStats



def getTimelyFilesFromHDFS(startTime, timeWindowInMinutes, measDirName):
	hadoopCommand = 'hadoop fs -ls %s/' % measDirName
	fullListings = os.popen(hadoopCommand).read().split('\n')
	fileNames = [x.split(' ')[-1].split('/')[-1] for x in fullListings][1:-1]
	timelyFiles = []
	dtStartTime = dt.datetime.strptime(startTime, DATETIME_FORMAT_PCMD_FILENAME)
	dtEndTime = dtStartTime + dt.timedelta(0, timeWindowInMinutes * 60)
	for fn in fileNames:
		fileCreateTimeString = '.'.join(fn.split('.')[:2])
		dtFileCreateTime = dt.datetime.strptime(fileCreateTimeString, DATETIME_FORMAT_PCMD_FILENAME)
		if ((dtFileCreateTime >= dtStartTime) and (dtFileCreateTime <= dtEndTime)):
			if (measDirName[-1] != '/'):
				sep = '/'
			else:
				sep = ''
			timelyFiles.append(measDirName + sep + fn)
		
	return timelyFiles



def mapPCMDString(isRealPCMD, pcmdString):
	if isRealPCMD:
		return mapRealPCMDString(pcmdString)
	else:
		return mapSyntheticPCMDString(pcmdString)



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
	serverRSRP = max(0, float(elementsOfPCMDString[STRING_INDEX_FIRST_CELL + 1]) - RSRP_MIN)
	listOfMeasurements = []
	for count in range(0,int(elementsOfPCMDString[STRING_INDEX_NUM_CELLS])):
		nghbrCell = elementsOfPCMDString[STRING_INDEX_FIRST_CELL + 2*count]
		nghbrRSRP = max(0, float(elementsOfPCMDString[STRING_INDEX_FIRST_CELL + 2*count + 1]) -RSRP_MIN)
		listOfMeasurements.append((
			(servingCell, nghbrCell),\
			(serverRSRP, nghbrRSRP, serverRSRP*serverRSRP,\
			 serverRSRP*nghbrRSRP, nghbrRSRP*nghbrRSRP, 1)\
			))

	return listOfMeasurements


"""
The following map function creates (key, value) pairs where 
key = (serving_cell, neighbor_cell) which is 2-tuple
value = (server_RSRP, nghbr_RSRP, server_RSRP^2, server_RSRP*nghbr_RSRP, nghbr_RSRP^2, 1)
which is a 5-tuple.
This representation helps us aggregate the values in the reduce function.
"""
def mapRealPCMDString_defunct(pcmdString):
	servingCell, listOfMeasRecords= getListOfMeasurementRecordsFromPCMDString(pcmdString)
	if not listOfMeasRecords:
		return []

	listOfMeasurements = []
	for measString in listOfMeasRecords:
		rsrpDict = getRSRPDictFromMeasRecord(servingCell, measString)
		if servingCell in rsrpDict.keys():
				serverRSRP = rsrpDict[servingCell]
		else:
				serverRSRP = - 1

		for cell in rsrpDict.keys():
			cellRSRP = rsrpDict[cell]
			listOfMeasurements.append((
				(servingCell, cell),\
				(serverRSRP, cellRSRP, serverRSRP*serverRSRP,\
			 	serverRSRP*cellRSRP, cellRSRP*cellRSRP, 1)\
				))
	#return key value pairs
	return listOfMeasurements

def mapRealPCMDString(pcmdString):
	servingCell, fieldDict = getFieldDictFromMeasRecord(pcmdString)
	#debug
	if not fieldDict:
		return []

	# Reset listOfMeasurements
	listOfMeasurements = []
	if servingCell in fieldDict.keys():
		serverDLRLC = fieldDict[servingCell][0][0]
	else:
		serverDLRLC = -1

	for cellID in fieldDict.keys():
		f1 = fieldDict[cellID][0][0]
		f2 = fieldDict[cellID][0][1]
		listOfMeasurements.append((
			(servingCell, cellID), \
			(f1,f1*f1,serverDLRLC,serverDLRLC*serverDLRLC,f1*serverDLRLC,1)	
			))
	return listOfMeasurements


"""
this function gets each field requested from the pcmd file
"""
def getFieldDictFromMeasRecord(pcmdString):
	#assume this function is getting one record at a time
	elementsOfPCMDString = pcmdString.split(";")
	if len(elementsOfPCMDString) < 253:
		return -1, []
	cellID = elementsOfPCMDString[FIELD_CELL_ID -1]
	servingCell = cellID
	secondaryFieldNum = elementsOfPCMDString[173-1]
	if secondaryFieldNum ==2:
		DLRLC_First = map(float,elementsOfPCMDString[251-1])
	else:
		DLRLC_First = []
	        return servingCell, []

	fieldDict =defaultdict(list)

	fieldDict[cellID].append((float(DLRLC_First),servingCell))
	print fieldDict[cellID], cellID
	

	return servingCell, fieldDict

"""
this function retrieves all measurement strings from a pcmd string 
"""
def getListOfMeasurementRecordsFromPCMDString(pcmdString):
	elementsOfPCMDString = pcmdString.split(";")
	servingCell = elementsOfPCMDString[FIELD_SERVING_CELL_ID_PRIMARY - 1]
	#RSRP is just one field needed.
	if START_CHAR_MEAS not in pcmdString:
		return servingCell, []

	startOfMeasRecords = pcmdString.index(START_CHAR_MEAS) + 1
	endOfMeasRecords = pcmdString.index(END_CHAR_PCMD)
	allMeasRecords = (pcmdString[startOfMeasRecords : endOfMeasRecords]).split(";")
	listOfMeasRecords = []
	count = 0
	while (count < len(allMeasRecords)):
		startOfThisMeasRecord = count
		lengthOfThisMeasRecord = int(allMeasRecords[count]) * LENGTH_MEAS_DATA + 1
		endOfThisMeasRecord = startOfThisMeasRecord + lengthOfThisMeasRecord
		listOfMeasRecords.append(allMeasRecords[startOfThisMeasRecord : endOfThisMeasRecord])
		count = endOfThisMeasRecord
	#Add an additional return value that will include additional PCMD fields
	#Need to correct the syntax to combine these two structures into the listOfllMeasRecords
	return servingCell, listOfMeasRecords


"""
The following function reads a pcmd measurement string and retrieves the values in
a dictionary format with keys being cellids and values being RSRPs 
"""
def getRSRPDictFromMeasRecord(servingCell, measRecord):
	numCellsInMeasRecord = int(measRecord[0])
	numMeasData = int((len(measRecord) - 1)/ LENGTH_MEAS_DATA)
	""" create a dictionary with rsrp measurements in this record """
	index = 0
	rsrpMeasVal = defaultdict(float) 

	for count in range(0, numMeasData):
		thisMeasData = measRecord[1+count*LENGTH_MEAS_DATA : 1+(count + 1)*LENGTH_MEAS_DATA]
		thisCell = thisMeasData[FIELD_CELL_ID_MEAS - 1]
		#Apparently the RSRP was stored as a string as well and needs to be converted
		thisRSRPString = thisMeasData[FIELD_RSRP_MEAS - 1]
		if (thisCell == '') or (thisRSRPString == ''):
			continue
		else:
			#This line creates a dictionary based on the cell found in the measurement data. 
			rsrpMeasVal[thisCell] = float(thisRSRPString)
	#Note servingCell was passed to this function but not used.
	return rsrpMeasVal


"""
define a commutative and associative function for combining aggregate statistics
this is used by reduce function in the code later
"""
def reduceToSingleStat(stat1, stat2):
	if stat2.share[1] < NUM_MEAS_STATISTICS -1:
		return []
	sumMeasCount = stat1[NUM_MEAS_STATISTICS-1] + stat2[NUM_MEAS_STATISTICS-1]
	wtStat1 = 1.0 * stat1[NUM_MEAS_STATISTICS-1] / sumMeasCount
	wtStat2 = 1.0 * stat2[NUM_MEAS_STATISTICS-1] / sumMeasCount
	reducedStat = [0] * NUM_MEAS_STATISTICS
	for index in range(0, NUM_MEAS_STATISTICS - 1):
		reducedStat[index] = wtStat1 * stat1[index] + wtStat2 * stat2[index]
	reducedStat[NUM_MEAS_STATISTICS-1] = sumMeasCount
	return tuple(reducedStat)




def toCovarMatrix(stat):
	params = [0] * NUM_MEAS_STATISTICS
	for index in [0, 1, NUM_MEAS_STATISTICS-1]:
		params[index] = stat[index]
	params[2] = stat[2] - stat[0]*stat[0]
	params[3] = stat[3] - stat[0]*stat[1]
	params[4] = stat[4] - stat[1]*stat[1]
	return tuple(params)


def saveAsFile(outputFile, cellStats, caseId, rnpId):
	with open(outputFile + '.txt', 'w') as opf:
		for cs in cellStats:
			op_string = '{:16s}  {:16s}   '.format(cs[0][0], cs[0][1])
			for i in range(0, NUM_MEAS_STATISTICS - 1):
				op_string =  op_string + '{:>10s}'.format('{:4.2f}'.format(cs[1][i]))
			op_string =  op_string +\
				 '{:>8s}'.format('{:4d}'.format(cs[1][NUM_MEAS_STATISTICS-1]))
			opf.write(op_string + '\n')


	allDBRowsAsStrings = getAllDBRowsAsStrings(cellStats, caseId, rnpId)
	with open(outputFile + '.csv', 'w') as opf:
		for measString in allDBRowsAsStrings:
			opf.write(measString + '\n')

	return


def getAllDBRowsAsStrings(cellStats, caseId, rnpId):
	allDBRowsAsStrings = []
	for cs in cellStats:
		commonString = "%d, %d" % (caseId, rnpId)
		measStats = [RSRP_MIN + cs[1][0], RSRP_MIN + cs[1][1], cs[1][2], cs[1][3], cs[1][4], cs[1][5]]
		for i in range(len(MEAS_STRINGS)):
			measType = MEAS_STRINGS[i]
			measuredCells= (cs[0][0], cs[0][1])
			measString = "%s, '%s', '%s', '%s', '%s', %s, %s"\
			             % (commonString, measType, cs[0][0], measuredCells[0], measuredCells[1],
			                str(measStats[i]), str(measStats[5]))
			allDBRowsAsStrings.append('(' + measString + ')')
	return allDBRowsAsStrings


def saveToDatabase(cellStats, caseId, rnpId):
	conStr = getDBConnStringConfig()
	cnx = pyodbc.connect(conStr)
	cursor = cnx.cursor()
	cnf = ConfigParser.ConfigParser()
	cnf.read(CONFIG_FILE)
	tableName = dict(cnf.items('DB_TABLE'))['name']
	tableOverwrite = dict(cnf.items('DB_TABLE'))['overwrite']
	if tableOverwrite == 'y':
		delStr = 'DELETE FROM %s WHERE Case_Id = %d AND RNP_Simulation_Id = %d' % (tableName, caseId, rnpId)
		cursor.execute(delStr)
		print ('Deleted %d from DB: ' % cursor.rowcount) + delStr
		cnx.commit()


	allDBRowsAsStrings = getAllDBRowsAsStrings(cellStats, caseId, rnpId)
	for dbRowString in allDBRowsAsStrings:
		insertStr = "INSERT INTO RSRPJointGaussianParams VALUES %s" % dbRowString
		cursor.execute(insertStr)
	cnx.commit()

	cnx.close()
	return


def getDBConnStringConfig():
	conStr = r''
	cnf = ConfigParser.ConfigParser()
	cnf.read(CONFIG_FILE)
	for option in cnf.items('DATABASE'):
		conStr += '%s=%s;' %  option
	return conStr.rstrip(';')


def getBasicConfigInfo():
	cnf = ConfigParser.ConfigParser()
	cnf.read(CONFIG_FILE)
	isRealPCMD = cnf.getint('BASIC', 'REAL_PCMD')
	sparkMaster = cnf.get('BASIC', 'SPARK_MASTER')
	measDir = cnf.get('BASIC', 'HADOOP_PCMD_DIR')
	caseId = cnf.getint('BASIC', 'CASE_ID')
	rnpId = cnf.getint('BASIC', 'RNP_SIM_ID')
	return isRealPCMD, sparkMaster, measDir, caseId, rnpId


def createClusters(cellStats):
	cellGraph = nx.DiGraph()
	cellNodes =set([])
	cellEdges = []
	for cs in cellStats:
		cellNodes = cellNodes.union(set(cs[0]))

	cellTotalWeight = defaultdict(int)
	for node in cellNodes:
		cellTotalWeight[node] = sum([x[1][5] for x in cellStats if x[0][0] == node and x[0][1] != node])
		#print '%s : %d' % (node, cellTotalWeight[node])
		#sys.stdin.read(1)

	for cs in cellStats:
		if ((cs[0][0] != cs[0][1]) and (cs[1][5] > cellTotalWeight[cs[0][0]] * INT_THRESHOLD)):
			cellEdges.append(cs[0])
			#cellEdges.append((cs[0][0], cs[0][1], cs[1][5]))

	cellGraph.add_nodes_from(list(cellNodes))
	cellGraph.add_edges_from(cellEdges)
	comps = nx.connected_component_subgraphs(cellGraph.to_undirected())
	print 'Nodes = %d, Edges = %d' % (cellGraph.number_of_nodes(), cellGraph.number_of_edges())
	ind = 0
	for sg in comps:
		ind += 1
		print '%d : %d , %d' % (ind, sg.number_of_nodes(), sg.number_of_edges())

	return


if __name__ == "__main__":
	if len(sys.argv) < 3:
		print >> sys.stderr, "Usage: generate_windowed_cell_statistics  start_time_string  time_in_minutes"
		print >> sys.stderr, "Enter time in year-month-day.hr_min format . Everything but year is in 2 digits"
		exit(-1)
	else:
		startTime = sys.argv[1]
		timeWindowInMinutes = int(sys.argv[2])
	#########################################################
	# Unused commands:
	#sc = SparkContext(sparkMaster, "PCMDMiner")
	#cellStats = parallelyComputeCellStatsForManyMeasFiles(startTime, timeWindowInMinutes, isRealPCMD, sparkMaster, measDir)
	#saveToDatabase(cellStats, caseId, rnpId)
	#########################################################
	# use these three steps
	isRealPCMD, sparkMaster, measDir, caseId, rnpId = getBasicConfigInfo() 
	cellStats = computeCellStatsForManyMeasFiles(startTime, timeWindowInMinutes, isRealPCMD, sparkMaster, measDir)
	print "########################  cellStats ####################################", cellStats
	#saveAsFile("output_cell_statistics", cellStats, caseId, rnpId)
	#createClusters(cellStats)
	#########################################################


