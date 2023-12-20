import commands
import time
from multiprocessing import Process,Queue,Pool,Lock,Manager

def logProcess(sIndex,eIndex):
	global HlsLogList
	global HlsIbDict
	HlsLogDict = {}
	for logLine in HlsLogList[sIndex:eIndex]:
       		logLineContent = logLine.split('%')
		objHashList = logLineContent[-1].split(':')
		objType = objHashList[0]
		if 'vxctoken' in logLineContent[3] or 'vxttoken' in logLineContent[3]:
			li = logLineContent[3].split('=')
			key = li[-1]+'%'+objType
		else:
			if objType == 'BT':
				url = 'UNKNOWN'
				key = logLineContent[2]+"%"+url+'%'+objType
			else:
				urlContent = logLineContent[3].split('/')
				if (len(urlContent) < 6):
					url = urlContent[2]
					key = logLineContent[2]+"%"+url+'%'+objType
				else:
					url = urlContent[2]+'/'+urlContent[3]+'/'+urlContent[4]
					if HlsIbDict.has_key(url):			
						key = logLineContent[2]+"%"+url+'%'+objType
					else:
						url = urlContent[2]
						key = logLineContent[2]+"%"+url+'%'+objType
	
		endTime = int(time.mktime(time.strptime("%s" %(logLineContent[0]), "%Y-%m-%d %H:%M:%S")))
		if HlsLogDict.has_key(key):
			HlsLogDict[key].append(endTime)
		else:
			HlsLogDict[key] = [endTime]

	return HlsLogDict

global HlsLogList
HlsIb = open("HlsStreamIb","r")
HlsIbList = HlsIb.readlines()
HlsIb.close()

HlsIbDict = {}

for HlsIbLine in HlsIbList:
        IbContentList = HlsIbLine.split(';')
        HlsIbDict[IbContentList[2]] = IbContentList[3]

HlsLog = open("24oct_751.txt","r")
HlsLogList = HlsLog.readlines()
HlsLog.close()

manager = Manager()
lock = manager.Lock()

indexVal = int(len(HlsLogList)/20)
processThreadList = []
global threadSyncList
threadSyncList = []
global processLogDict
processLogDict = {}

def dataAggr(HlsLogDict):
	for key in HlsLogDict.keys():
                lock.acquire()
                if processLogDict.has_key(key):
                        processLogDict[key].extend(HlsLogDict[key])
                else:
                        processLogDict[key] = []
			processLogDict[key].extend(HlsLogDict[key])
                lock.release()
        del(HlsLogDict)

processLogPool = Pool(20)
for i in range(20):
	j = i+1
	processLogPool.apply_async(logProcess, args=(i*indexVal,j*indexVal), callback=dataAggr)

processLogPool.close()
processLogPool.join()

sessionDict = {}

for key in processLogDict.keys():
	processLogDict[key].sort()
	iniTimestmp = processLogDict[key][0]
	keyList = key.split('%')
	objType = keyList[-1]

	if sessionDict.has_key(key):
		for timeStamp in processLogDict[key]:
			diff = timeStamp-iniTimestmp
			if(objType == 'WP'):
				if(timeDiff <= 300):
					iniTimestmp = timeStamp
				else:
					sessionDict[key]+=1
					iniTimestmp = timeStamp
			if(objType == 'BT'):
                                if(timeDiff <= 3600):
                                        iniTimestmp = timeStamp
                                else:
                                        sessionDict[key]+=1	
					iniTimestmp = timeStamp	
	else:
		sessionDict[key] = 1
	
sessions = 0
for key in sessionDict.keys():
        sessions = sessions+sessionDict[key]

print sessions


