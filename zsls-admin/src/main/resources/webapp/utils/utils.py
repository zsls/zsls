import re

def dict2List(domainDict):
        domainList = []
        for key in domainDict:
            domainList.append([str(key),str(domainDict[key])])
        return domainList

def dict2List2(unitDict):
        unitList = []
        for key in unitDict:
            unit = [str(key)]
            for k in unitDict[key]:
                if k == 'tasks':
                    tasks = str()
                    for task in unitDict[key][k]:
                        tasks += str(task) + ","
                    unit.append(tasks)
                else:
                    unit.append(str(unitDict[key][k]))
            unitList.append(unit)
        return unitList    

def dict2List3(domainDict):
    taskList = []
    for domain in domainDict:
        for worker in domainDict[domain]:
            for task in domainDict[domain][worker]:
                taskList.append([str(task["taskId"]), str(task["stat"]), str(task["unitId"]), str(domain), str(worker)])
    return taskList

def dict2List4(domainDict):
    workUnitList = []
    for domain in domainDict:
        for worker in domainDict[domain]:
            unitDict = {}
            for task in domainDict[domain][worker]:
                unitId = str(task["unitId"])
                if unitDict.has_key(unitId):
                    unitDict[unitId].append([str(task["taskId"]), str(task["stat"])])
                else:
                    unitDict[unitId] = [[str(task["taskId"]), str(task["stat"])]]
            workUnitList.append([str(worker), str(domain), unitDict])
    return workUnitList

def dict2List5(domainDict):
    workUnitList = []
    for domain in domainDict:
        for worker in domainDict[domain]:
            unitDict = {}
            for task in domainDict[domain][worker]:
                unitId = "taskList"
                if unitDict.has_key(unitId):
                    unitDict[unitId].append([str(task["taskId"]), str(task["stat"])])
                else:
                    unitDict[unitId] = [[str(task["taskId"]), str(task["stat"])]]
            workUnitList.append([str(worker), str(domain), unitDict])
    return workUnitList

def dict2dict(domainDict):
    domainDict2 = {}
    domainDict2["rows"] = []
    for key in domainDict:
        domainDict2["rows"].append({"domain":key,"status":domainDict[key]})
    return domainDict2
def sortRTTask(list):
    t = sorted(list, key = lambda d : d["taskId"].split('-')[1])
    return t
def sortDict(dict):
    #split like unitXXX
    t = sorted(dict.items(), lambda x, y : comp(x[0][4:], y[0][4:]), reverse=True)
    return t
def comp(x, y):
    x = int(x)
    y = int(y)
    if x - y >= 0:
        return 1
    else :
        return -1 
    
