from __future__ import absolute_import, division, print_function, unicode_literals
import json
from bunch import bunchify
from collections import OrderedDict
import time
from datetime import datetime, timedelta
from pathlib import Path
import csv
import copy
# Zato
from zato.server.service import Service


class run(Service):
    file_path_log = '/opt/zato/instanceFiles/log/'
    file_path_config = '/opt/zato/instanceFiles/config/'

    path_trackerInstances = "api/tracker/trackedEntities"
    path_tracker = "api/39/tracker"
    path_tracker_Events = "api/39/tracker/events"
    path_metadata = "api/33/metadata"

    #path_runs = "runs.json?"
    path_runs = "api/v2/runs.json"
    cronData = None
    dateDebut = None
    dateFin = None
    all_data = []
    data_value = []
    nbreInteration = 5
    tempsAttente = datetime.now()


    def handle(self):
        
        periode = None
        cron_data = json.loads(self.request.payload.replace("'", "\""))
        self.logger.info('\n:carteVaccination- cron_data  after: {}\n\n'.format(cron_data))
        self.cronData = cron_data
        #system_source = self.request.input.source
        if 'source' in cron_data:
            system_source = cron_data['source']
            self.logger.info('\n\n:getData- system_source = {}'.format(system_source))
        if 'destination' in cron_data:
            system_destination = cron_data['destination']
            self.logger.info('\n\n:getData- system_destination = {}'.format(system_destination))
        
        #mappingFile = self.request.input.mappingFile
        mappingFile = cron_data['mappingFile']
        self.logger.info('\n\n\n:getData- mappingFile = : {}'.format(mappingFile))
        
        #periode = cron_data['periode']
        duration = cron_data['duration']

        if 'dateDebut' in cron_data:
            self.dateDebut = cron_data['startDate']
        if 'dateFin' in cron_data:
            self.dateFin = cron_data['endDate']
        
        self.downloadFile(mappingFile)
        

        self.connexion_source = self.outgoing.plain_http[system_source].conn
        self.connexion_destination = self.outgoing.plain_http[system_destination].conn
        

        if duration is not None:
            data_periode = {'typePeriode': 'rapidPro','duration': duration}
            periode_P = self.invoke('gestion-periode.gestion-periode', data_periode, as_bunch=True)
            temp = json.loads(periode_P)
            liste_periode = temp["liste_periode"]
            self.logger.info('\n\n:getData- liste_periode = {}'.format(liste_periode))
        
        if self.dateDebut is not None and self.dateFin is not None:
            liste_periode = [self.dateDebut,self.dateFin]
        
        self.getOrgUnitAndPragram()
        parametre = self.rapidProParametre(mappingFile)
        parametre = self.ordonnerExecution(parametre)
        
        self.gestionData(parametre,liste_periode)
        
        self.logger.info('\n\n:getData- Fin self.response.payload :')
    

    def downloadFile(self, fileName):
        self.logger.info('\n\n: downloadFile......................... ')
        query_param = {}
        query_param['url_path'] = fileName
        connexion = self.outgoing.plain_http['github'].conn

        response = connexion.get(self.cid, params=query_param)
        self.logger.info('\n\n:getData-  response : {}'.format(response))
        
        #self.enregisterLog(response.text,'github_data')
        self.enregisterFile(response.text,fileName)


    def gestionData(self, parametre,liste_periode):
        self.logger.info('\n\n: gestionData......................... ')
        for param in parametre:
            self.gestionPeriode(param,liste_periode)

    def gestionPeriode(self, param,liste_periode):
        self.logger.info('\n\n: gestionPeriode......................... ')
        cursor = None

        while True:
            reponse = self.collecteData(param,liste_periode,cursor)
            data = self.traitementData(param,reponse['data'])
            self.sendService(data)
            cursor = reponse['cursor']
            self.logger.info('\n\n:gestionPeriode-  cursor : {}\n\n'.format(cursor))
            if cursor is None:
                break
            

    def collecteData(self, param,liste_periode,cursor):
        self.logger.info('\n\n: collecteData ......................... ')
        all_data = []
        interation = range(self.nbreInteration)
        for nbre in interation:
            reponse = self.getdata(param['flowUuid'],liste_periode,cursor)
            if not 'results' in reponse:
                break
            data = self.getValue(reponse['results'])
            all_data.extend(data)
            if reponse["next"] is None:
                cursor = None
                break
            else:
                query = reponse["next"].split('&')
                for curt in query:
                    if 'cursor' in curt:
                        next = curt.split('=')
                        cursor = next[1]
        tmp = {}
        tmp['data'] = all_data
        tmp['cursor'] = cursor
        self.logger.info('\n\n: collecteData FIN !!!!!!!!!!!!!!!!!!! ......................... ')
        return tmp

    def getdata(self, flow,liste_periode,cursor):
        #self.logger.info('\n\n:getData- getdata ......................... ')
        #headers = {'X-App-Name': 'Zato','Authorization': 'token d10aa0d81005e9fc14f4d6d20b4e5388ff6a868d', 'Content-Type': 'application/json'}
        query_param = {}
        query_param['url_path'] = self.path_runs
        query_param['flow'] = flow
        query_param['after'] = liste_periode[0]
        query_param['before'] = liste_periode[1]
        if cursor != None:
            query_param['cursor'] = cursor
                
        response = self.connexion_source.get(self.cid, params=query_param)
        self.logger.info('\n\n:getData-  response : {}'.format(response))
        #self.enregisterLog(response.text,'rapidPro_data.txt')
        data = json.loads(response.text)
        return data

    def getValue(self,results):
        
        data_value = []
        for data in results:
            value = data['values']['declancheur']['input']
            data_value.append(value)
        
        return data_value
                

    def traitementData(self,param,data):
       
        data_value = []

        for dat in data:
            _donne = dat.split('+')
            #meta = self.copyList(param['data'])
            meta = copy.deepcopy(param['data'])
            donnes = self.addValue(meta,_donne)
            if len(donnes) != 0:
                data_value.append(donnes)
        
        return data_value
 

    def addValue(self,meta,data):
        #self.logger.info('\n\n: addValue')

        for met in meta:
            if len(data) -1 < met['position']:
                return []
            met['value'] = data[met['position']]
        
        return meta


    def rapidProParametre(self,mappingFile):
        self.logger.info('\n\n: rapidProParametre')
        data_file =  self.lireLocal(mappingFile,";")
        data_file = self.deleteCaractereSpeciale(data_file)

        metaData = []
        
        self.logger.info('\n\n\n\n: data_file["header"] : {}'.format(data_file["header"]))
        
        for data in data_file["rows"]:
            self.logger.info('\n\n:rapidProParametre data : {}\n\n\n'.format(data))
            if len(data) == 0:
                continue

            tmp = {}
            for index_entete, entete in enumerate(data_file["header"]):
                tmp[entete] = data[index_entete]
            metaData.append(tmp)
        
        self.enregisterLog(json.dumps(metaData, ensure_ascii=False),'rapidPro_metaData')
        
        metaData = self.organiserMetaData(metaData)
        self.enregisterLog(json.dumps(metaData, ensure_ascii=False),'rapidPro_metaData_2')
        
        return metaData
    

    def ordonnerExecution(self,parametre):
        self.logger.info('\n\n: ordonnerExecution')
        ordre = []
        NonOrdre = []
        for param in parametre:
            if not 'ordre' in param:
                NonOrdre.append(param)
                continue

            index = None
            for ind, ord in enumerate(ordre):
                if ord['ordre'] > param['ordre']:
                    index = ind
                    break
            
            if index is None:
                ordre.append(param)
            else:
                ordre.insert(index,param)
                
        ordre.extend(NonOrdre)
        return ordre


    def deleteCaractereSpeciale(self,data_file):
        self.logger.info('\n\n: rapidProParametre')
        lesEntetes = []
        for entete in data_file["header"]:
            lesEntetes.append(entete.replace("\ufeff", ""))
        
        data_file["header"] = lesEntetes

        for data in data_file["rows"]:
            temp = []
            for dat in data:
                temp.append(dat.replace("\ufeff", ""))
            data = temp
        
        return data_file


    def organiserMetaData(self,metaData):
        self.logger.info('\n\n: organiserMetaData')
        _order = []

        for meta in metaData:
            #self.logger.info('\n\n\n\n: meta : {}'.format(meta))
            trouveFlow = False
            for ord in _order:
                if meta['rapidpro_flowUuid'] == ord['flowUuid']:
                    trouveFlow = True
                    ord['data'].append(self.formatedMetaData(meta))
            
            if len(_order) == 0 or trouveFlow == False:
                tmp = {}
                tmp['ordre'] = int(meta['ordreExecution'])
                tmp['flowUuid'] = meta['rapidpro_flowUuid']
                tmp['flowName'] = meta['rapidpro_flowName']
                tmp['declancheur'] = meta['rapidpro_declancheur']
                tmp['data'] = []
                tmp['data'].append(self.formatedMetaData(meta))
                _order.append(tmp)
        
        return _order
                

    def formatedMetaData(self,meta):
        #self.logger.info('\n\n: formatedMetaData')
        tp = {}
        tp['position'] = int(meta['rapidpro_position'])
        tp['name'] = meta['rapidpro_name']
        tp['programName'] = meta['dhis_programName']
        tp['programId'] = meta['dhis_programId']
        tp['programId'] = meta['dhis_programId']
        if meta['dhis_programEnrolmentDate'] != "":
            tp['enrolmentDate'] = ''
        if meta['dhis_programIncidentDate'] != "":
            tp['incidentDate'] = ''
        if meta['dhis_programOrgUnitCode'] != "":
            tp['orgUnitCode'] = ''
        if meta['dhis_attributName'] != "":
            tp['attributName'] = meta['dhis_attributName']
        if meta['dhis_attributId'] != '':
            tp['attributId'] = meta['dhis_attributId']
        if meta['dhis_programStageName'] != "":
            tp['programStageName'] = meta['dhis_programStageName']
        if meta['dhis_programStageId'] != "":
            tp['programStageId'] = meta['dhis_programStageId']
        if meta['dhis_programStageOrgUnitCode'] != "":
            tp['eventOrgUnitCode'] = meta['dhis_programStageOrgUnitCode']
        if meta['dhis_programStageEventDate'] != "":
            tp['eventDate'] = meta['dhis_programStageEventDate']
        if meta['dhis_programStageDataElementName'] != "":
            tp['dataElementName'] = meta['dhis_programStageDataElementName']
        if meta['dhis_programStageDataElementId'] != "":
            tp['dataElementId'] = meta['dhis_programStageDataElementId']
        if meta['dhis_attributValueUniqueName'] != "":
            tp['attributUniqueName'] = meta['dhis_attributValueUniqueName']
        if meta['dhis_attributValueUniqueId'] != "":
            tp['attributUniqueId'] = meta['dhis_attributValueUniqueId']
        if meta['dhis_DataElementValueUniqueName'] != "":
            tp['dataElementUniqueName'] = meta['dhis_DataElementValueUniqueName']
        if meta['dhis_DataElementValueUniqueId'] != "":
            tp['dataElementUniqueId'] = meta['dhis_DataElementValueUniqueId']
        
        return tp


    def sendService(self,data):
        self.logger.info('\n\n: sendService')
        if len(data) != 0:
            #data = {'payload': data, 'cron_data': self.cronData}
            #self.invoke('send-to-dhis-tracker.send', data, as_bunch=True)
            instances = self.traitementRapidProData(data)
            #self.enregisterLog(json.dumps(instances, ensure_ascii=False),'dhis__dataSend')
            self.sendData(instances)


    def getOrgUnitAndPragram(self):
        self.logger.info('\n\n\n:getOrgUnitAndPragram \n\n\n')
        
        params_metadata = {}
        params_metadata['organisationUnits'] = True
        params_metadata['programs'] = True
        
        params_source = {}
        params_source = params_metadata
        params_source['url_path'] = self.path_metadata

        response = self.connexion_destination.get(self.cid, params=params_source)
        self.logger.info('\n:transfert- response : {}\n\n'.format(response))
        metadata = json.loads(response.text)
        #self.enregisterLog(json.dumps(metadata, ensure_ascii=False),'metadata_get')
        self.allOrgUnit = metadata['organisationUnits']
        self.allProgram = metadata['programs']


    def traitementRapidProData(self,data):

        self.logger.info('\n\n: traitementRapidProData')

        dhisData = []
        for rapidPro_data in data:
            #self.enregisterLog(json.dumps(rapidPro_data, ensure_ascii=False),'rapidPro_data')
            #self.logger.info('\n\n\n: rapidPro_data = : {} \n\n'.format(json.dumps(rapidPro_data, ensure_ascii=False)))
            donnee = self.dhisFormat(rapidPro_data)
            if donnee is not None:
                dhisData.append(donnee)
        
        return dhisData
    

    def dhisFormat(self,data):
        #self.logger.info('\n\n\n:dhisFormat \n\n\n')
        trackerInstance = None
        self.controleTempsAttente()
        old_instance = False
        for dat in data:
            if 'attributUniqueId' in dat and 'value' in dat and dat['value'] != '':
                old_instance = True
                self.updateDhisInstance(dat['attributUniqueId'],dat['value'],dat['programId'],data)
                return None
        
        if not old_instance:
            trackerInstance = self.newDhisInstance(data)
        
        return trackerInstance


    def controleTempsAttente(self):
        #self.logger.info('\n\n\n:controleTempsAttente \n\n\n')
        
        tempsActuel = datetime.now()
        ecart = tempsActuel - self.tempsAttente
        minutes = ecart.total_seconds() / 60
        #self.logger.info('\n\n\n: Nombre minutes = : {} \n\n'.format(minutes))
        if minutes > 3:
            self.logger.info('\n\n: Nombre minutes = : {} \n\n'.format(minutes))
            params_source = {}
            params_source['url_path'] = "api/organisationUnits/hazsksuasas"
            response = self.connexion_destination.get(self.cid, params=params_source)
            self.logger.info('\n\n\n: response = : {} \n\n'.format(response))
            self.tempsAttente = tempsActuel


    def newDhisInstance(self,data):
        #self.logger.info('\n\n\n:newDhisInstance \n\n\n')
        trackerInstance = None
        trackedType = None
        orgUnit = None
        attributs = []
        enrolments = []
        attributs = self.getAttribut(data)
        enrolments = self.getEnrolment(data,attributs)
        #self.logger.info('\n\n\n:dhisFormat after attributs and enrolments\n\n\n')
        for dat in data:
            if 'orgUnitCode' in dat:
                orgUnit = self.getOrgUnitIdbyCode(dat['value'])
            if 'programId' in dat:
                trackedType = self.getTrackerType(dat['programId'])
        
        #self.logger.info('\n\n\n: orgUnit = : {}  /// trackedType = : {} \n\n'.format(orgUnit,trackedType))
        if trackedType is not None and orgUnit is not None:
            trackerInstance = {}
            trackerInstance['trackedEntityType'] = trackedType
            trackerInstance['orgUnit'] = orgUnit
            #if len(attributs) != 0:
                #trackerInstance['attributes'] = attributs
            if len(enrolments) != 0:
                trackerInstance['enrollments'] = enrolments

        #self.logger.info('\n\n\n: trackerInstance = : {} \n\n'.format(json.dumps(trackerInstance, ensure_ascii=False)))
        
        return trackerInstance
            

    def updateDhisInstance(self,attribut,uniqueValue,programId,data):
        #self.logger.info('\n\n\n:getProgram programId = : {}'.format(programId))
        
        instanceId = self.searchInstance(uniqueValue,attribut,programId,data)
        self.logger.info('\n\n\n:getProgram instanceId = : {}'.format(instanceId))
        if instanceId is not None:
            instance = self.getInstance(instanceId,programId)
            self.updateEventValue(instance,programId,data)
            self.updateAttribut(instance,programId,data,attribut,uniqueValue)



    def updateAttribut(self,instance,programId,data,attribut,uniqueValue):
        #self.logger.info('\n\n\n :getAttribut ................... ')
        attributs = []
        orgUnit = None
        enrolmentDate = None

        attributs = self.getAttribut(data)
        for dat in data:
            if 'orgUnitCode' in dat:
                orgUnit = self.getOrgUnitIdbyCode(dat['value'])
            if 'enrolmentDate' in dat:
                enrolmentDate = self.convertToDate(dat['value'])

        if len(attributs) != 0:
            instance['attributes'] = attributs
            if orgUnit is not None:
                instance['orgUnit'] = orgUnit
            for enrol in instance['enrollments']:
                if enrol['program'] == programId:
                    enrol['attributes'] = attributs
                    if orgUnit is not None:
                        enrol['orgUnit'] = orgUnit
                    if enrolmentDate is not None:
                        enrol['enrolledAt'] = enrolmentDate
            
            data_payload = {}
            data_payload['trackedEntities'] = []
            data_payload['trackedEntities'].append(instance)
            payload = json.dumps(data_payload, ensure_ascii=False)
            self.sendOneData(payload)


    def getAttribut(self,data):
        #self.logger.info('\n\n\n :getAttribut ................... ')
        attibuts = []
        for _dat in data:
            if 'attributId' in _dat:
                tmp = {}
                tmp['attribute'] = _dat['attributId']
                if 'DATE' in _dat['name'].upper():
                    tmp['value'] = self.convertToDate(_dat['value'])
                else:
                    tmp['value'] = _dat['value']
                
                if _dat['value'] != "":
                    attibuts.append(tmp)
        #self.logger.info('\n\n\n:getAttribut attibuts = : {}'.format(json.dumps(attibuts, ensure_ascii=False)))
        
        return attibuts
    
    def convertToDate(self,data):
        #self.logger.info('\n\n\n :convertToDate ................... ')
        if '-' in data:
            return data
        
        slice_year = slice(4)
        slice_month = slice(4,6)
        slice_day = slice(6,8)
        year = data[slice_year]
        month = data[slice_month]
        day = data[slice_day]
        ladate = year + "-" + month + "-" + day
        return ladate

    def getEnrolment(self,data,attributs):
        #self.logger.info('\n\n\n :getEnrolment ................... ')

        lesEnrolment = self.formatEnrolment(data)

        for _dat in data:
            if 'programId' in _dat:
                for enrol in lesEnrolment:
                    if enrol['program'] == _dat['programId']: 
                        enrol['events'] = self.getEvents(data)
                        if len(attributs) != 0:
                            enrol['attributes'] = attributs
        #self.logger.info('\n\n\n:getEnrolment lesEnrolment = : {} \n\n'.format(json.dumps(lesEnrolment, ensure_ascii=False)))
        
        return lesEnrolment


    def getEvents(self,data):
        #self.logger.info('\n\n\n:getEvents ...................')
        lesEvents = self.formatEvent(data)

        for dat in data:
            if 'programStageId' in dat:
                for event in lesEvents:
                    if dat['programStageId'] == event['programStage']:
                        if 'dataElementId' in dat:
                            dataValue = {}
                            dataValue['dataElement'] = dat['dataElementId']
                            dataValue['value'] = dat['value']
                            if dat['value'] != "":
                                event['dataValues'].append(dataValue)
        #self.logger.info('\n\n\n:getEvents lesEvents = : {} \n\n'.format(json.dumps(lesEvents, ensure_ascii=False)))
        #self.enregisterLog(json.dumps(lesEvents, ensure_ascii=False),'dhis_lesEvents_data')
        return lesEvents


    def formatEvent(self,data):
        #self.logger.info('\n\n\n:formatEvent ...................')
        listProgramStageId = []
        lesEvents = []

        for dat in data:
            if 'programStageId' in dat:
                
                trouve = False
                for _ids in listProgramStageId:
                    if _ids == dat['programStageId']:
                        trouve = True
                        break
                if not trouve:
                    listProgramStageId.append(dat['programStageId'])
        
        for _id in listProgramStageId:
            orgUnit = None
            evenDate = None
            for dat in data:
                if 'programStageId' in dat:
                    if dat['programStageId'] == _id:
                        if 'eventOrgUnitCode' in dat:
                            orgUnit = self.getOrgUnitIdbyCode(dat['value'])
                        if 'eventDate' in dat:
                            evenDate = self.convertToDate(dat['value'])
            
            if orgUnit is not None and evenDate is not None:
                tmp = {}
                tmp['orgUnit'] = orgUnit
                tmp['occurredAt'] = evenDate
                tmp['programStage'] = _id
                tmp['dataValues'] = []
                lesEvents.append(tmp)
        #self.logger.info('\n\n\n:formatEvent lesEvents = : {} \n\n'.format(json.dumps(lesEvents, ensure_ascii=False)))
        #self.enregisterLog(json.dumps(lesEvents, ensure_ascii=False),'dhis_lesEvents')
        return lesEvents

    def formatEnrolment(self,data):
        #self.logger.info('\n\n\n:formatEnrolment ...................')
        listProgramId = []
        lesEnrolment = []

        for dat in data:
            if 'programId' in dat:
                trouve = False
                for _ids in listProgramId:
                    if _ids == dat['programId']:
                        trouve = True
                        break
                if not trouve:
                    listProgramId.append(dat['programId'])
        
        for _id in listProgramId:
            enrolmentDate = None
            orgUnit = None
            for dat in data:
                if 'programId' in dat:
                    if dat['programId'] == _id:
                        if 'orgUnitCode' in dat:
                            orgUnit = self.getOrgUnitIdbyCode(dat['value'])
                        if 'enrolmentDate' in dat:
                            enrolmentDate = self.convertToDate(dat['value'])
            
            if enrolmentDate is not None and orgUnit is not None:
                tmp = {}
                tmp['orgUnit'] = orgUnit
                tmp['enrolledAt'] = enrolmentDate
                tmp['program'] = _id
                tmp['events'] = []
                lesEnrolment.append(tmp)
        
        #self.logger.info('\n\n\n:formatEnrolment listProgramId = : {} \n\n'.format(json.dumps(listProgramId, ensure_ascii=False)))
        #self.logger.info('\n\n\n:formatEnrolment lesEnrolment = : {} \n\n'.format(json.dumps(lesEnrolment, ensure_ascii=False)))
        #listProgramId_______________________
        return lesEnrolment
        

    def getOrgUnitIdbyCode(self,code):
        #self.logger.info('\n\n\n:getOrgUnitId \n\n\n')
        for unit in self.allOrgUnit:
            if 'code' in unit:
                if unit['code'] == code:
                    return unit['id']
        return None
    
    def getTrackerType(self,programId):
        #self.logger.info('\n\n\n:getTrackerType \n\n\n')
        for prog in self.allProgram:
            if prog['id'] == programId:
                if 'trackedEntityType' in prog:
                    return prog['trackedEntityType']['id']
        return None
                

    def getOrgUnitAndPragram(self):
        self.logger.info('\n\n\n:getOrgUnitAndPragram \n\n\n')
        
        params_metadata = {}
        params_metadata['organisationUnits'] = True
        params_metadata['programs'] = True
        
        params_source = {}
        params_source = params_metadata
        params_source['url_path'] = self.path_metadata

        response = self.connexion_destination.get(self.cid, params=params_source)
        self.logger.info('\n:transfert- response : {}\n\n'.format(response))
        metadata = json.loads(response.text)
        #self.enregisterLog(json.dumps(metadata, ensure_ascii=False),'metadata_get')
        self.allOrgUnit = metadata['organisationUnits']
        self.allProgram = metadata['programs']
        

    def searchInstance(self,uniqueValue,attributId,programId,data):
        self.logger.info('\n\n\n:searchInstance \n\n\n')
        #self.enregisterLocal(dumps(data, ensure_ascii=False),'data_search.txt')
        #uniqueValue = "072105010210014"
        orgUnit = None
        instanceId = None
        for dat in data:
            if 'eventOrgUnitCode' in dat:
                orgUnit = self.getOrgUnitIdbyCode(dat['value'])
                break

        url_path_query = "api/trackedEntityInstances/query"
        params_select = {}
        params_select['ou'] = orgUnit
        params_select['ouMode'] = 'ACCESSIBLE'
        params_select['paging'] = False
        params_select['program'] = programId
        params_select['attribute'] = attributId + ':EQ:'+ uniqueValue
        
        params_select['url_path'] = url_path_query

        response = self.connexion_destination.get(self.cid, params=params_select)
        self.logger.info('\nL31:transfert- response : {}\n\n'.format(response))
        response_data = json.loads(response.text)
        self.logger.info('\nL31:transfert- response_text : {}\n\n'.format(response_data))
        #self.enregisterLocal(json.dumps(response_data, ensure_ascii=False),'dhis__SearchValeurUnique')
        
        if 'rows' in response_data:
            if len(response_data['rows']) != 1:
                return None
        instanceId = response_data['rows'][0][0]        
        return instanceId


    
    def getInstance(self,instanceId,programId):
        self.logger.info('\n\n\n:getInstance \n\n\n')
        #url_path_instances = "api/33/trackedEntityInstances "

        params_source = {}
        params_source['program'] = programId
        params_source['fields'] = '*'

        params_source['url_path'] = self.path_trackerInstances + '/' + instanceId
        response = self.connexion_destination.get(self.cid, params=params_source)
        self.logger.info('\n\n:getInstance- response : {}\n\n'.format(response))
        data_reponse = json.loads(response.text)
        self.logger.info('\n\n:getInstance- data_reponse : {}\n\n'.format(data_reponse))
        #self.enregisterLog(json.dumps(data_reponse, ensure_ascii=False),'dhis_InstanceData')
        return data_reponse


    def updateEventValue(self,instance,programId,data):
        self.logger.info('\n\n\n:updateEventValue ...................')
        
        for enrol in instance['enrollments']:
            if enrol['program'] == programId:
                self.gestionEnrolment(enrol,programId,data,enrol['enrollment'])


    def gestionEnrolment(self,enrol,programId,data,enrollmentId):
        self.logger.info('\n\n\n:gestionEnrolment ...................')
        programData = []
        dataElementUniqueId = None
        dataElementUniqueValue = None
        old_event = None
        for dat in data:
            if 'programId' in dat:
                if dat['programId'] == programId:
                    programData.append(dat)
            if 'dataElementUniqueId' in dat:
                self.logger.info('\n:getInstance- dat : {}\n\n'.format(dat))
                dataElementUniqueId = dat['dataElementUniqueId']
                dataElementUniqueValue = dat['value']

        NewEvent = self.creerEvent(programData,enrollmentId)
        
        if dataElementUniqueId is not None:
            old_event = self.searchEvent(enrol['events'],dataElementUniqueId,dataElementUniqueValue)
        
        if old_event is not None:
            NewEvent['event'] = old_event['event']
        
        if NewEvent is not None:
            dataSend = {}
            dataSend['events'] = []
            dataSend['events'].append(NewEvent)
            payload = json.dumps(dataSend, ensure_ascii=False)
            #self.sendEventData(payload)
            self.logger.info('\n:getInstance- payload : {}\n\n'.format(payload))
            self.sendOneData(payload)
           

    def searchEvent(self,events,dataElementUniqueId,dataElementUniqueValue):
        self.logger.info('\n\n\n:searchEvent \n\n\n')
        for event in events:
            for dataValue in event['dataValues']:
                if dataValue['dataElement'] == dataElementUniqueId:
                    if dataValue['value'] == dataElementUniqueValue:
                        return event
        return None

    def creerEvent(self,data,enrollmentId):
        self.logger.info('\n\n\n:getInstance \n\n\n')
        event = None
        orgUnit = None
        eventDate = None
        programStageId = None
        dataValues = []
        for dat in data:
            if 'eventOrgUnitCode' in dat:
                orgUnit = self.getOrgUnitIdbyCode(dat['value'])
            if 'eventDate' in dat:
                eventDate = self.convertToDate(dat['value'])
            if 'programStageId' in dat:
                programStageId = dat['programStageId']
            if 'dataElementId' in dat:
                tmp = {}
                tmp['dataElement'] = dat['dataElementId']
                if 'DATE' in dat['name'].upper():
                    tmp['value'] = self.convertToDate(dat['value'])
                else:
                    tmp['value'] = dat['value']
                
                if dat['value'] != '':
                    dataValues.append(tmp)

        if orgUnit is not None and eventDate is not None and programStageId is not None:
            event = {}
            event['occurredAt'] = eventDate
            event['orgUnit'] = orgUnit
            event['programStage'] = programStageId
            event['enrollment'] = enrollmentId
            event['dataValues'] = dataValues

        return event


    def sendData(self, data):
        
        for dat in data:
            data_payload = {}
            data_payload['trackedEntities'] = []
            data_payload['trackedEntities'].append(dat)
            payload = json.dumps(data_payload, ensure_ascii=False)
            self.sendOneData(payload)
       
        
        
    def sendOneData(self, payload):
        self.logger.info('\n\n\n:sendData data = : {}'.format(payload))
        query_param = {}
        query_param['url_path'] = self.path_tracker
        query_param['async'] = 'false'

        response = self.connexion_destination.post(self.cid, payload, params=query_param)
        self.logger.info('\n\n\n:sendData response = : {}'.format(response))
        self.logger.info('\n\n\n:sendData response text = : {}'.format(response.text))
        #self.enregisterLocal(response.text,'dhis_response')
        #self.enregisterSuiteLog(response.text,'dhis_response_')
        #self.enregisterSuiteLog("\n",'dhis_response_enregPers')
        self.logger.info('\n\n\n:sendData: Envoi réussi.')

        return response

    def sendEventData(self, payload):
        #self.logger.info('\n\n\n:sendData data_payload = : {}'.format(data_payload))
        self.logger.info('\n\n\n:sendData data = : {}'.format(payload))
        query_param = {}
        query_param['url_path'] = self.path_tracker_Events
        query_param['async'] = 'false'

        response = self.connexion_destination.post(self.cid, payload, params=query_param)
        self.logger.info('\n\n\n:sendData response = : {} \n\n'.format(response))
        self.logger.info('\n\n\n:sendData response text = : {} \n\n'.format(response.text))
        #self.enregisterLog(response.text,'dhis_response')
        #self.enregisterSuiteLog(response.text,'dhis_response_')
        #self.enregisterSuiteLog("\n",'dhis_response_enregPers')
        self.logger.info('\n\n\n:sendData: Envoi réussi.')

        return response
   







    def lireLocal(self,filename,delimiteur):
        self.logger.info('\n\n: lireLocal')
        
        #file_path = '/opt/zato/instanceLog/'+filename
        file_path = self.file_path_config+filename

        donnee = {}
        rows = []
        try:
            file = open(file_path, 'r', encoding='utf8')
            self.logger.info('\n\n:lireLocal- fichier Trouver: \n\n')
            self.logger.info('\n\n:lireLocal- file : {}\n\n'.format(file))
            csvreader = csv.reader(file,delimiter=delimiteur)
            header = next(csvreader)
            for row in csvreader:
                rows.append(row)
            file.close()
            donnee['header'] = header
            donnee['rows'] = rows
            self.logger.info('\n\n:lireLocal- data : \n\n')
            self.logger.info('\n\n:lireLocal- data : {}\n\n'.format(donnee))
            #self.enregisterLog(json.dumps(donnee, ensure_ascii=False),'csv_data.txt')
            return donnee
        except IOError:
            self.logger.info('\n\n:lireLocal- IOError de fichier: \n\n')
            self.logger.info('\n\n:lireLocal- IOError : {}\n\n'.format(IOError))
            
            self.enregisterLog(IOError,'csv_error.txt')
            return []   


    def enregisterLog(self, data,filename):
        #file_path = '/opt/zato/instanceLog/'+filename
        file_path = self.file_path_log+filename
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        json_file = open(file_path, 'w', encoding='utf8')
        json_file.write(str(data))
        json_file.write('\n')
        json_file.close()
        #self.logger.info('\n\n:enregisterLocal- Enregistrement reussi .......\n\n')

    def enregisterSuiteLog(self, data,filename):
        file_path = self.file_path_log+filename
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        json_file = open(file_path, 'a', encoding='utf8')
        json_file.write(str(data))
        json_file.close()
        #self.logger.info('\n\nL31:enregisterLocal- Enregistrement reussi .......\n\n')
    
    def enregisterFile(self, data,filename):
        #file_path = '/opt/zato/instanceLog/'+filename
        file_path = self.file_path_config+filename
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        json_file = open(file_path, 'w', encoding='utf8')
        json_file.write(str(data))
        json_file.write('\n')
        json_file.close()
        #self.logger.info('\n\n:enregisterLocal- Enregistrement reussi .......\n\n')