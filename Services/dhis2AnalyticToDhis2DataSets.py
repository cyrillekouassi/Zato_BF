import json
from bunch import bunchify
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
import csv
import copy

# Zato
from zato.server.service import Service

class scheduler(Service):

    path_program = "api/programs"
    path_dataSet = "api/dataSets"
    path_ouGroup = "api/organisationUnitGroups"

    path_analytics = "api/analytics?"
    path_dataValues = "api/dataValues"
    path_dataValueSets = "api/dataValueSets"

    file_path_config = '/opt/zato/instanceFiles/config/'
    file_path_log = '/opt/zato/instanceFiles/log/'

    source_connexion = None
    destination_connexion = None
    
    nbre_periode = 50
    nbre_orgUnit = 50
    nbre_element = 1

    allPeriode = {}
    allCategorie =[]
    categoryOptionCombosDefault = None

    tempsAttente = datetime.now()


    def handle(self):

        periode_fixe = None
        periode = None
        self.logger.info('\n\n\n:scheduler- Initial data payload received : {}'.format(self.request.payload))
        cron_data = json.loads(self.request.payload.replace("'", "\""))
        #self.logger.info('\n\n\n:scheduler- cron_data : {}'.format(cron_data))
        
        mappingFile = cron_data['mappingFile']
        self.downloadFile(mappingFile)

        metaData = self.gestionParametre(mappingFile)
        metaData = self.ordonnerExecution(metaData)
        self.enregisterLocal(json.dumps(metaData, ensure_ascii=False),'mapping_metaData_2')
        
        if 'duration' in cron_data:
            periode = cron_data['duration']
        
        if 'periode_fixe' in cron_data:
            periode_fixe = cron_data['periode_fixe']
        temp_debut = datetime.now()

        # Get Periode
        if periode_fixe is not None:
            self.allPeriode['fixe'] = periode_fixe.split(";")
        else:
            self.allPeriode = self.getPeriode(periode)
        

        for meta in metaData:
            for mapp in meta['mapping']:
                for period in mapp['periode']:
                        typePeriod = None
                        source = meta['source']
                        destination = mapp['destination']
                        if 'typePeriode' in period:
                            typePeriod = period['typePeriode']
                        self.selectedOrgUnit(source,destination,typePeriod,period['orgUnit'])
                        
        temp_fin = datetime.now()
        self.logger.info('\n\n -scheduler: temp_debut : {} /// temp_fin : {}'.format(temp_debut,temp_fin))
        self.logger.info('\n\n\n:scheduler- Fin execution du service */*')

    def downloadFile(self, fileName):
        self.logger.info('\n\n: downloadFile......................... ')
        query_param = {}
        query_param['url_path'] = fileName
        connexion = self.outgoing.plain_http['GITHUB'].conn

        response = connexion.get(self.cid, params=query_param)
        self.logger.info('\n\n:getData-  response : {}'.format(response))
        
        #self.enregisterLog(response.text,'github_data')
        self.enregisterFile(response.text,fileName)


    def selectedOrgUnit(self,source,destination,typePeriod,orgUnit):
        #self.logger.info('\n\n Entrer dans la methode selectedOrgUnit')
        self.logger.info('\n\n\n\nget: orgUnit : {}\n\n'.format(orgUnit))

        self.source_connexion = self.outgoing.plain_http[source].conn
        self.destination_connexion = self.outgoing.plain_http[destination].conn
        
        for org in orgUnit:
            self.logger.info('\n\nget: org : {}\n\n'.format(org))
            ou_set = self.recupererOrgUnit(source,destination,org)
            if 'fixe' in self.allPeriode:
                period = self.allPeriode['fixe']
            else:
                period = self.allPeriode[typePeriod]
            #self.logger.info('\n\nget: ou_set : {}'.format(ou_set))
            self.gestionCollecteData(period,ou_set,org['data'])


    def gestionCollecteData(self,period,ou_set,element):
        #self.logger.info('\n\n Entrer dans la methode gestionCollecteData')
        self.gestionCollecteOrgUnitSet(period,ou_set,element)


    def gestionCollecteOrgUnitSet(self,period,ou_set,element):
        #self.logger.info('\n\n Entrer dans la methode gestionCollectePeriod')
        #self.logger.info('\n\nget: ou_set : {}'.format(ou_set))
        
        lesOrgUnit = []
        nbre = 0
        for index_var, var in enumerate(ou_set):
            lesOrgUnit.append(var)
            nbre += 1
            if nbre < self.nbre_orgUnit and index_var + 1 < len(ou_set):
                continue
            
            self.gestionCollectePeriod(period,lesOrgUnit,element)
            #self.gestionCollecteElement(period,lesOrgUnit,element)
            lesOrgUnit.clear()
            nbre = 0
            self.logger.info('\n\n Fin OrgUnitSet !!!!!!!!!!!!!!!!!!!!\n\n')
        
        

    def gestionCollectePeriod(self,period,ou_set,element):
        #self.logger.info('\n\n Entrer dans la methode gestionCollectePeriod')
        lesPeriod = []
        nbre = 0
        for index_var, var in enumerate(period):
            lesPeriod.append(var)
            nbre += 1
            if nbre < self.nbre_periode and index_var + 1 < len(period):
                continue
            
            self.gestionCollecteElement(lesPeriod,ou_set,element)
            #self.gestionCollecteOrgUnitSet(lesPeriod,ou_set,element)
            lesPeriod.clear()
            nbre = 0
            self.logger.info('\n\n Fin Period !!!!!!!!!!!!!!!!!!!!\n\n')
        

    def gestionCollecteElement(self,period,ou_set,element):
        #self.logger.info('\n\n Entrer dans la methode gestionCollectePeriod')
        lesElements = []
        nbre = 0
        for index_var, var in enumerate(element):
            lesElements.append(var)
            nbre += 1
            if nbre < self.nbre_element and index_var + 1 < len(element):
                continue
            
            self.prepareData(period,ou_set,lesElements)
            lesElements.clear()
            nbre = 0
            self.logger.info('\n\n Fin Element !!!!!!!!!!!!!!!!!!!!\n\n')
        
        #Element________________________
            

    def formatRequete(self, lists):
        #self.logger.info('\n\n L23-get: Entrer dans la methode formatRequete')
        laListe = ""
        for index_var, var in enumerate(lists):
            laListe = laListe + var

            if index_var + 1 < len(lists):
                laListe = laListe + ';'

        return laListe

    def formatElement(self, lists):
        #self.logger.info('\n\n L23-get: Entrer dans la methode formatRequete')
        listElement = ''
        for index_var, var in enumerate(lists):
            if 'source_categorieComboOptionId' in var:
                listElement = listElement + var['source_dataElementId'] +'.'+ var['source_categorieComboOptionId']
            else:
                if 'source_dataElementId' in var:
                    listElement = listElement + var['source_dataElementId']
                if 'source_indicatorId' in var:
                    listElement = listElement + var['source_indicatorId']
            
            if index_var + 1 < len(lists):
                listElement = listElement + ';'

        return listElement
    
    def formatFilter(self, lists):
        #self.logger.info('\n\n L23-get: Entrer dans la methode formatRequete')
        listElement = ''
        for var in lists:
            if 'source_filter' in var:
                if not var['source_filter'] in listElement:
                    listElement = listElement + var['source_filter']
        if listElement == '':
            return None
        return listElement


    def prepareData(self,period,ou_set,elements):
        #self.logger.info('\n\n Entrer dans la methode prepareData')
        metaData = []
        listPeriod = ""
        listOrgUnit = ""
        listElement = ""
        
        listPeriod = self.formatRequete(period)
        listOrgUnit = self.formatRequete(ou_set)
        listElement = self.formatElement(elements)
        listFilter = self.formatFilter(elements)
        
        #self.logger.info('\n\nget: listPeriod : {}'.format(listPeriod))
        #self.logger.info('\n\nget: listOrgUnit : {}'.format(listOrgUnit))
        #self.logger.info('\n\nget: listElement : {}'.format(listElement))

        for per in period:
            for ou in ou_set:
                for elem in elements:
                    tp = copy.deepcopy(elem)
                    if 'source_categorieComboOptionId' in elem:
                        tp['source_de_co'] = elem['source_dataElementId'] +'.'+ elem['source_categorieComboOptionId']
                    else:
                        if 'source_dataElementId' in elem:
                            tp['source_de_co'] = elem['source_dataElementId']
                        if 'source_indicatorId' in elem:
                            tp['source_de_co'] = elem['source_indicatorId']
                    

                    tp['period'] = per
                    tp['orgUnit'] = ou
                    metaData.append(tp)

        dataRespone = self.collecteData(listPeriod,listOrgUnit,listElement,listFilter,'source')
        metaData = self.addDataValue(dataRespone,metaData)
        #self.enregisterLocal(json.dumps(metaData, ensure_ascii=False),'metaData_3')
        #self.enregisterLocal(json.dumps(dataRespone, ensure_ascii=False),'dataRespone')
        
        self.prepareSendData(metaData)


    def recupererOrgUnit(self,source,destination,orgUnit):
        self.logger.info('\n\n Entrer dans la methode recupererOrgUnit')
        _path = None
        instance = None
        orgUnit_Path  = None
        
        if 'source_dataSetId' in orgUnit:
            instance = source
            orgUnit_Path = self.path_dataSet + "/" + orgUnit['source_dataSetId'] + "?fields=organisationUnits"

        if 'source_programId' in orgUnit:
            instance = source
            orgUnit_Path = self.path_program + "/" + orgUnit['source_programId'] + "?fields=organisationUnits"
        
        if 'source_ouGroup' in orgUnit:
            instance = source
            orgUnit_Path = self.path_ouGroup + "/" + orgUnit['source_ouGroup'] + "?fields=organisationUnits"
        
        if 'destination_dataSetId' in orgUnit:
            instance = destination
            orgUnit_Path = self.path_dataSet + "/" + orgUnit['destination_dataSetId'] + "?fields=organisationUnits"

        if 'destination_programId' in orgUnit:
            instance = destination
            orgUnit_Path = self.path_program + "/" + orgUnit['destination_programId'] + "?fields=organisationUnits"
        
        if 'destination_ouGroup' in orgUnit:
            instance = destination
            orgUnit_Path = self.path_ouGroup + "/" + orgUnit['destination_ouGroup'] + "?fields=organisationUnits"
        
        self.logger.info('\n\nget: instance : {}'.format(instance))
        self.logger.info('\n\nget: orgUnit_Path : {}'.format(orgUnit_Path))

        connexion = self.outgoing.plain_http[instance].conn
        self.logger.info('\n\n: connexion = {}'.format(connexion))


        program_OrgUnit_Path = {}
        program_OrgUnit_Path['url_path'] = orgUnit_Path
        response_orgUnit = connexion.get(self.cid, params=program_OrgUnit_Path)
        
        self.logger.info('\n\n: response = {}'.format(response_orgUnit))
        data_orgUnit = json.loads(response_orgUnit.text)
        #self.logger.info('\n\n L65-get: data_orgUnit = {}'.format(data_orgUnit))
        
        orgUnitID = data_orgUnit['organisationUnits']
        #self.logger.info('\n\n L68-get: orgUnitID = {}'.format(orgUnitID))
        liste_orgUnitID = []
        for ou in orgUnitID:
            liste_orgUnitID.append(ou['id'])
        
        #self.logger.info('\n\nget: liste_orgUnitID : {}'.format(liste_orgUnitID))
        #self.enregisterLocal(json.dumps(liste_orgUnitID, ensure_ascii=False),'ou_set')
        return liste_orgUnitID
    
    def getPeriode(self,periode):
        #self.logger.info('\n\n Entrer dans la methode getPeriode')
        data_periode = {'duration': periode}
        periode_P = self.invoke('gestion-periode.gestion-periode', data_periode, as_bunch=True)
        self.logger.info('\n\n : periode_P = {}'.format(periode_P))

        period = json.loads(periode_P.replace("'", "\""))
        return period['liste_periode']


    def getSendData(self,cron_data):
        
        self.logger.info('\n\n Entrer dans la methode getPeriode')
        response = self.invoke('get-dhis2-analytic.get-data', cron_data, as_bunch=True)
        
        data = {'payload': response, 'cron_data': cron_data}
        self.logger.info('\n\n\n L18:scheduler-  Final data payload received : {}'.format(data))
    
        self.invoke('send-to-dhis2-data-set.send', data, as_bunch=True)
        #update_response = self.invoke('oh-send-data-to-dhis2.send-data-to-dhis2', data, as_bunch=True)


    def gestionParametre(self,mappingFile):
        #self.logger.info('\n\n Entrer dans la methode gestionParamtre')
        data_file =  self.lireLocal(mappingFile,";")
        data_file = self.deleteCaractereSpeciale(data_file)

        metaData = []
        
        self.logger.info('\n\n\n\n: data_file["header"] : {}'.format(data_file["header"]))
        
        for data in data_file["rows"]:
            tmp = {}
            for index_entete, entete in enumerate(data_file["header"]):
                tmp[entete] = data[index_entete]
            metaData.append(tmp)
        
        metaData = self.organiserMetaData(metaData)
        #self.enregisterLocal(json.dumps(metaData, ensure_ascii=False),'mapping_metaData_2')
        return metaData

    def ordonnerExecution(self,metaData):
        #self.logger.info('\n\n: ordonnerExecution')
        metaData = self.ordonner(metaData)
        for meta in metaData:
            meta['mapping'] = self.ordonner(meta['mapping'])

        return metaData            

    def ordonner(self,parametre):
        #self.logger.info('\n\n: ordonner')
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
        #self.logger.info('\n\n: deleteCaractereSpeciale')
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
        #self.logger.info('\n\n: organiserMetaData')
        _order = []
        
        for meta in metaData:
            self.initialeCategorie(meta)
            source = None
            trouveSource = False
            for ord in _order:
                if meta['source_name'] == ord['source']:
                    trouveSource = True
                    source = meta['source_name']
                    ord['mapping'] = self.destinationMetaData(ord['mapping'],meta)
            self.controleTempsAttente(source)
            if len(_order) == 0 or not trouveSource:
                if meta['source_name'] != '':
                    _order.append(self.initialeMapping(meta))
                    self.enregisterLocal(json.dumps(_order, ensure_ascii=False),'order_metaData')
        
        self.enregisterLocal(json.dumps(_order, ensure_ascii=False),'orderMetaData')
        #self.enregisterLocal(json.dumps(self.allCategorie, ensure_ascii=False),'allCategorie')
        _order = self.appliquerCategorie(_order)
        self.enregisterLocal(json.dumps(_order, ensure_ascii=False),'orderMetaData_new')
        return _order

    def destinationMetaData(self,mapping,meta):
        trouveDestination = False
        for map in mapping:
            if map['destination'] == meta['destination_name']:
                trouveDestination = True
                trouvePeriode = False
                for period in map['periode']:
                    if period['typePeriode'] == meta['typePeriode']:
                        trouvePeriode = True
                        trouveOrgUnit = False
                        for ou in period['orgUnit']:
                            locatisation = self.gestionOrgUnit(meta)
                            self.logger.info('\n\n: locatisation : {}\n\n'.format(locatisation))
                            trouveOrgUnit = self.controleOrgUnit(locatisation,ou)
                            if trouveOrgUnit:
                                ou['data'].append(self.gestionData(meta))
                        
                        if not trouveOrgUnit:
                            orgUnit = self.initialeOrgUnit(meta)
                            if len(orgUnit) != 0:
                                period['orgUnit'].append(orgUnit)
                
                if not trouvePeriode:
                    map['periode'].append(self.initialePeriod(meta))

        if not trouveDestination:
            mapping.append(self.initialeDestination(meta))
                    
        return mapping

    def controleOrgUnit(self,times,period):
        #self.logger.info('\n\n: formatedMetaData')
        self.logger.info('\n\n:controleOrgUnit__ times : {}\n\n'.format(times))
        self.logger.info('\n\n:controleOrgUnit__ period : {}\n\n'.format(period))
        if len(times) == 0:
            return False
        
        timesCles = times.keys()
        periodCles = period.keys()

        #self.logger.info('\n\n:controleOrgUnit__ timesCles : {}\n\n'.format(timesCles))
        #self.logger.info('\n\n:controleOrgUnit__ periodCles : {}\n\n'.format(periodCles))

        for keyTime in timesCles:
            #self.logger.info('\n\n:controleOrgUnit__ key : {}\n\n'.format(keyTime))
            if keyTime in period:
                if times[keyTime] == period[keyTime]:
                    return True
        return False

    def initialeMapping(self,meta):
        #self.logger.info('\n\n: formatedMetaData')
        tmp = {}
        if 'ordreExecution' in meta and meta['ordreExecution'] != '':
            tmp['ordre'] = int(meta['ordreExecution'])
        tmp['source'] = meta['source_name']
        tmp['mapping'] = []
        
        tmp['mapping'].append(self.initialeDestination(meta))

        return tmp

    def initialeDestination(self,meta):
        #self.logger.info('\n\n: initialeDestination')
        tp = {}
        if 'ordreExecution' in meta and meta['ordreExecution'] != '':
            tp['ordre'] = int(meta['ordreExecution'])
        tp['destination'] = meta['destination_name']
        tp['periode'] = []
        tp['periode'].append(self.initialePeriod(meta))
        return tp

    def initialePeriod(self,meta):
        #self.logger.info('\n\n: initialePeriod')
        tp2 = {}
        tp2['typePeriode'] = meta['typePeriode']
        tp2['orgUnit'] = []
        tmp = self.initialeOrgUnit(meta)
        if len(tmp) != 0:
            tp2['orgUnit'].append(tmp)
        return tp2
    
    def initialeOrgUnit(self,meta):
        #self.logger.info('\n\n: initialeOrgUnit')
        tp3 = {}
        tp3 = self.gestionOrgUnit(meta)
        
        if len(tp3) == 0:
            return tp3

        tp3['data'] = [] 
        tp3['data'].append(self.gestionData(meta))
        
        return tp3

    def gestionOrgUnit(self,meta):
        #self.logger.info('\n\n: gestionOrgUnit')
        tp = {}
        if meta['source_orgUnitName'] != "":
            tp['source_orgUnitName'] = meta['source_orgUnitName']
        if meta['source_orgUnitId'] != "":
            tp['source_orgUnitId'] = meta['source_orgUnitId']
        if meta['source_orgUnitGroupName'] != "":
            tp['source_ouGroupName'] = meta['source_orgUnitGroupName']
        if meta['source_orgUnitGroupId'] != "":
            tp['source_ouGroup'] = meta['source_orgUnitGroupId']
        if meta['source_orgUnitOfProgramName'] != "":
            tp['source_programName'] = meta['source_orgUnitOfProgramName']
        if meta['source_orgUnitOfProgramId'] != "":
            tp['source_programId'] = meta['source_orgUnitOfProgramId']
        if meta['source_orgUnitOfDataSetName'] != "":
            tp['source_dataSetName'] = meta['source_orgUnitOfDataSetName']
        if meta['source_orgUnitOfDataSetId'] != "":
            tp['source_dataSetId'] = meta['source_orgUnitOfDataSetId']

        if meta['destination_orgUnitName'] != "":
            tp['destination_orgUnitName'] = meta['destination_orgUnitName']
        if meta['destination_orgUnitId'] != "":
            tp['destination_orgUnitId'] = meta['destination_orgUnitId']
        if meta['destination_orgUnitGroupName'] != "":
            tp['destination_ouGroupName'] = meta['destination_orgUnitGroupName']
        if meta['destination_orgUnitGroupId'] != "":
            tp['destination_ouGroup'] = meta['destination_orgUnitGroupId']
        if meta['destination_orgUnitOfProgramName'] != "":
            tp['destination_programName'] = meta['destination_orgUnitOfProgramName']
        if meta['destination_orgUnitOfProgramId'] != "":
            tp['destination_programId'] = meta['destination_orgUnitOfProgramId']
        if meta['destination_orgUnitOfDataSetName'] != "":
            tp['destination_dataSetName'] = meta['destination_orgUnitOfDataSetName']
        if meta['destination_orgUnitOfDataSetId'] != "":
            tp['destination_dataSetId'] = meta['destination_orgUnitOfDataSetId']
        
        return tp

    def gestionData(self,meta):
        #self.logger.info('\n\n: gestionData')
        tp = {}
        if meta['description'] != "":
            tp['description'] = meta['description']
        if meta['source_indicatorName'] != "":
            tp['source_indicatorName'] = meta['source_indicatorName']
        if meta['source_indicatorId'] != "":
            tp['source_indicatorId'] = meta['source_indicatorId']
        if meta['source_dataElementName'] != "":
            tp['source_dataElementName'] = meta['source_dataElementName']
        if meta['source_dataElementId'] != "":
            tp['source_dataElementId'] = meta['source_dataElementId']
        if meta['source_categorieComboName'] != "":
            tp['source_categorieComboName'] = meta['source_categorieComboName']
        if meta['source_categorieComboId'] != "":
            tp['source_categorieComboId'] = meta['source_categorieComboId']

        if meta['source_categorieComboOptionName'] != "":
            tp['source_categorieComboOptionName'] = meta['source_categorieComboOptionName']
        if meta['source_categorieComboOptionId'] != "":
            tp['source_categorieComboOptionId'] = meta['source_categorieComboOptionId']
        if meta['source_filter'] != "":
            tp['source_filter'] = meta['source_filter']
        
        if meta['destination_dataElementName'] != "":
            tp['destination_dataElementName'] = meta['destination_dataElementName']
        if meta['destination_dataElementId'] != "":
            tp['destination_dataElementId'] = meta['destination_dataElementId']
        if meta['destination_categorieComboName'] != "":
            tp['destination_categorieComboName'] = meta['destination_categorieComboName']
        if meta['destination_categorieComboId'] != "":
            tp['destination_categorieComboId'] = meta['destination_categorieComboId']
        if meta['destination_categorieComboOptionName'] != "":
            tp['destination_categorieComboOptionName'] = meta['destination_categorieComboOptionName']
        if meta['destination_categorieComboOptionId'] != "":
            tp['destination_categorieComboOptionId'] = meta['destination_categorieComboOptionId']
        
        return tp


    def initialeCategorie(self,meta):
        #self.logger.info('\n\n: initialeCategorie')
        if meta['source_categorieComboId'] != "" and meta['source_categorieComboOptionId'] != "" and meta['destination_categorieComboId'] != "" and meta['destination_categorieComboOptionId'] != "":
            trouveCat = False
            for cat in self.allCategorie:
                if meta['source_categorieComboId'] in cat['source_categorieComboId']:
                    trouveCat = True
                    trouveOpt = False
                    for opt in cat['options']:
                        if meta['source_categorieComboOptionId'] in opt['source_categorieComboOptionId']:
                            trouveOpt = True
                    
                    if not trouveOpt:
                        tmp = {}
                        tmp['source_categorieComboOptionName'] = meta['source_categorieComboOptionName']
                        tmp['source_categorieComboOptionId'] = meta['source_categorieComboOptionId']
                        tmp['destination_categorieComboOptionName'] = meta['destination_categorieComboOptionName']
                        tmp['destination_categorieComboOptionId'] = meta['destination_categorieComboOptionId']
                        cat['options'].append(tmp)
            
            if not trouveCat:
                tmp = {}
                tmp['source_categorieComboName'] = meta['source_categorieComboName']
                tmp['source_categorieComboId'] = meta['source_categorieComboId']
                tmp['destination_categorieComboName'] = meta['destination_categorieComboName']
                tmp['destination_categorieComboId'] = meta['destination_categorieComboId']
                tmp['options'] = []
                tp = {}
                tp['source_categorieComboOptionName'] = meta['source_categorieComboOptionName']
                tp['source_categorieComboOptionId'] = meta['source_categorieComboOptionId']
                tp['destination_categorieComboOptionName'] = meta['destination_categorieComboOptionName']
                tp['destination_categorieComboOptionId'] = meta['destination_categorieComboOptionId']
                tmp['options'].append(tp)
                self.allCategorie.append(tmp)

    def appliquerCategorie(self,order):
        #self.logger.info('\n\n: appliquerCategorie')

        for ord in order:
            for mapping in ord['mapping']:
                self.getCategorieOptionDefault(mapping['destination'])
                
                for period in mapping['periode']:
                    for orgUnit in period['orgUnit']:
                        orgUnit['data'] = self.repartirCategorie(orgUnit['data'])

        return order
    
    def getCategorieOptionDefault(self,destination):
        #self.logger.info('\n\n: repartirCategorie')
        connexion = self.outgoing.plain_http[destination].conn
        params_source = {}
        params_source['paging'] = False
        params_source['url_path'] = "api/categoryCombos"
        response = connexion.get(self.cid, params=params_source)
        self.logger.info('\n\n\n: response = : {} \n\n'.format(response))
        allCategoryCombos = json.loads(response.text)
        defaultIds = []
        
        for category in allCategoryCombos['categoryCombos']:
            if 'displayName' in category:
                if category['displayName'] == 'default':
                    defaultIds.append(category['id'])
            if 'name' in category:
                if category['name'] == 'default':
                    defaultIds.append(category['id'])
                    
        for defau in defaultIds:
            params_source = {}
            params_source['url_path'] = "api/categoryCombos/" + defau
            response = connexion.get(self.cid, params=params_source)
            categoryCombo = json.loads(response.text)
            if 'isDefault' in categoryCombo:
                for option in categoryCombo['categoryOptionCombos']:
                    self.categoryOptionCombosDefault = option['id']
                    return


    def repartirCategorie(self,data):
        #self.logger.info('\n\n: repartirCategorie')
        newData = []
        for dat in data:
            if 'source_categorieComboId' in dat and not 'source_indicatorId' in dat:
                newData.extend(self.manageCategorie(dat))
            if not 'source_categorieComboId' in dat and 'destination_categorieComboOptionId' in dat:
                newData.append(dat)
            if not 'source_categorieComboId' in dat and not 'destination_categorieComboOptionId' in dat:
                dat['destination_categorieComboOptionId'] = self.categoryOptionCombosDefault
                newData.append(dat)
            
        return newData


    def manageCategorie(self,data):
        #self.logger.info('\n\n: manageCategorie')
        newData = []
        if not 'destination_categorieComboOptionId' in data:
            for cat in self.allCategorie:
                if data['source_categorieComboId'] == cat['source_categorieComboId']:
                    for opt in cat['options']:
                        _data = copy.deepcopy(data)
                        _data['source_categorieComboOptionName'] = opt['source_categorieComboOptionName']
                        _data['source_categorieComboOptionId'] = opt['source_categorieComboOptionId']
                        _data['destination_categorieComboOptionName'] = opt['destination_categorieComboOptionName']
                        _data['destination_categorieComboOptionId'] = opt['destination_categorieComboOptionId']
                        newData.append(_data)
        
        return newData


    def collecteData(self, listPeriod,listOrgUnit,listElement,listFilter,instance):
        self.logger.info('\n\n: Entrer dans la methode collecteData')
        query_param = {}
        requete = 'dimension=dx:' + listElement + '&dimension=ou:' + listOrgUnit + '&dimension=pe:' + listPeriod
        if listFilter is not None:
            requete = requete + listFilter
        query_param['url_path'] = self.path_analytics + requete
        query_param['ignoreLimit'] = 'TRUE'
        query_param['hideEmptyRows'] = 'TRUE'
        query_param['displayProperty'] = 'NAME'
        query_param['skipMeta'] = 'true'

        if instance == 'source':
            response = self.source_connexion.get(self.cid, params=query_param)
        else:
            response = self.destination_connexion.get(self.cid, params=query_param)
        # data_old = loads(response.text)
        self.logger.info('\n\n -get: response : {}'.format(response))
        self.logger.info('\n\n -get: response.text : {}'.format(response.text))
        self.enregisterLocal(response.text,'collecteData_logs')
        data = json.loads(response.text)
        #self.logger.info('\n\n L178-get: JSON data value file content : {}'.format(data))
        
        return data['rows']

    def addDataValue(self,data,metaData):
        #self.logger.info('\n\n L163-get: Entrer dans la methode addDataValue')
        
        for dat in data:
            for meta in metaData:
                if 'de_co' in meta:
                    if dat[0] == meta['source_de_co'] and dat[1] == meta['orgUnit'] and dat[2] == meta['period']:
                         meta['value'] =  self.controleInteger(dat[3])
                else:
                    if 'source_dataElementId' in meta:
                        if dat[0] == meta['source_dataElementId'] and dat[1] == meta['orgUnit'] and dat[2] == meta['period']:
                            meta['value'] = self.controleInteger(dat[3])
                    if 'source_indicatorId' in meta:
                        if dat[0] == meta['source_indicatorId'] and dat[1] == meta['orgUnit'] and dat[2] == meta['period']:
                            meta['value'] = self.controleInteger(dat[3])

        return metaData 
    

    def controleInteger(self, valeur):
        #self.logger.info('\n\n L163-get: Entrer dans la methode controleInteger')
        laValeur = None

        if '.' in valeur:
            _val = valeur.split('.')
            
            if int(_val[1]) == 0:
                laValeur = int(_val[0])
            else:
                laValeur = float(valeur)
        else:
            laValeur = int(valeur)
                                             
        return laValeur


    def prepareSendData(self,metaData):
        #self.logger.info('\n\n: prepareSendData')
        self.enregisterLocal(json.dumps(metaData, ensure_ascii=False),'metaData_prepareSendData')
        
        dataValide = []
        deleteDataOrgUnit = None
        deleteDataPeriod = None
        deleteDataDataElement = None
        for data in metaData:
            if 'value' in data:
                tmp = {}
                tmp['dataElement'] = data['destination_dataElementId']
                tmp['categoryOptionCombo'] = data['destination_categorieComboOptionId']
                tmp['period'] = data['period']
                tmp['orgUnit'] = data['orgUnit']
                tmp['value'] = data['value']
                dataValide.append(tmp)
            else:
                deleteDataOrgUnit = self.echeckElement(deleteDataOrgUnit, data['orgUnit'], None)
                deleteDataPeriod = self.echeckElement(deleteDataPeriod, data['period'], None)
                deleteDataDataElement = self.echeckElement(deleteDataDataElement, data['destination_dataElementId'], data['destination_categorieComboOptionId'])
        
        #self.logger.info('\n\n\n : dataValide = : {}'.format(dataValide))
        #self.logger.info('\n\n\n : deleteDataOrgUnit = : {}'.format(deleteDataOrgUnit))
        #self.logger.info('\n\n\n : deleteDataPeriod = : {}'.format(deleteDataPeriod))
        #self.logger.info('\n\n\n : deleteDataDataElement = : {}\n\n'.format(deleteDataDataElement))
        
        self.prepareDeleteData(deleteDataOrgUnit,deleteDataPeriod,deleteDataDataElement)
        self.send_data(dataValide,'save')
        

    def echeckElement(self,lists, element, combo):
        #self.logger.info('\n\n: echeckElement')
        de_co = element
        if combo is not None:
            if combo != self.categoryOptionCombosDefault:
                de_co = de_co + '.' + combo
        
        if lists is None:
            lists = de_co
        
        if not de_co in lists:
            lists = lists + ';' + de_co

        return lists

    def prepareDeleteData(self,orgUnits,periods,dataElements):
        #self.logger.info('\n\n: prepareDeleteData')
        dataRespone = self.collecteData(periods,orgUnits,dataElements,None,'destination')
        #self.logger.info('\n\n\n dataRespone = : {}'.format(dataRespone))
        deleteData = []
        #if dat[0] == meta['source_de_co'] and dat[1] == meta['orgUnit'] and dat[2] == meta['period']:

        for data in dataRespone:
            tmp = self.separeCategoCombo(data[0])
            tmp['orgUnit'] = data[1]
            tmp['period'] = data[2]
            tmp['value'] = data[3]
            deleteData.append(tmp)
        
        self.send_data(deleteData,'delete')
        

    def separeCategoCombo(self,de_co):
        self.logger.info('\n\n: separeCategoCombo')
        separe = de_co.split('.')
        tmp = {}
        tmp['dataElement'] = separe[0]
        if len(separe) != 1:
            tmp['categoryOptionCombo'] = separe[1]
        else:
            tmp['categoryOptionCombo'] = self.categoryOptionCombosDefault

        return tmp

    def send_data(self, data,action):
        self.logger.info('\n\n\n sendData: data = : {}\n\n'.format(data))
        if len(data) == 0:
            return
        
        data_payload = {}
        data_payload['dataValues'] = data
        _data = json.dumps(data_payload, ensure_ascii=False)
        query_param = {}
        query_param['url_path'] = self.path_dataValueSets
        
        if action == 'delete':
            query_param['importStrategy'] = 'DELETE'

        response = self.destination_connexion.post(self.cid, _data, params=query_param)

        #if action == 'save':
        #    response = self.destination_connexion.post(self.cid, data, params=query_param)
        #if action == 'delete':
        #    response = self.destination_connexion.delete(self.cid, data, params=query_param)
        
        self.logger.info('\n\n\n Send: response : {}'.format(response))
    
        self.logger.info('\n\n\n Send: response.text: {}'.format(response.text))
        self.enregisterLocal(response.text,'send_data_logs')
        #if len(response.text) != 0:
        #    self.enregisterLogLocal(response.text,'data_logs.txt')
        #    self.enregisterLogLocal('\n','data_logs.txt')


    """ def sendData(self, data):
        #self.logger.info('\n\n\n sendData: data = : {}'.format(data))
        donnees = 'de='+data['destination_dataElementId']+'&co='+data['destination_categorieComboOptionId']+'&ou='+data['orgUnit']+'&pe='+str(data['period'])+'&value='
        if 'value' in data:
            donnees = donnees +str(data['value'])

        query_param = {}
        query_param['url_path'] = self.path_dataValues+'?'+donnees
        
        # Invoke the resource providing all the information on input
        
        response = self.destination_connexion.post(self.cid, params=query_param)
        self.logger.info('\n\n\n Send: response : {}'.format(response))
    
        self.logger.info('\n\n\n Send: response.text: {}'.format(response.text))
        if len(response.text) != 0:
            self.enregisterLogLocal(response.text,'data_logs.txt')
            self.enregisterLogLocal('\n','data_logs.txt') """

        

    def lireLocal(self,filename,delimiteur):
        self.logger.info('\n\n: lireLocal')
        file_path = self.file_path_config +filename

        donnee = {}
        rows = []
        try:
            file = open(file_path, 'r', encoding='utf8')
            csvreader = csv.reader(file,delimiter=delimiteur)
            header = next(csvreader)
            for row in csvreader:
                rows.append(row)
            file.close()
            donnee['header'] = header
            donnee['rows'] = rows
            self.logger.info('\n\n:lireLocal- data : \n\n')
            self.logger.info('\n\n:lireLocal- data : {}\n\n'.format(donnee))
            #self.enregisterLocal(json.dumps(donnee, ensure_ascii=False),'csv_data.txt')
            return donnee
        except IOError:
            self.logger.info('\n\n: lireLocal- IOError : {}\n\n'.format(IOError))
            self.enregisterLocal(IOError,'csv_error.txt')
            return []   

    
    def controleTempsAttente(self,source):
        #self.logger.info('\n\n\n:controleTempsAttente \n\n\n')
        if source is None:
            return
        
        tempsActuel = datetime.now()
        ecart = tempsActuel - self.tempsAttente
        minutes = ecart.total_seconds() / 60
        #self.logger.info('\n\n\n: Nombre minutes = : {} \n\n'.format(minutes))
        if minutes > 3:
            self.logger.info('\n\n: Nombre minutes = : {} \n\n'.format(minutes))
            connexion = self.outgoing.plain_http[source].conn
            params_source = {}
            params_source['url_path'] = "api/organisationUnits/hazsksuasas"
            response = connexion.get(self.cid, params=params_source)
            self.logger.info('\n\n\n: response = : {} \n\n'.format(response))
            self.tempsAttente = tempsActuel

    def enregisterLocal(self, data,filename):
        file_path = self.file_path_log +filename
        #file_path = '/opt/zato/instanceFiles/'+filename
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        json_file = open(file_path, 'w', encoding='utf8')
        json_file.write(str(data))
        json_file.write('\n')
        json_file.close()
        #self.logger.info('\n\n:enregisterLocal- Enregistrement reussi .......\n\n')


    def enregisterLogLocal(self, data,filename):
        file_path = self.file_path_log +filename
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