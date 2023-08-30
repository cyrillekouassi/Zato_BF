from __future__ import absolute_import, division, print_function, unicode_literals
from json import dumps, loads
from bunch import bunchify
from collections import OrderedDict
import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Zato
from zato.server.service import Service


class gestionPeriode(Service):
    class SimpleIO:
        #input_required = ('typePeriode','periodeSelectionne')
        input_optional = ('typePeriode','duration')
        output_required = 'listePeriode'

    def getNbrePeriode(self, laPeriodeSelectionne):
        num = ''
        for c in laPeriodeSelectionne:
            if c.isdigit():
                num = num + c
                #return int(num)
        #self.logger.info('\n\n getNbrePeriode: num = {}'.format(num))
        if num != '':
            return int(num)
        return 0


    def handle(self):
        typePeriode = self.request.input.typePeriode
        laPeriodeSelectionne = self.request.input.duration
        
        today = datetime.now()
        periode_debut = ''
        periode_fin = ''
        
        liste_periode = []
        nbre_periode = 0
        self.logger.info('\n\n periode: duration = {}'.format(laPeriodeSelectionne))
        nbre_periode = self.getNbrePeriode(laPeriodeSelectionne)
        self.logger.info('\n\n periode: nbre_periode = {}'.format(nbre_periode))

        # Gestion des periodes DHIS2
        if typePeriode != 'rapidPro':
            periodeDhis = {}
            dernierDate = None
            permerierDate = None
           
            if 'c' in laPeriodeSelectionne:
                dernierDate = today
            else:
                if 'd' in laPeriodeSelectionne:
                    dernierDate = today - timedelta(days = 1)

                if 'w' in laPeriodeSelectionne:
                    dernierDate = today - timedelta(weeks = 1)
                
                if 'M' in laPeriodeSelectionne:
                    leMois = today.strftime('%m')
                    lAnnee = today.strftime('%Y')
                    laDate = lAnnee + '-' + leMois + '-01'
                    dernierDate = datetime.strptime(laDate, '%Y-%m-%d')
                    dernierDate = dernierDate - relativedelta(days = 1)

                if 'Q' in laPeriodeSelectionne:
                    dernierDate = today - relativedelta(months = 3)

                if 'S' in laPeriodeSelectionne:
                    dernierDate = today - relativedelta(months = 6)
                                    
                if 'Y' in laPeriodeSelectionne:
                    la_periode = today.strftime('%Y') # format: yyyyMM
                    annee = int(la_periode)
                    annee -= 1
                    laAnnee = str(annee) + '-12-31'
                    dernierDate = datetime.strptime(laAnnee, '%Y-%m-%d')

            #nbre_periode += 1 ce_trimestre
            if 'd' in laPeriodeSelectionne:
                permerierDate = today - timedelta(days = nbre_periode)

            if 'w' in laPeriodeSelectionne:
                permerierDate = today - timedelta(weeks = nbre_periode)
            
            if 'M' in laPeriodeSelectionne:
                #permerierDate = today - timedelta(months = nbre_periode)
                periode = today - relativedelta(months = nbre_periode)
                leMois = periode.strftime('%m')
                lAnnee = periode.strftime('%Y')
                laDate = lAnnee + '-' + leMois + '-01'
                permerierDate = datetime.strptime(laDate, '%Y-%m-%d')
            
            if 'Q' in laPeriodeSelectionne:
                trimestre = nbre_periode * 3
                permerierDate = today - relativedelta(months = trimestre)
            
            if 'S' in laPeriodeSelectionne:
                semestre = nbre_periode * 6
                permerierDate = today - relativedelta(months = semestre)
            
            if 'Y' in laPeriodeSelectionne:
                la_periode = today.strftime('%Y') # format: yyyyMM
                annee = int(la_periode)
                annee -= nbre_periode
                laAnnee = str(annee) + '-01-01'
                permerierDate = datetime.strptime(laAnnee, '%Y-%m-%d')

            self.logger.info('\n\n : permerierDate = {} //// dernierDate = {}\n\n\n'.format(permerierDate,dernierDate))
          
            # Gestion des jours
            if 'd' in laPeriodeSelectionne:
                periodeDhis['QUOTIDIEN'] = self.jourDHIS(permerierDate, dernierDate)

            # Gestion des semaines
            if 'w' in laPeriodeSelectionne:
                periodeDhis['QUOTIDIEN'] = self.jourDHIS(permerierDate, dernierDate)
                periodeDhis['HEBDOMADAIRE'] = self.semaineDHIS(permerierDate, dernierDate)

            # Gestion des mois
            if 'M' in laPeriodeSelectionne:
                periodeDhis['QUOTIDIEN'] = self.jourDHIS(permerierDate, dernierDate)
                periodeDhis['HEBDOMADAIRE'] = self.semaineDHIS(permerierDate, dernierDate)
                periodeDhis['MENSUELLE'] = self.moisDHIS(permerierDate, dernierDate)

            # Gestion des trimestre
            if 'Q' in laPeriodeSelectionne:
                periodeDhis['QUOTIDIEN'] = self.jourDHIS(permerierDate, dernierDate)
                periodeDhis['HEBDOMADAIRE'] = self.semaineDHIS(permerierDate, dernierDate)
                periodeDhis['MENSUELLE'] = self.moisDHIS(permerierDate, dernierDate)
                periodeDhis['TRIMESTRIELLE'] = self.trimestreDHIS(permerierDate, dernierDate)
            
            # Gestion des semestre
            if 'S' in laPeriodeSelectionne:
                periodeDhis['QUOTIDIEN'] = self.jourDHIS(permerierDate, dernierDate)
                periodeDhis['HEBDOMADAIRE'] = self.semaineDHIS(permerierDate, dernierDate)
                periodeDhis['MENSUELLE'] = self.moisDHIS(permerierDate, dernierDate)
                periodeDhis['TRIMESTRIELLE'] = self.trimestreDHIS(permerierDate, dernierDate)
                periodeDhis['SEMESTRIELLE'] = self.semestreDHIS(permerierDate, dernierDate)

            # Gestion des annees
            if 'Y' in laPeriodeSelectionne:
                periodeDhis['QUOTIDIEN'] = self.jourDHIS(permerierDate, dernierDate)
                periodeDhis['HEBDOMADAIRE'] = self.semaineDHIS(permerierDate, dernierDate)
                periodeDhis['MENSUELLE'] = self.moisDHIS(permerierDate, dernierDate)
                periodeDhis['TRIMESTRIELLE'] = self.trimestreDHIS(permerierDate, dernierDate)
                periodeDhis['SEMESTRIELLE'] = self.semestreDHIS(permerierDate, dernierDate)
                periodeDhis['ANNUELLE'] = self.anneeDHIS(permerierDate, dernierDate)

            lesPeriode = {'liste_periode': periodeDhis}
    
        # Gestion des periodes rapidPro
        if typePeriode == 'rapidPro':

            # Gestion des minutes
            if 'm' in laPeriodeSelectionne:
                temps = today - timedelta(minutes = nbre_periode)
                fin = today - timedelta(minutes = 1)
                if 'c' in laPeriodeSelectionne:
                    fin = today
                periode_debut = temps.strftime('%Y-%m-%dT%H:%M')
                periode_fin = fin.strftime('%Y-%m-%dT%H:%M')
                liste_periode.append(periode_debut)
                liste_periode.append(periode_fin)

            # Gestion des heures
            if 'h' in laPeriodeSelectionne:
                temps = today - timedelta(hours = nbre_periode)
                fin = today - timedelta(hours = 1)
                if 'c' in laPeriodeSelectionne:
                    fin = today
                
                periode_debut = temps.strftime('%Y-%m-%dT%H:%M')
                periode_fin = fin.strftime('%Y-%m-%dT%H:%M')
                liste_periode.append(periode_debut)
                liste_periode.append(periode_fin)

            # Gestion des jours
            if 'd' in laPeriodeSelectionne:
                temps = today - timedelta(days = nbre_periode)
                fin = today - timedelta(days = 1)
                if 'c' in laPeriodeSelectionne:
                    fin = today
                
                periode_debut = temps.strftime('%Y-%m-%d')
                periode_fin = fin.strftime('%Y-%m-%d')
                liste_periode.append(periode_debut)
                liste_periode.append(periode_fin)

            # Gestion des semaines
            if 'w' in laPeriodeSelectionne:
                temps = today - timedelta(weeks = nbre_periode)
                periode_debut = temps.strftime('%Y-%m-%d')
                periode_fin = today.strftime('%Y-%m-%d')
                liste_periode.append(periode_debut)
                liste_periode.append(periode_fin)

            # Gestion des mois 
            if 'M' in laPeriodeSelectionne:
                debut = today + relativedelta(months=-nbre_periode)
                debut = debut + relativedelta(day=1)
                fin = today + relativedelta(day=1)

                if 'c' in laPeriodeSelectionne:
                    fin = fin + relativedelta(months=1)
                    #fin = fin + relativedelta(months=1)
                periode_debut = debut.strftime('%Y-%m-%d')
                periode_fin = fin.strftime('%Y-%m-%d')
                liste_periode.append(periode_debut)
                liste_periode.append(periode_fin)

            # Gestion des mois
            if 'Y' in laPeriodeSelectionne:
                la_periode = today.strftime('%Y') # format: yyyy
                annee = int(la_periode)
                derniere_annee = annee - 1
                periode_fin = str(derniere_annee) +'-12-31'
                if 'c' in laPeriodeSelectionne:
                    periode_fin = str(annee) +'-12-31'
                periode_fin = str(annee) +'-12-31'
                le_annee = annee - nbre_periode
                periode_debut = str(le_annee) +'-01-01'
                
                liste_periode.append(periode_debut)
                liste_periode.append(periode_fin)

            lesPeriode = {'liste_periode': liste_periode}
    
        
        
        self.response.payload = dumps(lesPeriode)

    def jourDHIS(self, datedebut, dateFin):
        #self.logger.info('\n\n : Entrer dans jourDHIS')
        debut = datedebut.strftime('%Y%m%d')
        fin = dateFin.strftime('%Y%m%d')
        tmp = []
        tmp.append(debut)
        nbre = 0
        while True:
            nbre += 1
            laDate = datedebut + relativedelta(days = nbre)
            periode = laDate.strftime('%Y%m%d')
            tmp.append(periode)
            if periode == fin:
                break
        
        return tmp
    
    def semaineDHIS(self, datedebut, dateFin):
        #self.logger.info('\n\n : Entrer dans semaineDHIS')
        debut = datedebut.strftime('%Y%m%d')
        debut = str(datedebut.isocalendar()[0])+'W'+str(datedebut.isocalendar()[1])
        fin = str(dateFin.isocalendar()[0])+'W'+str(dateFin.isocalendar()[1])
        tmp = []
        tmp.append(debut)
        nbre = 0
        while True:
            nbre += 1
            laDate = datedebut + relativedelta(weeks = nbre)
            periode = str(laDate.isocalendar()[0])+'W'+str(laDate.isocalendar()[1])
            tmp.append(periode)
            if periode == fin:
                break
        
        return tmp

    def moisDHIS(self, datedebut, dateFin):
        #self.logger.info('\n\n : Entrer dans moisDHIS')
        debut = datedebut.strftime('%Y%m')
        fin = dateFin.strftime('%Y%m')
        tmp = []
        tmp.append(debut)
        nbre = 0
        while True:
            nbre += 1
            laDate = datedebut + relativedelta(months = nbre)
            periode = laDate.strftime('%Y%m')
            tmp.append(periode)
            if periode == fin:
                break
        
        return tmp
        
    def trimestreDHIS(self, datedebut, dateFin):
        #self.logger.info('\n\n : Entrer dans trimestreDHIS')
        T1 = {'debut': 1, 'fin': 3, 'Trim': 1}
        T2 = {'debut': 4, 'fin': 6, 'Trim': 2}
        T3 = {'debut': 7, 'fin': 9, 'Trim': 3}
        T4 = {'debut': 10, 'fin': 12, 'Trim': 4}
        trimes = [T1,T2,T3,T4]
        fin_annee = dateFin.strftime('%Y')
        fin_mois = int(dateFin.strftime('%m'))

        fin = None
        for var in trimes:
            if fin_mois >= var['debut'] and fin_mois <= var['fin']:
                fin = fin_annee + 'Q' + str(var['Trim'])
                break

        debut = None
        debut_annee = datedebut.strftime('%Y')
        debut_mois = int(datedebut.strftime('%m'))
        for var in trimes:
            if debut_mois >= var['debut'] and debut_mois <= var['fin']:
                debut = debut_annee + 'Q' + str(var['Trim'])
                
        tmp = []
        tmp.append(debut)
        nbre = 0
        while True:
            nbre += 3
            laDate = datedebut + relativedelta(months = nbre)
            laDate_annee = laDate.strftime('%Y')
            laDate_mois = int(laDate.strftime('%m'))
            periode = None
            for var in trimes:
                if laDate_mois >= var['debut'] and laDate_mois <= var['fin']:
                    periode = laDate_annee + 'Q' + str(var['Trim'])
                    break
            tmp.append(periode)
            if periode == fin:
                break
            
        return tmp
    
    def semestreDHIS(self, datedebut, dateFin):
        #self.logger.info('\n\n : Entrer dans semestreDHIS')
        S1 = {'debut': 1, 'fin': 6, 'Trim': 1}
        S2 = {'debut': 2, 'fin': 12, 'Trim': 2}
        trimes = [S1,S2]
        fin_annee = dateFin.strftime('%Y')
        fin_mois = int(dateFin.strftime('%m'))

        fin = None
        for var in trimes:
            if fin_mois >= var['debut'] and fin_mois <= var['fin']:
                fin = fin_annee + 'S' + str(var['Trim'])
                break
        debut = None
        debut_annee = datedebut.strftime('%Y')
        debut_mois = int(datedebut.strftime('%m'))
        for var in trimes:
            if debut_mois >= var['debut'] and debut_mois <= var['fin']:
                debut = debut_annee + 'S' + str(var['Trim'])

        tmp = []
        tmp.append(debut)
        nbre = 0
        while True:
            nbre += 6
            laDate = datedebut + relativedelta(months = nbre)
            laDate_annee = laDate.strftime('%Y')
            laDate_mois = int(laDate.strftime('%m'))
            periode = None
            for var in trimes:
                if laDate_mois >= var['debut'] and laDate_mois <= var['fin']:
                    periode = laDate_annee + 'S' + str(var['Trim'])
                    break
            tmp.append(periode)
            if periode == fin:
                break
        return tmp

    def anneeDHIS(self, datedebut, dateFin):
        #self.logger.info('\n\n : Entrer dans anneeDHIS')
        debut = datedebut.strftime('%Y')
        fin = int(dateFin.strftime('%Y'))
        tmp = []
        tmp.append(debut)
        datedeb = int(debut)
        while True:            
            datedeb += 1
            tmp.append(str(datedeb))
            if datedeb == fin:
                break
        return tmp