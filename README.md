# Zato_BF
Service Zato

1.	Envoie des données individuelles de rapidPro vers 
rapidproToDhis2Tracker.py permet le transfert des données de rapidpro vers dhis2 Mhealth.
Parametre :
Paramètre	Description
source	Le nom du système contenant les données.
destination	Le nom du système recevant les données.
mappingFile	Nom du fichier CSV (separteur point-virgule « ; »)
duration	Période de collecte des données. Inclure uniquement les valeurs de données qui sont mises à jour pendant la durée donnée. Le format est <valeur><unité de temps>, les unités sont :
-	"m" (minutes) : 15m
-	"h" (heures) : 5h
-	"d" (jours) : 2d
-	"w" (semaine) : 3w
-	"M" (mois) : 6M
-	"Y" (année) : 2Y
-	"c" (periode en cours) : 6Mc
NB : ne pas l’utiliser avec startDate et endDate

startDate	La date de début de collecte des données au format yyyy-MM-dd. Ne pas l’utiliser avec duration
endDate	La date de fin de collecte des données au format yyyy-MM-dd. Ne pas l’utiliser avec duration

Exemple de contenu du paramètre :
{'source':'RAPIDPRO','destination':'MHEALTH','mappingFile':'configFile.csv','duration':'5d'}
{'source':'RAPIDPRO','destination':'MHEALTH','mappingFile':'configFile.csv','startDate':'2023-08-20','endDate':'2023-02-05'}
