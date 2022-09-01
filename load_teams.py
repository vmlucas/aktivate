import pandas as pd
import json
import psycopg2
from functions import getEngine,createTables

#connect to the PostgreSQL on Heroku
engine = getEngine()
connStr= engine.connect().connection
mycursor = connStr.cursor()

#creating all tables
createTables()

#loading teams
teamsDF = pd.read_csv('teams.csv')
teamsDF = teamsDF.fillna("")
queryOther = "INSERT INTO OtherSources (id, source, table_id, table_name) VALUES (%s,%s,%s,%s)"
queryTeams = "INSERT INTO Teams (team_id,_fivetran_deleted,_fivetran_synced,activity_id,collective_name,gender,inserted_at,name,other_sources,season_id,team_level,updated_at,registration_code,school_activity_id,status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

for index, row in teamsDF.iterrows():
    #load json row.other_sources
    odID = ""
    data = json.loads(row.other_sources)
    if( len(data) > 0):
         osDF = pd.json_normalize(data)
         osID = str(osDF["id"][0])
         mycursor.execute(queryOther, (osID, str(osDF["source"][0]), str(osDF["table_id"][0]), str(osDF["table_name"][0])))
    
    mycursor.execute(queryTeams, (row.team_id,row._fivetran_deleted,row._fivetran_synced,row.activity_id,row.collective_name,row.gender,row.inserted_at,row.name,osID,row.season_id,row.team_level,row.updated_at,row.registration_code,row.school_activity_id,row.status))
connStr.commit()
print("Teams loaded")
mycursor.close()
connStr.close()
engine.dispose()