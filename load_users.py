import pandas as pd
import json
import psycopg2
from functions import getEngine

#connect to the PostgreSQL on Heroku
engine = getEngine()
connStr= engine.connect().connection
mycursor = connStr.cursor()

#loading users
usersDF = pd.read_csv('users.csv')
usersDF = usersDF.fillna("")
queryOther = "INSERT INTO OtherSources (id, source, table_id, table_name) VALUES (%s,%s,%s,%s)"
queryUsers = "INSERT INTO Users (user_id,_fivetran_deleted,_fivetran_synced,confirmed_at, email, hashed_password, inserted_at,other_sources,phone_number, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

for index, row in usersDF.iterrows():
    #load json row.other_sources
    odID = ""
    data = json.loads(row.other_sources)
    if( len(data) > 0):
         osDF = pd.json_normalize(data)
         osID = str(osDF["id"][0])
         mycursor.execute(queryOther, (osID, str(osDF["source"][0]), str(osDF["table_id"][0]), str(osDF["table_name"][0])))
    
    mycursor.execute(queryUsers, (row.user_id,row._fivetran_deleted, row._fivetran_synced, row.confirmed_at, row.email, row.hashed_password, row.inserted_at, osID ,row.phone_number,  row.updated_at))
connStr.commit()
print("Users loaded")
mycursor.close()
connStr.close()
engine.dispose()