import pandas as pd
import json
import psycopg2
from functions import getEngine

#connect to the PostgreSQL on Heroku
engine = getEngine()
connStr= engine.connect().connection
mycursor = connStr.cursor()

mycursor.execute('CREATE TABLE push_tokens (push_token_id VARCHAR(100) PRIMARY KEY,_fivetran_deleted VARCHAR(155),\
                  _fivetran_synced VARCHAR(155), expo_push_token VARCHAR(155) ,inserted_at VARCHAR(155), \
                       updated_at VARCHAR(155) ,user_id VARCHAR(255),FOREIGN KEY (user_id) REFERENCES users(user_id))')    
#loading users
ptDF = pd.read_csv('push_tokens.csv')
ptDF = ptDF.fillna("")
queryTokens = "INSERT INTO push_tokens (push_token_id,_fivetran_deleted,_fivetran_synced,expo_push_token,inserted_at,updated_at,user_id) \
                          VALUES (%s,%s,%s,%s,%s,%s,%s)"


for index, row in ptDF.iterrows():
    mycursor.execute(queryTokens, (row.push_token_id,row._fivetran_deleted,row._fivetran_synced,
                                    row.expo_push_token,row.inserted_at, row.updated_at, row.user_id))

connStr.commit()
print("push tokens loaded")
mycursor.close()
connStr.close()
engine.dispose()