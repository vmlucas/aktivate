import pandas as pd
from pandas import DataFrame 
import json
import psycopg2
from sqlalchemy import create_engine
import numpy
from psycopg2.extensions import register_adapter, AsIs
import datetime 
import os
from dotenv import load_dotenv

load_dotenv('../Auth-keys/.env')

db_user = os.getenv('db_user_postgres')
db_pass = os.getenv('db_pass_postgres')
db_name = os.getenv('db_name_postgres')

def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)

#connect to the PostgreSQL on Heroku
def getEngine():    
    engine = create_engine('postgresql+psycopg2://'+db_user+':'+db_pass+'@localhost/'+db_name)
    return engine

def createTables():
    engine = getEngine()
    connStr= engine.connect().connection
    mycursor = connStr.cursor()
    
    #create OtherSources table
    mycursor.execute('CREATE TABLE otherSources (id VARCHAR(255) PRIMARY KEY, source VARCHAR(155), table_id VARCHAR(155), table_name VARCHAR(255))')
    
    #create Teams table
    mycursor.execute('CREATE TABLE teams (team_id int primary key,team_name VARCHAR(255),_fivetran_deleted VARCHAR(155),_fivetran_synced VARCHAR(155),activity_id VARCHAR(255), \
                                            collective_name VARCHAR(155), gender VARCHAR(50),inserted_at VARCHAR(155),name VARCHAR(200),other_sources VARCHAR(255),season_id VARCHAR(255) ,\
                                            team_level VARCHAR(100), updated_at VARCHAR(155),registration_code VARCHAR(155),school_activity_id VARCHAR(255), status VARCHAR(100), \
                                            FOREIGN KEY (other_sources) REFERENCES OtherSources(id))')

    #create user table    
    mycursor.execute('CREATE TABLE users (user_id varchar(255) PRIMARY KEY,created_at varchar(155) NULL,created_by int4 NULL,"name" varchar(255) NULL,banned bool NULL,channel_unread_count int4 NULL,\
                                                hashed_password VARCHAR(155),_fivetran_deleted VARCHAR(155), _fivetran_synced VARCHAR(155),other_sources VARCHAR(255),\
                                            	channel_last_read_at varchar(155) NULL,online bool NULL,"role" varchar(50) NULL,team_id int4 NULL,total_unread_count int4 NULL,\
                                                unread_channels int4 NULL,unread_count int4 NULL,confirmed_at varchar(255) NULL,email varchar(155) NULL,inserted_at varchar(155) NULL,\
                                                phone_number varchar(50) NULL,last_active varchar(155) NULL,updated_at varchar(155) NULL,teams varchar(200) NULL,deleted_at varchar(155) NULL,\
	                                            aktivate_teams varchar(200) NULL,dashboard_user varchar(200) NULL,first_name varchar(200) NULL,"language" varchar(200) NULL,last_name varchar(200) NULL,\
	                                           staff_user varchar(200), FOREIGN KEY (other_sources) REFERENCES OtherSources(id), FOREIGN KEY (team_id) REFERENCES Teams(team_id))')

    mycursor.execute('CREATE TABLE users_tmp (user_id varchar(255) NULL,created_at varchar(155) NULL,created_by int4 NULL,"name" varchar(255) NULL,banned bool NULL,channel_unread_count int4 NULL,\
                                                hashed_password VARCHAR(155),_fivetran_deleted VARCHAR(155), _fivetran_synced VARCHAR(155),other_sources VARCHAR(255),\
                                            	channel_last_read_at varchar(155) NULL,online bool NULL,"role" varchar(50) NULL,team_id int4 NULL,total_unread_count int4 NULL,\
                                                unread_channels int4 NULL,unread_count int4 NULL,confirmed_at varchar(255) NULL,email varchar(155) NULL,inserted_at varchar(155) NULL,\
                                                phone_number varchar(50) NULL,last_active varchar(155) NULL,updated_at varchar(155) NULL,teams varchar(200) NULL,deleted_at varchar(155) NULL,\
	                                            aktivate_teams varchar(200) NULL,dashboard_user varchar(200) NULL,first_name varchar(200) NULL,"language" varchar(200) NULL,last_name varchar(200) NULL,\
	                                           staff_user varchar(200))')


    #create aktivate_teams table    
    mycursor.execute('CREATE TABLE IF NOT EXISTS aktivate_teams ( guardian_of VARCHAR(255) , roles VARCHAR(150), team VARCHAR(150), user_id VARCHAR(255), FOREIGN KEY (user_id) REFERENCES users(user_id))' )
    mycursor.execute('CREATE TABLE IF NOT EXISTS aktivate_teams_tmp ( guardian_of VARCHAR(255) , roles VARCHAR(150), team VARCHAR(150), user_id VARCHAR(255))' )

    #create Callbacks table
    mycursor.execute('CREATE TABLE IF NOT EXISTS callbacks (callbacksid int4 PRIMARY KEY,cid varchar(155) NULL,\
                                                                 created_at varchar(155) NULL,team_id int4 NULL,"type" varchar(100) NULL,watcher_count int4 NULL,user_id varchar(255) NULL,\
	                                                             team varchar(150) NULL,message_id varchar(255) NULL,delete_conversation varchar(200) NULL,delete_conversation_channels varchar(200) NULL,\
	                                                             delete_messages varchar(200) NULL,delete_user varchar(200) NULL,hard_delete varchar(200) NULL,mark_messages_deleted varchar(200) NULL,\
                                                                 total_flags varchar(200) NULL,FOREIGN KEY (team_id) REFERENCES teams(team_id) )')
    #mycursor.execute('CREATE TABLE IF NOT EXISTS callbacks_tmp (callbacksid int4 NOT NULL,cid varchar(155) NULL,\
    #                                                             created_at varchar(155) NULL,team_id int4 NULL,"type" varchar(100) NULL,watcher_count int4 NULL,user_id varchar(255) NULL,\
	#                                                             team varchar(150) NULL,message_id varchar(255) NULL,delete_conversation varchar(200) NULL,delete_conversation_channels varchar(200) NULL,\
	#                                                             delete_messages varchar(200) NULL,delete_user varchar(200) NULL,hard_delete varchar(200) NULL,mark_messages_deleted varchar(200) NULL,\
    #                                                             total_flags varchar(200) NULL )')
    
    #create channel table
    mycursor.execute('CREATE TABLE channel (channel_id text,channel_type text NULL,callbacksid int8 NULL,cid varchar(155) NULL,created_at varchar(255) NULL,disabled varchar(100) NULL,\
                             frozen varchar(100) NULL,id varchar(155) NULL,member_count int4 NULL,team varchar(100) NULL,"type" varchar(100) NULL,updated_at varchar(155) NULL,\
                             deleted_at varchar(155) NULL,truncated_at varchar(155) NULL,FOREIGN KEY (callbacksid) REFERENCES callbacks(callbacksid))')
    mycursor.execute('CREATE TABLE channel_tmp (channel_id text NULL,channel_type text NULL,callbacksid int8 NULL,cid varchar(155) NULL,created_at varchar(255) NULL,disabled varchar(100) NULL,\
                             frozen varchar(100) NULL,id varchar(155) NULL,member_count int4 NULL,team varchar(100) NULL,"type" varchar(100) NULL,updated_at varchar(155) NULL,\
                             deleted_at varchar(155) NULL,truncated_at varchar(155) NULL)')

    #create message table    
    mycursor.execute('CREATE TABLE message (message_id text NULL, attachments text NULL,	cid text NULL,created_at text NULL,html text NULL,id text NULL,	mentioned_users text NULL,\
	                                                    own_reactions text NULL,pin_expires text NULL,pinned bool NULL,pinned_at text NULL,pinned_by text NULL,reply_count int8 NULL,\
                                                        shadowed bool NULL,silent bool NULL,"text" text NULL,"type" text NULL,updated_at text NULL,callbacksid int4 NULL,user_id text NULL,\
	                                                    "reaction_counts.love" varchar(200) NULL,"reaction_scores.love" varchar(200) NULL,"reaction_counts.like" varchar(200) NULL,\
                                                        "reaction_scores.like" varchar(200) NULL,args varchar(200) NULL,command varchar(200) NULL,"command_info.name" varchar(200) NULL,\
                                                        deleted_at varchar(255) NULL,"dateSeparator" varchar(255) NULL,"groupStyles" varchar(255) NULL,"readBy" varchar(255) NULL,\
                                                        status varchar(100) NULL,"reaction_counts.wow" varchar(100) NULL,"reaction_scores.wow" varchar(100) NULL,\
                                                        FOREIGN KEY (callbacksid) REFERENCES callbacks(callbacksid), FOREIGN KEY (user_id) REFERENCES users(user_id))' )
    mycursor.execute('CREATE TABLE message_tmp (message_id text NULL,attachments text NULL,cid text NULL,created_at text NULL,html text NULL,id text NULL,mentioned_users text NULL,\
	                                                    own_reactions text NULL,pin_expires text NULL,pinned bool NULL,pinned_at text NULL,pinned_by text NULL,reply_count int8 NULL,\
                                                        shadowed bool NULL,silent bool NULL,"text" text NULL,"type" text NULL,updated_at text NULL,callbacksid int4 NULL,user_id text NULL,\
	                                                    "reaction_counts.love" varchar(200) NULL,"reaction_scores.love" varchar(200) NULL,"reaction_counts.like" varchar(200) NULL,\
                                                        "reaction_scores.like" varchar(200) NULL,args varchar(200) NULL,command varchar(200) NULL,"command_info.name" varchar(200) NULL,\
                                                        deleted_at varchar(255) NULL,"dateSeparator" varchar(255) NULL,"groupStyles" varchar(255) NULL,"readBy" varchar(255) NULL,\
                                                        status varchar(100) NULL,"reaction_counts.wow" varchar(100) NULL,"reaction_scores.wow" varchar(100) NULL)' )

   
    #create details table
    mycursor.execute('CREATE TABLE details ("action" text NULL,	blocked_word text NULL,	blocklist_name text NULL,created_at text NULL,message_id text NULL,	moderated_by text NULL,\
                               updated_at text NULL,user_bad_karma bool NULL,user_karma float8 NULL,callbacksid int4 NULL,"ai_moderation_response.explicit" varchar(200) NULL,\
                               "ai_moderation_response.spam" varchar(200) NULL,"ai_moderation_response.toxic" varchar(200) NULL,"moderation_thresholds.explicit.block" varchar(200) NULL,\
                               "moderation_thresholds.explicit.flag" varchar(200) NULL,"moderation_thresholds.spam.block" varchar(200) NULL,"moderation_thresholds.spam.flag" varchar(200) NULL,\
	                           "moderation_thresholds.toxic.block" varchar(200) NULL,"moderation_thresholds.toxic.flag" varchar(200) NULL,\
                                FOREIGN KEY (callbacksid) REFERENCES callbacks(callbacksid))')
    mycursor.execute('CREATE TABLE details_tmp ("action" text NULL,	blocked_word text NULL,	blocklist_name text NULL,created_at text NULL,message_id text NULL,	moderated_by text NULL,\
                               updated_at text NULL,user_bad_karma bool NULL,user_karma float8 NULL,callbacksid int8 NULL,"ai_moderation_response.explicit" varchar(200) NULL,\
                               "ai_moderation_response.spam" varchar(200) NULL,"ai_moderation_response.toxic" varchar(200) NULL,"moderation_thresholds.explicit.block" varchar(200) NULL,\
                               "moderation_thresholds.explicit.flag" varchar(200) NULL,"moderation_thresholds.spam.block" varchar(200) NULL,"moderation_thresholds.spam.flag" varchar(200) NULL,\
	                           "moderation_thresholds.toxic.block" varchar(200) NULL,"moderation_thresholds.toxic.flag" varchar(200) NULL)')
    

    #create reactions table
    mycursor.execute('CREATE TABLE reactions (created_at text NULL,	message_id text NULL,score int8 NULL,"type" text NULL,updated_at text NULL,callbacksid int8 NULL,\
                               FOREIGN KEY (callbacksid) REFERENCES callbacks(callbacksid))')
    mycursor.execute('CREATE TABLE reactions_tmp (created_at text NULL,	message_id text NULL,score int8 NULL,"type" text NULL,updated_at text NULL,callbacksid int8 NULL)')


    #create image_labels table
    mycursor.execute('CREATE TABLE image_labels ("image_labels.https://us-east.stream-io-cdn.com/1173491/images/4" text NULL,\
                                        "image_labels.https://us-east.stream-io-cdn.com/1173491/images/e" text NULL,message_id text)')
    

    #create config table
    mycursor.execute('CREATE TABLE config (id text PRIMARY KEY,automod text NULL,automod_behavior text NULL,blocklist text NULL,blocklist_behavior text NULL,connect_events bool NULL,created_at text NULL,\
	                                            custom_events bool NULL,max_message_length int8 NULL,message_retention text NULL,mutes bool NULL,"name" text NULL,push_notifications bool NULL,\
	                                            quotes bool NULL,reactions bool NULL,read_events bool NULL,reminders bool NULL,	replies bool NULL,"search" bool NULL,typing_events bool NULL,\
	                                            updated_at text NULL,uploads bool NULL,url_enrichment bool NULL,channelid text NULL)')
    mycursor.execute('CREATE TABLE config_tmp (automod text NULL,automod_behavior text NULL,blocklist text NULL,blocklist_behavior text NULL,connect_events bool NULL,created_at text NULL,\
	                                            custom_events bool NULL,max_message_length int8 NULL,message_retention text NULL,mutes bool NULL,"name" text NULL,push_notifications bool NULL,\
	                                            quotes bool NULL,reactions bool NULL,read_events bool NULL,reminders bool NULL,	replies bool NULL,"search" bool NULL,typing_events bool NULL,\
	                                            updated_at text NULL,uploads bool NULL,url_enrichment bool NULL,channelid text NULL,id text NULL)')

    #create commands table    
    mycursor.execute('CREATE TABLE commands (args text NULL,description text NULL,"name" text NULL,"set" text NULL,configid text,\
                                               FOREIGN KEY (configid) REFERENCES config(id) )')
    mycursor.execute('CREATE TABLE commands_tmp (args text NULL,description text NULL,"name" text NULL,"set" text NULL,configid text NULL)')

    #create automod_thresholds table    
    mycursor.execute('CREATE TABLE automod_thresholds("explicit.block" float8 NULL,"explicit.flag" float8 NULL,"spam.block" float8 NULL,"spam.flag" float8 NULL,"toxic.block" float8 NULL,\
	                                                       "toxic.flag" float8 NULL,configid text NULL,FOREIGN KEY (configid) REFERENCES config(id))')
    mycursor.execute('CREATE TABLE automod_thresholds_tmp ("explicit.block" float8 NULL,"explicit.flag" float8 NULL,"spam.block" float8 NULL,"spam.flag" float8 NULL,"toxic.block" float8 NULL,\
	                                                       "toxic.flag" float8 NULL,configid text NULL)')

    #create attachments table    
    mycursor.execute('CREATE TABLE IF NOT EXISTS attachments(image_url text NULL,type text NULL,message_id text NULL,thumb_url varchar(255) NULL,title varchar(255) NULL, \
                          title_link varchar(255) NULL,"giphy.fixed_height.frames" varchar(255) NULL,"giphy.fixed_height.height" varchar(255) NULL,\
	                      "giphy.fixed_height.size" varchar(255) NULL,"giphy.fixed_height.url" varchar(255) NULL,"giphy.fixed_height.width" varchar(255) NULL, \
	                      "giphy.fixed_height_downsampled.frames" varchar(255) NULL, "giphy.fixed_height_downsampled.height" varchar(255) NULL,	"giphy.fixed_height_downsampled.size" varchar(255) NULL,\
	                      "giphy.fixed_height_downsampled.url" varchar(255) NULL,"giphy.fixed_height_downsampled.width" varchar(255) NULL, "giphy.fixed_height_still.frames" varchar(255) NULL,\
	                      "giphy.fixed_height_still.height" varchar(255) NULL,"giphy.fixed_height_still.size" varchar(255) NULL,"giphy.fixed_height_still.url" varchar(255) NULL,\
	                      "giphy.fixed_height_still.width" varchar(255) NULL,"giphy.fixed_width.frames" varchar(255) NULL,"giphy.fixed_width.height" varchar(255) NULL,\
	                      "giphy.fixed_width.size" varchar(255) NULL,"giphy.fixed_width.url" varchar(255) NULL,"giphy.fixed_width.width" varchar(255) NULL,"giphy.fixed_width_downsampled.frames" varchar(255) NULL,\
	                      "giphy.fixed_width_downsampled.height" varchar(255) NULL,"giphy.fixed_width_downsampled.size" varchar(255) NULL,"giphy.fixed_width_downsampled.url" varchar(255) NULL,\
	                      "giphy.fixed_width_downsampled.width" varchar(255) NULL,"giphy.fixed_width_still.frames" varchar(255) NULL,"giphy.fixed_width_still.height" varchar(255) NULL,\
	                      "giphy.fixed_width_still.size" varchar(255) NULL,"giphy.fixed_width_still.url" varchar(255) NULL,"giphy.fixed_width_still.width" varchar(255) NULL,"giphy.original.frames" varchar(255) NULL,\
	                      "giphy.original.height" varchar(255) NULL,"giphy.original.size" varchar(255) NULL,"giphy.original.url" varchar(255) NULL,"giphy.original.width" varchar(255) NULL,\
	                       asset_url varchar(3000) NULL,file_size int8 NULL,mime_type varchar(255) NULL )')
    
    #create members table
    mycursor.execute('CREATE TABLE members (banned bool NULL,channel_role text NULL,created_at text NULL,"role" text NULL,shadow_banned bool NULL,updated_at text NULL,user_id text NULL,\
                                                is_moderator bool NULL,	callbacksid int4 NULL,channelid varchar(255) NULL,\
                                                FOREIGN KEY (callbacksID) REFERENCES Callbacks(callbacksID),FOREIGN KEY (user_id) REFERENCES users(user_id))')
    mycursor.execute('CREATE TABLE members_tmp (banned bool NULL,channel_role text NULL,created_at text NULL,"role" text NULL,shadow_banned bool NULL,updated_at text NULL,user_id text NULL,\
                                                is_moderator bool NULL,	callbacksid int4 NULL,channelid varchar(255) NULL)')


    #create participant table
    mycursor.execute('CREATE TABLE participant ( participant_person_id int, user_id VARCHAR(255), participant_name VARCHAR(255), team_id int, team_name text NULL,\
                                                    FOREIGN KEY (user_id) REFERENCES Users(user_id), FOREIGN KEY (team_id) REFERENCES Teams(team_id)) ') 
    mycursor.execute('CREATE TABLE participant_tmp ( participant_person_id int , user_id VARCHAR(255), participant_name VARCHAR(255), team_id int, team_name text NULL)') 
    
    
    connStr.commit()
    print("Tables created")
    mycursor.close()
    connStr.close()
    engine.dispose()

def loadCallback(df):
    engine = getEngine()
    conn = engine.connect()
    """
    team =df['team'][0]
    team = team[team.find("id-")+3:]  
    teamID = int(team)
    df['team_id'] = teamID
    df = df.drop('team')
    """
    #print(df)
    if( df.columns.str.startswith('details').any() ):
        detailsDF = df.loc[:, df.columns.str.startswith('details')].copy()
        detailsDF['callbacksid'] = df['callbacksid']
        df = df.loc[:, ~df.columns.str.startswith('details')]
        detailsDF.columns = detailsDF.columns.str.removeprefix('details.')
        detailsDF.to_sql(name='details_tmp',con=engine,if_exists='append',index=False)
        print(" callbacks details loaded")
    
    if( df.columns.str.startswith('member').any() ):
        memberDF = df.loc[:, df.columns.str.startswith('details')].copy()
        memberDF['callbacksid'] = df['callbacksid']
        df = df.loc[:, ~df.columns.str.startswith('member')]
        memberDF.columns = memberDF.columns.str.removeprefix('member.')
        memberDF.to_sql(name='members_tmp',con=engine,if_exists='append',index=False)
        print("callbacks member loaded")

    df.to_sql(name='callbacks',con=engine,if_exists='append',index=False)
    print("Callback loaded")
    conn.close()
    engine.dispose()

def loadChannel(df,callbackid):
    engine = getEngine()
    conn = engine.connect()
    print('channel')
    df.columns = df.columns.str.removeprefix('channel.')                        
    if( df.columns.str.startswith('config').any() ):
        configDF = df.loc[:, df.columns.str.startswith('config')].copy()
        configDF['channelid'] = df['id']
        loadConfig(configDF)   

    if( df.columns.str.startswith('created_by').any() ):
        userDF = df.loc[:, df.columns.str.startswith('created_by')].copy()
        if( userDF.columns.str.startswith('created_by.guardian_of').any() ):
            partDF = pd.json_normalize(userDF['created_by.guardian_of'])
            userID = userDF['created_by.id']
            print('channel Participant')
            loadParticipants(partDF,userID) 
        userDF = userDF.loc[:, ~userDF.columns.str.startswith('created_by.guardian_of')]
        print('channel user')
        userDF.columns = userDF.columns.str.removeprefix('created_by.') 
        userDF = userDF.rename(columns={'id': 'user_id'})
        loadUser(userDF)
    
    if( df.columns.str.startswith('members').any() ):
       membersDF = pd.json_normalize(df['members'])
       print('channel members')
       loadMembers(membersDF,callbackid,df['id'])    

    df = df.loc[:, ~df.columns.str.startswith('config')]
    df = df.loc[:, ~df.columns.str.startswith('created_by')]   
    df = df.loc[:, ~df.columns.str.startswith('members')]
    df.to_sql(name='channel_tmp',con=engine,if_exists='append',index=False)
    conn.close()
    engine.dispose()

def loadConfig(df):
    engine = getEngine()
    conn = engine.connect()
    df.columns = df.columns.str.removeprefix('config.') 
    df['id'] = str(datetime.datetime.now())                       
    if( df.columns.str.startswith('automod_thresholds').any() ):
               atDF = df.loc[:, df.columns.str.startswith('automod_thresholds')].copy()
               print('config AT')
               atDF.columns = atDF.columns.str.removeprefix('automod_thresholds.') 
               atDF['configid'] = df['id']
               atDF.to_sql(name='automod_thresholds_tmp',con=engine,if_exists='append',index=False)
               print("config automod_thresholds loaded")

    if( df.columns.str.startswith('commands').any() ):
        commsDF = pd.json_normalize(df['commands'])
        df = df.loc[:, ~df.columns.str.startswith('commands')]
        for index,row in commsDF.iterrows():
            commDF = pd.json_normalize(row)
            commDF = commDF
            commDF['configid'] = df['id']
            commDF.to_sql(name='commands_tmp',con=engine,if_exists='append',index=False) 
            print("config command loaded")

    df = df.loc[:, ~df.columns.str.startswith('automod_thresholds')]  
    df.to_sql(name='config_tmp',con=engine,if_exists='append',index=False)
    print("config loaded")
    conn.close()
    engine.dispose()


def loadMembers(df,callbacksid,channelid):
    engine = getEngine()
    conn = engine.connect()

    for index,row in df.iterrows():
        memberDF = pd.json_normalize(row)
        memberDF = memberDF
        memberDF['callbacksid'] = callbacksid
        memberDF['channelid'] = channelid

        #load members user
        if( 'user' in memberDF.columns ):
           userDF = memberDF.loc[:, memberDF.columns.str.startswith('user.')].copy()
           if( 'user.guardian_of' in userDF.columns ):
               partDF = pd.json_normalize(userDF['user.guardian_of'])
               userID = userDF['user.id']
               print('member user Participant')
               loadParticipants(partDF,userID) 
           userDF = userDF.loc[:, ~userDF.columns.str.startswith('user.guardian_of')]
           print('Member user')
           userDF.columns = userDF.columns.str.removeprefix('user.') 
           userDF = userDF.rename(columns={'id': 'user_id'})
           #print(userDF)
           loadUser(userDF) 
        memberDF = memberDF.loc[:, ~memberDF.columns.str.startswith('user.')] 
        print('Member')
        #print(memberDF)
        memberDF.to_sql(name='members_tmp',con=engine,if_exists='append',index=False)
        print("member loaded")
    conn.close()
    engine.dispose()

def loadUser(df):
    engine = getEngine()
    conn = engine.connect()
    #Aktivate teams
    if( df.columns.str.startswith('aktivate_teams').any() ):
        aktsDF = pd.json_normalize(df['aktivate_teams'])
        df = df.loc[:, ~df.columns.str.startswith('aktivate_teams')]
        for index,row in aktsDF.iterrows():
            aktDF = pd.json_normalize(row)
            aktDF = aktDF
            aktDF['user_id'] = df['user_id']
            aktDF.to_sql(name='aktivate_teams_tmp',con=engine,if_exists='append',index=False) 
            print("user Aktivate teams loaded")

    df.to_sql(name='users_tmp',con=engine,if_exists='append',index=False)
    print("User loaded")
    conn.close()
    engine.dispose()

def loadMessage(df):
    engine = getEngine()
    conn = engine.connect()
    if( df.columns.str.startswith('attachments').any() ):
        attachsDF = pd.json_normalize(df['attachments'])
        df = df.loc[:, ~df.columns.str.startswith('attachments')]
        for index,row in attachsDF.iterrows():
            attachDF = pd.json_normalize(row)
            attachDF = attachDF 
            if not attachDF.empty:
                attachDF['message_id'] = df.message_id[0]
                attachDF.to_sql(name='attachments',con=engine,if_exists='append',index=False)
     
    if( df.columns.str.startswith('image_labels').any() ):
       imgDF = df.loc[:, df.columns.str.startswith('image_labels')].copy()
       df = df.loc[:, ~df.columns.str.startswith('image_labels')]
       if not imgDF.empty:
         imgDF['message_id'] = df.message_id[0]
         imgDF.to_sql(name='image_labels',con=engine,if_exists='append',index=False)   

    mentioned_users =""
    if( df.columns.str.startswith('mentioned_users').any() ):
        usersDF = pd.json_normalize(df['mentioned_users'])
        for index,row in usersDF.iterrows():
            userDF = pd.json_normalize(row)
            userDF = userDF 
            if not userDF.empty:
              mentioned_users = mentioned_users+userDF['name']+','
              
              if( userDF.columns.str.startswith('guardian_of').any() ):
                   partDF = pd.json_normalize(userDF['guardian_of'].notnull())
                   userID = userDF['id']
                   print('mentioned user Participant')
                   loadParticipants(partDF,userID) 

              userDF = userDF.loc[:, ~userDF.columns.str.startswith('guardian_of')]
              print('mentioned user')
              userDF = userDF.rename(columns={'id': 'user_id'})
              loadUser(userDF)    
              df['mentioned_users'] = mentioned_users 
    
    
    if( df.columns.str.startswith('latest_reactions').any() ):
        reactsDF = pd.json_normalize(df['latest_reactions'])
        df = df.loc[:, ~df.columns.str.startswith('latest_reactions')]
        for index,row in reactsDF.iterrows():
            reactDF = pd.json_normalize(row)
            reactDF = reactDF 

            if( reactDF.columns.str.startswith('user').any() ):
               userDF = reactDF.loc[:, reactDF.columns.str.startswith('user')].copy()
               reactDF = reactDF.loc[:, ~reactDF.columns.str.startswith('user')]
               if( userDF.columns.str.startswith('user.guardian_of').any() ):
                   partDF = pd.json_normalize(userDF['user.guardian_of'].notnull())
                   userID = userDF['user.id']
                   print('message latest_reactions Participant')
                   loadParticipants(partDF,userID) 

               userDF = userDF.loc[:, ~userDF.columns.str.startswith('user.guardian_of')]
               print('message latest_reactions user')
               userDF.columns = userDF.columns.str.removeprefix('user.') 
               userDF = userDF.rename(columns={'id': 'user_id'})
               loadUser(userDF)                
            reactDF.to_sql(name='reactions_tmp',con=engine,if_exists='append',index=False)   

    df.to_sql(name='message_tmp',con=engine,if_exists='append',index=False)
    print("Message loaded")
    conn.close()
    engine.dispose()

def loadReaction(df):
    engine = getEngine()
    conn = engine.connect()
    df.columns = df.columns.str.removeprefix('reaction.')                        
    if( df.columns.str.startswith('user').any() ):
               userDF = df.loc[:, df.columns.str.startswith('user')].copy()
               if( userDF.columns.str.startswith('user.guardian_of').any() ):
                   partDF = pd.json_normalize(userDF['user.guardian_of'])
                   userID = userDF['user.id']
                   print('reactions Participant')
                   loadParticipants(partDF,userID) 

               userDF = userDF.loc[:, ~userDF.columns.str.startswith('user.guardian_of')]
               print('reactions user')
               userDF.columns = userDF.columns.str.removeprefix('user.') 
               userDF = userDF.rename(columns={'id': 'user_id'})
               loadUser(userDF)
    df = df.loc[:, ~df.columns.str.startswith('user')]   
    df.to_sql(name='reactions_tmp',con=engine,if_exists='append',index=False)
    conn.close()
    engine.dispose()

def loadParticipants(df,userID):
    engine = getEngine()
    conn = engine.connect()

    for index,row in df.iterrows():
        partDF = pd.json_normalize(row)
        partDF = partDF
        partDF['user_id'] = userID
        partDF.to_sql(name='participant_tmp',con=engine,if_exists='append',index=False)
    print("Participants loaded")
    conn.close()
    engine.dispose()

"""
def updateData():
    engine = getEngine()
    connStr= engine.connect().connection
    cursor = connStr.cursor()

    cursor.execute ("UPDATE Teams \
                            SET team_name=%s \
                            WHERE team_id=%s", (team_name, team_id))
    print("Team updated")
    cursor.close()
    connStr.close()
"""  