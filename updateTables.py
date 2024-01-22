import pandas as pd
import json
import psycopg2
from functions import getEngine

engine = getEngine()
connStr= engine.connect().connection
mycursor = connStr.cursor()

#update users
mycursor.execute('update users set \
                    "name"  = t."name",\
                    created_at = t.created_at,\
                    created_by = t.created_by,\
                    banned = t.banned,\
                    channel_unread_count = t.channel_unread_count,\
                    channel_last_read_at = t.channel_last_read_at,\
                    online = t.online,\
                    role = t."role",\
                    total_unread_count = t.total_unread_count,\
                    unread_channels = t.unread_channels,\
                    unread_count = t.unread_count,\
                    confirmed_at = t.confirmed_at,\
                    last_active = t.last_active,\
                    teams = t.teams,\
                    deleted_at = t.deleted_at,\
                    aktivate_teams = t.aktivate_teams,\
                    dashboard_user = t.dashboard_user,\
                    first_name = t.first_name,\
                    "language" = t."language",\
                    last_name = t.last_name,\
                    staff_user = t.staff_user \
                    from users_tmp t\
                 where users.user_id = t.user_id ')

#reactions 
mycursor.execute('DELETE FROM reactions_tmp \
                   WHERE created_at  IN (SELECT created_at \
                   FROM (SELECT created_at, \
                             ROW_NUMBER() OVER (partition BY created_at,message_id,score,"type",updated_at,callbacksid \
                                 ORDER BY created_at,message_id,score,"type",updated_at,callbacksid) AS rnum \
                     FROM reactions_tmp) t \
              WHERE t.rnum > 1)')

mycursor.execute('DELETE FROM reactions_tmp \
                     where callbacksid is null') 
   
mycursor.execute('insert into reactions \
                     select * from reactions_tmp ')   
mycursor.execute('delete from reactions_tmp ')

#update aktivate teams
mycursor.execute('delete from aktivate_teams_tmp \
                      where guardian_of is null and roles is null and team is null ')

mycursor.execute('insert into aktivate_teams  \
                     SELECT distinct guardian_of,roles,team,user_id  \
                      FROM aktivate_teams_tmp') 
mycursor.execute('delete from aktivate_teams_tmp ') 

#update message
mycursor.execute('insert into message \
                     select * from message_tmp ')
mycursor.execute('delete from message_tmp  ') 

mycursor.execute('insert into details \
                     select * from details_tmp')
mycursor.execute('delete from details_tmp')

#upate channel
mycursor.execute('insert into channel \
                   select * from channel_tmp') 
mycursor.execute('delete from channel_tmp')

mycursor.execute('insert into config(automod,automod_behavior,blocklist,blocklist_behavior,connect_events,created_at,\
	                 custom_events,max_message_length,message_retention,mutes,"name",\
	                 push_notifications,quotes,reactions,read_events,reminders,\
	                 replies,"search",typing_events,updated_at,uploads,url_enrichment,\
	                 channelid,id)\
                  select automod,automod_behavior,blocklist,blocklist_behavior,cast(connect_events as boolean),created_at,\
	                 custom_events,max_message_length,message_retention,mutes,"name",\
	                 push_notifications,quotes,reactions,read_events,reminders,\
	                 replies,"search",typing_events,updated_at,uploads,url_enrichment,\
	                 channelid,id \
                         from config_tmp ') 
mycursor.execute('delete from config_tmp')

mycursor.execute('insert into automod_thresholds \
                     select * from automod_thresholds_tmp')
mycursor.execute('delete from automod_thresholds_tmp ')

mycursor.execute('insert into commands \
                     select * from commands_tmp ')
mycursor.execute('delete from commands_tmp')  

#members
mycursor.execute('insert into members \
                    select * from members_tmp ')
mycursor.execute('delete from members_tmp ')

   
mycursor.execute('DELETE FROM participant_tmp \
                     WHERE participant_person_id  IN (SELECT participant_person_id \
                            FROM (SELECT participant_person_id, \
                                         ROW_NUMBER() OVER ( \
                                             partition BY participant_name , participant_person_id, team_id,team_name,user_id \
                                             ORDER BY participant_name , participant_person_id, team_id,team_name,user_id) AS rnum \
                     FROM participant_tmp) t \
              WHERE t.rnum > 1)')
             
mycursor.execute('delete from participant_tmp \
        where participant_name is null and participant_person_id is null and team_id is null and team_name is null ')

mycursor.execute('insert into participant(participant_name, participant_person_id, team_id, team_name,user_id)  \
                     select participant_name, cast(participant_person_id as int), team_id, team_name, user_id from participant_tmp ')
mycursor.execute('delete from participant_tmp ')   

mycursor.execute('update teams set  \
                      team_name  = p.team_name \
                  from participant p \
                  where teams.team_id = p.team_id')

connStr.commit()
print("Data updated")
mycursor.close()
connStr.close()
engine.dispose()
