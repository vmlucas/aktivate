import pandas as pd
import json
import psycopg2
from functions import loadCallback,loadChannel,loadMembers,loadMessage,loadUser,loadParticipants,loadReaction

#loading Callbacks
dataDF = pd.read_csv('callbacks.csv')
dataDF = dataDF.fillna("")
for index, row in dataDF.iterrows():
    data = json.loads(row.data)
    callbackDF = pd.json_normalize(data)
    print('Callback:',index+2)
    #load callbacks
    df = callbackDF.loc[:, ~callbackDF.columns.str.startswith('user.')].copy()
    df = df.loc[:, ~df.columns.str.startswith('members')] 
    df = df.loc[:, ~df.columns.str.startswith('message.')] 
    df = df.loc[:, ~df.columns.str.startswith('channel')]  
    df = df.loc[:, ~df.columns.str.startswith('reaction')]  
    df['callbacksid'] = index
    df['user_id'] = callbackDF['user.id']
    #if index == 5:
    #      display(callbackDF)
    loadCallback(df) 
    
    #load channel
    if( callbackDF.columns.str.startswith('channel').any() ):
       channelDF = callbackDF.loc[:, callbackDF.columns.str.startswith('channel')].copy()
       channelDF['callbacksid'] = index
       loadChannel(channelDF,index)

    #load members
    if( callbackDF.columns.str.startswith('members').any() ):
       membersDF = pd.json_normalize(callbackDF['members'])
       loadMembers(membersDF,index,"")
    
    #load reaction
    if( callbackDF.columns.str.startswith('reaction').any() ):
       reactDF = callbackDF.loc[:, callbackDF.columns.str.startswith('reaction')].copy()
       reactDF['callbacksid'] = index
       loadReaction(reactDF)   

    #load message
    if( callbackDF.columns.str.startswith('message').any() ):
       mDF = callbackDF.loc[:, callbackDF.columns.str.startswith('message')].copy()
       mDF['callbacksid'] = index

       #load message user
       if( 'message.user.id' in callbackDF.columns ):
          userDF = mDF.loc[:, mDF.columns.str.startswith('message.user.')].copy()
          mDF['user_id'] = userDF['message.user.id']
          if( 'message.user.guardian_of' in userDF.columns ):
             partDF = pd.json_normalize(userDF['message.user.guardian_of'])
             userID = userDF['message.user.id']
             print('message Participant')
             loadParticipants(partDF,userID) 
          userDF = userDF.loc[:, ~userDF.columns.str.startswith('message.user.guardian_of')]
          print('Message user')
          userDF.columns = userDF.columns.str.removeprefix('message.user.') 
          userDF = userDF.rename(columns={'id': 'user_id'})
          loadUser(userDF) 
       mDF = mDF.loc[:, ~mDF.columns.str.startswith('message.user.')] 
       print('Message')
       mDF.columns = mDF.columns.str.removeprefix('message.') 
       loadMessage(mDF) 
       

    #load user
    userDF = callbackDF.loc[:, callbackDF.columns.str.startswith('user.')].copy()
    userDF = userDF.loc[:, ~userDF.columns.str.startswith('user.guardian_of')]
    userDF.columns = userDF.columns.str.removeprefix('user.')
    userDF = userDF.rename(columns={'id': 'user_id'})
    userID = userDF['user_id'] 
    loadUser(userDF) 
    if( 'user.guardian_of' in callbackDF.columns ):
         partDF = pd.json_normalize(callbackDF['user.guardian_of'])
         print('Participant')
         loadParticipants(partDF,userID) 
    