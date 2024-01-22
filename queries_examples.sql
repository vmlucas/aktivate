--users and their child with message type and text
select u.name, p.participant_name as Child, c."type",c.team,m.html as "message text",m."type"
   from callbacks c, message m, users u, participant p, reactions r 
   where c.message_id = m.message_id
        and u.user_id = c.user_id
        and p.user_id = u.user_id
        
--users with message type, text and reaction
select u.name,c."type",c.team,m.html,m."type",r.score,r."type"  
  from reactions r, callbacks c, message m, users u 
   where r.callbacksid = c.callbacksid 
      and m.message_id = c.message_id 
      and u.user_id = c.user_id 
        
 --gruping reactions types     
select u.name, m.message_id,r."type", count(*) total  
  from reactions r, callbacks c, message m, users u 
   where r.callbacksid = c.callbacksid 
      and m.message_id = c.message_id 
      and u.user_id = c.user_id 
   group by u.user_id, m.message_id, r."type"
   order by u.name
   
--Christopher Williams role, activity and channel type on a day   
select c.created_at,c.callbacksid, u.name,m."role",m.banned,c.type as activity, c.team,c2.channel_type  
     from members m, users u, callbacks c,channel c2 
     where c.callbacksid = m .callbacksid  
        and m.user_id = u.user_id 
        and c.user_id = u.user_id 
        and m.channelid = c2.channel_id 
        and m.callbacksid = c2.callbacksid 
        and u.name ='Christopher Williams'
     order by c.type, c.created_at  
     
 