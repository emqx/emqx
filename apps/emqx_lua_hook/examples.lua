--
-- Given all funcation names needed register to system
--
function register_hook()
    return "on_client_connected",
           "on_client_disconnected",
           "on_client_subscribe",
           "on_client_unsubscribe",
           "on_session_subscribed",
           "on_session_unsubscribed",
           "on_message_delivered",
           "on_message_acked",
           "on_message_publish"
end

----------------------------------------------------------------------
-- Callback Functions 

function on_client_connected(clientid, username, returncode)
    print("Lua: on_client_connected - " .. clientid)
    -- do your job here
    return
end

function on_client_disconnected(clientid, username, reason)
    print("Lua: on_client_disconnected - " .. clientid)
    -- do your job here
    return
end

function on_client_subscribe(clientid, username, topic)
    print("Lua: on_client_subscribe - " .. clientid)
    -- do your job here
    return topic
end

function on_client_unsubscribe(clientid, username, topic)
    print("Lua: on_client_unsubscribe - " .. clientid)
    -- do your job here
    return topic
end

function on_session_subscribed(clientid, username, topic)
    print("Lua: on_session_subscribed - " .. clientid)
    -- do your job here
    return
end

function on_session_unsubscribed(clientid, username, topic)
    print("Lua: on_session_unsubscribed - " .. clientid)
    -- do your job here
    return
end

function on_message_delivered(clientid, username, topic, payload, qos, retain)
    print("Lua: on_message_delivered - " .. clientid)
    -- do your job here
    return topic, payload, qos, retain
end

function on_message_acked(clientid, username, topic, payload, qos, retain)
    print("Lua: on_message_acked- " .. clientid)
    -- do your job here
    return
end

function on_message_publish(clientid, username, topic, payload, qos, retain)
    print("Lua: on_message_publish - " .. clientid)
    -- do your job here
    return topic, payload, qos, retain
end
