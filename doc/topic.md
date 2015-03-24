Topic Types:

static: 

    /brokers/alerts/
    /brokers/clients/connected
    /brokers/clients/disconnected

dynamic:
    
    created when subscribe...

bridge:

    cretated when bridge...


## Create Topics

emqttd_pubsub:create(Type, Name) 
emqttd_pubsub:create(#topic{name = Name, node= node(), type = Type}).

