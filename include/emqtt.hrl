
-define(COPYRIGHT, "Copyright (C) 2007-2012 VMware, Inc."). 
-define(LICENSE_MESSAGE, "Licensed under the MPL.").               
-define(PROTOCOL_VERSION, "MQTT/3.1").                                                 
-define(ERTS_MINIMUM, "5.6.3").                  

-record(topic, {words, path}).

-record(subscriber, {topic, pid}).


