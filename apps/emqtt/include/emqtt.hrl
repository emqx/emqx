%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%%
%% The Initial Developer of the Original Code is ery.lee@gmail.com
%% Copyright (c) 2012 Ery Lee.  All rights reserved.
%%

%% ---------------------------------
%% banner
%% ---------------------------------
-define(COPYRIGHT, "Copyright (C) 2012 Ery Lee."). 

-define(LICENSE_MESSAGE, "Licensed under the MPL.").               

-define(PROTOCOL_VERSION, "MQTT/3.1").                                                 

-define(ERTS_MINIMUM, "5.6.3").                  

%% qos levels

-define(QOS_0, 0).
-define(QOS_1, 1).
-define(QOS_2, 2).

-record(mqtt_msg,            {retain,
                              qos,
                              topic,
                              dup,
                              message_id,
                              payload,
							  encoder}).
