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

-record(topic, {words, path}).

-record(subscriber, {topic, pid}).


%% ---------------------------------
%% Logging mechanism

-define(PRINT(Format, Args),
    io:format(Format, Args)).

-define(PRINT_MSG(Msg),
    io:format(Msg)).

-define(DEBUG(Format, Args),
    lager:debug(Format, Args)).

-define(DEBUG_TRACE(Dest, Format, Args),
    lager:debug(Dest, Format, Args)).

-define(DEBUG_MSG(Msg),
    lager:debug(Msg)).

-define(INFO(Format, Args),
    lager:info(Format, Args)).

-define(INFO_TRACE(Dest, Format, Args),
    lager:info(Dest, Format, Args)).

-define(INFO_MSG(Msg),
    lager:info(Msg)).

-define(WARN(Format, Args),
    lager:warning(Format, Args)).

-define(WARN_TRACE(Dest, Format, Args),
    lager:warning(Dest, Format, Args)).

-define(WARN_MSG(Msg),
    lager:warning(Msg)).
			      
-define(WARNING(Format, Args),
    lager:warning(Format, Args)).

-define(WARNING_TRACE(Dest, Format, Args),
    lager:warning(Dest, Format, Args)).

-define(WARNING_MSG(Msg),
    lager:warning(Msg)).

-define(ERROR(Format, Args),
    lager:error(Format, Args)).

-define(ERROR_TRACE(Dest, Format, Args),
    lager:error(Dest, Format, Args)).

-define(ERROR_MSG(Msg),
    lager:error(Msg)).

-define(CRITICAL(Format, Args),
    lager:critical(Format, Args)).

-define(CRITICAL_TRACE(Dest, Format, Args),
    lager:critical(Dest, Format, Args)).

-define(CRITICAL_MSG(Msg),
    lager:critical(Msg)).

