%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is eMQTT
%%
%% The Initial Developer of the Original Code is <ery.lee at gmail dot com>
%% Copyright (C) 2012 Ery Lee All Rights Reserved.

-module(emqtt_db).

-export([init/0, stop/0]).

init() ->
	case mnesia:system_info(extra_db_nodes) of
    [] -> mnesia:create_schema([node()]);
    _ -> ok
    end,
    ok = mnesia:start(),
    mnesia:wait_for_tables(mnesia:system_info(local_tables), infinity).

stop() ->
	mnesia:stop().


