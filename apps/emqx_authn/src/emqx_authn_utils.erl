%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_authn_utils).

-export([ replace_placeholder/2
        ]).

replace_placeholder(PlaceHolders, Data) ->
    replace_placeholder(PlaceHolders, Data, []).

replace_placeholder([], _Data, Acc) ->
    lists:reverse(Acc);
replace_placeholder([<<"${mqtt-username}">> | More], #{username := Username} = Data, Acc) ->
    replace_placeholder(More, Data, [convert_to_sql_param(Username) | Acc]);
replace_placeholder([<<"${mqtt-clientid}">> | More], #{clientid := ClientID} = Data, Acc) ->
    replace_placeholder(More, Data, [convert_to_sql_param(ClientID) | Acc]);
replace_placeholder([<<"${ip-address}">> | More], #{peerhost := IPAddress} = Data, Acc) ->
    replace_placeholder(More, Data, [convert_to_sql_param(IPAddress) | Acc]);
replace_placeholder([<<"${cert-subject}">> | More], #{dn := Subject} = Data, Acc) ->
    replace_placeholder(More, Data, [convert_to_sql_param(Subject) | Acc]);
replace_placeholder([<<"${cert-common-name}">> | More], #{cn := CommonName} = Data, Acc) ->
    replace_placeholder(More, Data, [convert_to_sql_param(CommonName) | Acc]);
replace_placeholder([_ | More], Data, Acc) ->
    replace_placeholder(More, Data, [null | Acc]).

convert_to_sql_param(undefined) ->
    null;
convert_to_sql_param(V) ->
    bin(V).

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(L) when is_list(L) -> list_to_binary(L);
bin(X) -> X.
