%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

%% @doc emqttd distribution functions
%% @author Feng Lee <feng@emqtt.io>
-module(emqttd_dist).

-import(lists, [concat/1]).

-export([parse_node/1]).

parse_node(Name) when is_list(Name) ->
    case string:tokens(Name, "@") of
        [_Node, _Server] ->
            list_to_atom(Name);
        _ ->
            list_to_atom(with_domain(Name))
    end.

with_domain(Name) ->
    case net_kernel:longnames() of
    true ->
        concat([Name, "@", inet_db:gethostname(),
                  ".", inet_db:res_option(domain)]);
    false ->
        concat([Name, "@", inet_db:gethostname()]);
    _ ->
        Name
    end.

