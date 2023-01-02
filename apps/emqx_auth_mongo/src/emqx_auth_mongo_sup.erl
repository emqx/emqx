%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_mongo_sup).

-behaviour(supervisor).

-include("emqx_auth_mongo.hrl").

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, Opts} = application:get_env(?APP, server),
    NOpts = may_parse_srv_and_txt_records(Opts),
    PoolSpec = ecpool:pool_spec(?APP, ?APP, ?APP, NOpts),
    {ok, {{one_for_all, 10, 100}, [PoolSpec]}}.

may_parse_srv_and_txt_records(Opts) when is_list(Opts) ->
    Default = #{srv_record => false},
    maps:to_list(may_parse_srv_and_txt_records(maps:merge(Default, maps:from_list(Opts))));

may_parse_srv_and_txt_records(#{type := Type,
                                srv_record := false,
                                server := Server} = Opts) ->
    Hosts = to_hosts(Server),
    case Type =:= rs of
        true ->
            case maps:get(rs_set_name, Opts, undefined) of
                undefined ->
                    error({missing_parameter, rs_set_name});
                ReplicaSet ->
                    Opts#{type => {rs, ReplicaSet},
                          hosts => Hosts}
            end;
        false ->
            Opts#{hosts => Hosts}
    end;

may_parse_srv_and_txt_records(#{type := Type,
                                srv_record := true,
                                server := Server,
                                worker_options := WorkerOptions} = Opts) ->
    Hosts = parse_srv_records(Server),
    Opts0 = parse_txt_records(Type, Server),
    NWorkerOptions = maps:to_list(maps:merge(maps:from_list(WorkerOptions), maps:with([auth_source], Opts0))),
    NOpts = Opts#{hosts => Hosts, worker_options => NWorkerOptions},
    case Type =:= rs of
        true ->
            case maps:get(rs_set_name, Opts0, maps:get(rs_set_name, NOpts, undefined)) of
                undefined ->
                    error({missing_parameter, rs_set_name});
                ReplicaSet ->
                    NOpts#{type => {Type, ReplicaSet}}
            end;
        false ->
            NOpts
    end.

to_hosts(Server) ->
    [string:trim(H) || H <- string:tokens(Server, ",")].

parse_srv_records(Server) ->
    case inet_res:lookup("_mongodb._tcp." ++ Server, in, srv) of
        [] ->
            error(service_not_found);
        Services ->
            [Host ++ ":" ++ integer_to_list(Port) || {_, _, Port, Host} <- Services]
    end.

parse_txt_records(Type, Server) ->
    case inet_res:lookup(Server, in, txt) of
        [] ->
            #{};
        [[QueryString]] ->
            case uri_string:dissect_query(QueryString) of
                {error, _, _} ->
                    error({invalid_txt_record, invalid_query_string});
                Options ->
                    Fields = case Type of
                                 rs -> ["authSource", "replicaSet"];
                                 _ -> ["authSource"]
                             end,
                    take_and_convert(Fields, Options)
            end;
        _ ->
            error({invalid_txt_record, multiple_records})
    end.

take_and_convert(Fields, Options) ->
    take_and_convert(Fields, Options, #{}).

take_and_convert([], [_ | _], _Acc) ->
    error({invalid_txt_record, invalid_option});
take_and_convert([], [], Acc) ->
    Acc;
take_and_convert([Field | More], Options, Acc) ->
    case lists:keytake(Field, 1, Options) of
        {value, {"authSource", V}, NOptions} ->
            take_and_convert(More, NOptions, Acc#{auth_source => list_to_binary(V)});
        {value, {"replicaSet", V}, NOptions} ->
            take_and_convert(More, NOptions, Acc#{rs_set_name => list_to_binary(V)});
        {value, _, _} ->
            error({invalid_txt_record, invalid_option});
        false ->
            take_and_convert(More, Options, Acc)
    end.
