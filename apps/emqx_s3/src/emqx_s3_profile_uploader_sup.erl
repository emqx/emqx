%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_profile_uploader_sup).

-behaviour(supervisor).

-include_lib("emqx/include/types.hrl").

-export([
    start_link/1,
    child_spec/1,
    id/1,
    start_uploader/2
]).

-export([init/1]).

-export_type([id/0]).

-type id() :: {?MODULE, emqx_s3:profile_id()}.

-spec start_link(emqx_s3:profile_id()) -> supervisor:start_ret().
start_link(ProfileId) ->
    supervisor:start_link(?MODULE, [ProfileId]).

-spec child_spec(emqx_s3:profile_id()) -> supervisor:child_spec().
child_spec(ProfileId) ->
    #{
        id => id(ProfileId),
        start => {?MODULE, start_link, [ProfileId]},
        restart => permanent,
        shutdown => 5000,
        type => supervisor,
        modules => [?MODULE]
    }.

-spec id(emqx_s3:profile_id()) -> id().
id(ProfileId) ->
    {?MODULE, ProfileId}.

-spec start_uploader(emqx_s3:profile_id(), emqx_s3_uploader:opts()) ->
    supervisor:start_ret() | {error, profile_not_found}.
start_uploader(ProfileId, Opts) ->
    Id = id(ProfileId),
    case gproc:where({n, l, Id}) of
        undefined -> {error, profile_not_found};
        Pid -> supervisor:start_child(Pid, [Opts])
    end.

%%--------------------------------------------------------------------
%% supervisor callbacks
%%-------------------------------------------------------------------

init([ProfileId]) ->
    true = gproc:reg({n, l, id(ProfileId)}, ignored),
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 10,
        period => 5
    },
    ChildSpecs = [
        #{
            id => emqx_s3_uploader,
            start => {emqx_s3_uploader, start_link, [ProfileId]},
            restart => temporary,
            shutdown => 5000,
            type => worker,
            modules => [emqx_s3_uploader]
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.
