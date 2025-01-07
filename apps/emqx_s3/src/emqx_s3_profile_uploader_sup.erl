%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_profile_uploader_sup).

-behaviour(supervisor).

-include_lib("emqx/include/types.hrl").
-include_lib("emqx/include/logger.hrl").

-include("src/emqx_s3.hrl").

-export([
    start_link/1,
    child_spec/1,
    id/1,
    start_uploader/3
]).

-export([init/1]).

-export_type([id/0]).

-type id() :: {?MODULE, emqx_s3:profile_id()}.

-spec start_link(emqx_s3:profile_id()) -> emqx_types:startlink_ret().
start_link(ProfileId) ->
    supervisor:start_link(?VIA_GPROC(id(ProfileId)), ?MODULE, [ProfileId]).

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

-spec start_uploader(emqx_s3:profile_id(), emqx_s3_client:key(), emqx_s3_client:upload_options()) ->
    emqx_types:startlink_ret() | {error, profile_not_found}.
start_uploader(ProfileId, Key, UploadOpts) ->
    try supervisor:start_child(?VIA_GPROC(id(ProfileId)), [Key, UploadOpts]) of
        Result -> Result
    catch
        exit:{noproc, _} -> {error, profile_not_found}
    end.

%%--------------------------------------------------------------------
%% supervisor callbacks
%%-------------------------------------------------------------------

init([ProfileId]) ->
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
