%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_secret_loader).

%% API
-export([load/1]).
-export([file/1]).

-export_type([source/0]).

-type source() :: {file, string() | binary()}.

-spec load(source()) -> binary() | no_return().
load({file, <<"file://", Path/binary>>}) ->
    file(Path);
load({file, "file://" ++ Path}) ->
    file(Path).

-spec file(file:filename_all()) -> binary() | no_return().
file(Filename0) ->
    Filename = emqx_schema:naive_env_interpolation(Filename0),
    case file:read_file(Filename) of
        {ok, Secret} ->
            string:trim(Secret, trailing);
        {error, Reason} ->
            throw(#{
                msg => failed_to_read_secret_file,
                path => Filename,
                reason => emqx_utils:explain_posix(Reason)
            })
    end.
