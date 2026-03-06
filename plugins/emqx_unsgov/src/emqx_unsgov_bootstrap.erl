%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_unsgov_bootstrap).

-moduledoc """
Loads bootstrap UNS models from priv/bootstrap_models/ JSON files.
Called during emqx_unsgov_store init to seed initial models.
""".

-export([load/0]).

-include("emqx_unsgov.hrl").

load() ->
    Files = bootstrap_model_files(),
    lists:foreach(fun load_bootstrap_model_file/1, Files),
    ok.

bootstrap_model_files() ->
    case code:priv_dir(emqx_unsgov) of
        {error, _} ->
            [];
        Dir when is_list(Dir) ->
            lists:sort(filelib:wildcard(filename:join(Dir, "bootstrap_models/*.json")))
    end.

load_bootstrap_model_file(Path) ->
    case file:read_file(Path) of
        {ok, Bin} ->
            try emqx_utils_json:decode(Bin, [return_maps]) of
                Model when is_map(Model) ->
                    ensure_bootstrap_model(Model, Path);
                _Other ->
                    ?LOG(error, #{
                        msg => "skip_bootstrap_model_invalid_json",
                        path => Path
                    })
            catch
                Class:Reason:Stacktrace ->
                    ?LOG(error, #{
                        msg => "skip_bootstrap_model_decode_failed",
                        path => Path,
                        kind => Class,
                        reason => Reason,
                        stacktrace => Stacktrace
                    })
            end;
        {error, Reason} ->
            ?LOG(error, #{
                msg => "skip_bootstrap_model_read_failed",
                path => Path,
                reason => Reason
            })
    end.

ensure_bootstrap_model(Model, Path) ->
    case emqx_unsgov_model:compile(Model) of
        {ok, Compiled} ->
            insert_bootstrap_model(Compiled, Path);
        {error, Reason} ->
            ?LOG(error, #{
                msg => "skip_bootstrap_model_compile_failed",
                path => Path,
                reason => Reason
            })
    end.

insert_bootstrap_model(Compiled, Path) ->
    Id = maps:get(id, Compiled),
    case emqx_unsgov_store:get_model_record(Id) of
        {ok, _} ->
            ?LOG(info, #{
                msg => "skip_bootstrap_model_already_exists",
                path => Path,
                model_id => Id
            });
        error ->
            Ts = erlang:system_time(millisecond),
            Record = #?MODEL_TAB{
                id = Id,
                model = maps:get(raw, Compiled),
                summary = emqx_unsgov_model:summary(Compiled),
                active = true,
                updated_at_ms = Ts
            },
            ok = mria:dirty_write(?MODEL_TAB, Record),
            ?LOG(info, #{
                msg => "loaded_bootstrap_model",
                path => Path,
                model_id => Id
            })
    end.
