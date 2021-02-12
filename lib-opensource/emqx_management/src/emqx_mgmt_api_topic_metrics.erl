%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_api_topic_metrics).

-import(minirest, [return/1]).

-rest_api(#{name   => list_all_topic_metrics,
            method => 'GET',
            path   => "/topic-metrics",
            func   => list,
            descr  => "A list of all topic metrics of all nodes in the cluster"}).

-rest_api(#{name   => list_topic_metrics,
            method => 'GET',
            path   => "/topic-metrics/:bin:topic",
            func   => list,
            descr  => "A list of specfied topic metrics of all nodes in the cluster"}).

-rest_api(#{name   => register_topic_metrics,
            method => 'POST',
            path   => "/topic-metrics",
            func   => register,
            descr  => "Register topic metrics"}).

-rest_api(#{name   => unregister_all_topic_metrics,
            method => 'DELETE',
            path   => "/topic-metrics",
            func   => unregister,
            descr  => "Unregister all topic metrics"}).

-rest_api(#{name   => unregister_topic_metrics,
            method => 'DELETE',
            path   => "/topic-metrics/:bin:topic",
            func   => unregister,
            descr  => "Unregister topic metrics"}).

-export([ list/2
        , register/2
        , unregister/2
        ]).

list(#{topic := Topic0}, _Params) ->
    execute_when_enabled(fun() ->
        Topic = emqx_mgmt_util:urldecode(Topic0),
        case safe_validate(Topic) of
            true -> 
                case emqx_mgmt:get_topic_metrics(Topic) of
                    {error, Reason} -> return({error, Reason});
                    Metrics         -> return({ok, maps:from_list(Metrics)})
                end;
            false ->
                return({error, invalid_topic_name})
        end
    end);

list(_Bindings, _Params) ->
    execute_when_enabled(fun() ->
        case emqx_mgmt:get_all_topic_metrics() of
            {error, Reason} -> return({error, Reason});
            Metrics         -> return({ok, Metrics})
        end
    end).
    
register(_Bindings, Params) ->
    execute_when_enabled(fun() ->
        case proplists:get_value(<<"topic">>, Params) of
            undefined ->
                return({error, missing_required_params});
            Topic ->
                case safe_validate(Topic) of
                    true -> 
                        emqx_mgmt:register_topic_metrics(Topic),
                        return(ok);
                    false ->
                        return({error, invalid_topic_name})
                end
        end
    end).

unregister(Bindings, _Params) when map_size(Bindings) =:= 0 ->
    execute_when_enabled(fun() ->
        emqx_mgmt:unregister_all_topic_metrics(),
        return(ok)
    end);

unregister(#{topic := Topic0}, _Params) ->
    execute_when_enabled(fun() ->
        Topic = emqx_mgmt_util:urldecode(Topic0),
        case safe_validate(Topic) of
            true -> 
                emqx_mgmt:unregister_topic_metrics(Topic),
                return(ok);
            false ->
                return({error, invalid_topic_name})
        end
    end).

execute_when_enabled(Fun) ->
    Enabled = case emqx_modules:find_module(emqx_mod_topic_metrics) of
                  [{_, false}] -> false;
                  [{_, true}] -> true
              end,
    case Enabled of
        true ->
            Fun();
        false ->
            return({error, module_not_loaded})
    end.

safe_validate(Topic) ->
    try emqx_topic:validate(name, Topic) of
        true -> true
    catch
        error:_Error ->
            false
    end.
