-module(alinkdata_cache).
-behaviour(gen_server).
-export([start_link/0, insert/2, lookup/1, delete/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([clear_device/3, clear_product/3, clear_sub_device/3, clear_scene_device/3]).
-define(SERVER, ?MODULE).
-record(state, {}).

insert(Key, Value) ->
    ets:insert(?MODULE, {Key, Value}).

lookup(Key) ->
    ets:lookup(?MODULE, Key).

delete(Key) ->
    ets:delete(?MODULE, Key).


-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    ets:new(?MODULE, [set, public, named_table]),
    ehttpd_hook:add('PUT_product', {?MODULE, clear_product}),
    ehttpd_hook:add('DELETE_product', {?MODULE, clear_product}),
    ehttpd_hook:add('PUT_device', {?MODULE, clear_device}),
    ehttpd_hook:add('DELETE_device', {?MODULE, clear_device}),
    ehttpd_hook:add('PUT_sub_device', {?MODULE, clear_sub_device}),
    ehttpd_hook:add('POST_sub_device', {?MODULE, clear_sub_device}),
    ehttpd_hook:add('DELETE_sub_device', {?MODULE, clear_sub_device}),
    ehttpd_hook:add('POST_scene_device', {?MODULE, clear_scene_device}),
    ehttpd_hook:add('DELETE_scene_device', {?MODULE, clear_scene_device}),
    {ok, #state{}}.

handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
    {noreply, State}.

handle_info(_Info, State = #state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #state{}) ->
    ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
    {ok, State}.

clear_device(_, #{<<"addr">> := Addrs}, _) when is_list(Addrs) ->
    lists:foreach(
        fun(Addr) ->
            alinkcore_cache:delete(device, Addr)
        end, Addrs);
clear_device(_, #{<<"addr">> := Addr}, _) ->
    alinkcore_cache:delete(device, Addr),
    ok.




clear_product(_, #{<<"id">> := Ids, <<"pkss">> := Pkss}, _) when is_list(Ids) ->
    lists:foreach(
        fun(Id) ->
            alinkcore_cache:delete(product, alinkutil_type:to_integer(Id))
        end, Ids),
    lists:foreach(
        fun(#{<<"pk">> := Pk, <<"ps">> := Ps}) ->
            alinkcore_cache:delete(product, {Pk, Ps})
    end, Pkss);
clear_product(_, #{<<"id">> := Id, <<"pkss">> := Pkss}, _) ->
    alinkcore_cache:delete(product, alinkutil_type:to_integer(Id)),
    lists:foreach(
        fun(#{<<"pk">> := Pk, <<"ps">> := Ps}) ->
            alinkcore_cache:delete(product, {Pk, Ps})
        end, Pkss),
    ok;
clear_product(_, #{<<"id">> := Ids}, _) when is_list(Ids) ->
    lists:foreach(
        fun(Id) ->
            alinkcore_cache:delete(product, alinkutil_type:to_integer(Id))
        end, Ids);
clear_product(_, #{<<"id">> := Id}, _) ->
    alinkcore_cache:delete(product, alinkutil_type:to_integer(Id)),
    ok.



clear_sub_device(_, #{<<"gateways">> := Gateways}, _) when is_list(Gateways) ->
    lists:foreach(
        fun(Addr) ->
            alinkcore_cache:delete(children, Addr)
        end, lists:usort(Gateways));
clear_sub_device(_, #{<<"gateways">> := Gateway}, _) ->
    alinkcore_cache:delete(children, Gateway),
    ok.


clear_scene_device(_, #{<<"devices">> := DeviceIds}, _) when is_list(DeviceIds) ->
    lists:foreach(
        fun(DeviceId) ->
            alinkcore_cache:delete(device_scene, alinkutil_type:to_integer(DeviceId))
        end, lists:usort(DeviceIds));
clear_scene_device(_, #{<<"devices">> := Device}, _) when Device =/= <<>> ->
    alinkcore_cache:delete(device_scene, Device),
    ok;
clear_scene_device(_, _, _) ->
    ok.
