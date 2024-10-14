-module(alinkcore_cache).
-behaviour(gen_server).
-export([start_link/0, save/3, delete/2, lookup/2, delete_object/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([auth_by_device/3, auth_by_product/3, query_product/1, query_product/2, query_device/1, query_children/1]).
-export([sessions_count/0, get_session/1, query_device_scene/1]).
-define(TAB(Name, Idx), list_to_atom(lists:concat([Name, Idx]))).
-define(PRE(ID),
    case is_integer(ID) of
        true -> list_to_binary(lists:concat(["IOT", ID]));
        false when is_tuple(ID) -> ID;
        false -> <<"IOT", ID/binary>>
    end).

-define(SERVER, ?MODULE).
-record(state, {}).

-include("alinkcore.hrl").

get_session(Addr) ->
    case lookup(session, Addr) of
        {ok, #session{node = Node, pid = Pid, connect_time = ConnectTime}} ->
            {ok, #{node => Node, pid => Pid, connect_time => ConnectTime}};
        undefined ->
            undefined
    end.

sessions_count() ->
    Size = get_size(session),
    lists:foldl(
        fun(Idx, Acc) ->
            TableName = list_to_atom(lists:concat([session, Idx - 1])),
            Acc + ets:info(TableName, size)
        end, 0, lists:seq(1, Size)).

auth_by_device(Addr, Dk, Ds) ->
    case query_device(Addr) of
        {error, Reason} ->
            {error, Reason};
        {ok, #{<<"dk">> := Dk0, <<"ds">> := Ds0}} ->
            Dk == Dk0 andalso Ds == Ds0
    end.

auth_by_product(Addr, Pk, Ps) ->
    case query_product(Pk, Ps) of
        {ok, #{
            <<"auto_register">> := AutoRegister,
            <<"owner">> := Owner,
            <<"owner_dept">> := OwnerDept,
            <<"id">> := ProductId
        }} ->
            case query_device(Addr) of
                {error, notfound} when AutoRegister =:= <<"1">> ->
                    Info = #{
                        <<"addr">> => Addr,
                        <<"owner">> => Owner,
                        <<"owner_dept">> => OwnerDept,
                        <<"product">> => ProductId,
                        <<"name">> => Addr
                    },
                    catch alinkdata_dao:query_no_count('POST_device', Info),
                    true;
                {ok, #{<<"product">> := ProductId}} ->
                    true;
                _ ->
                    false
            end;
        _ ->
            false
    end.

query_product(Pk, Ps) ->
    Query = #{
        <<"keys">> => <<"id,thing,protocol,auto_register,pk,ps,node_type,owner,owner_dept">>,
        <<"pageNum">> => 1,
        <<"where">> => #{
            <<"status">> => <<"0">>,
            <<"pk">> => Pk,
            <<"ps">> => Ps
        }
    },
    Cache = lookup(product, {Pk, Ps}),
    case Cache == undefined andalso query_table(<<"sys_product">>, Query) of
        false ->
            Cache;
        {error, Reason} ->
            {error, Reason};
        {ok, #{<<"id">> := _ProductId} = Product} ->
            save(product, {Pk, Ps}, Product),
%%            save(product, ProductId, maps:without([<<"id">>], Product)),
            {ok, Product}
    end.

query_product(ProductId) ->
    Query = #{
        <<"keys">> => <<"thing,protocol,auto_register,pk,ps,node_type,owner,owner_dept,config">>,
        <<"pageNum">> => 1,
        <<"where">> => #{
            <<"status">> => <<"0">>,
            <<"id">> => ProductId
        }
    },
    Cache = lookup(product, ProductId),
    case Cache == undefined andalso query_table(<<"sys_product">>, Query) of
        false ->
            Cache;
        {error, Reason} ->
            {error, Reason};
        {ok, Product} ->
            save(product, ProductId, Product),
            {ok, Product}
    end.

query_device(Addr) ->
    Query = #{
        <<"keys">> => <<"id,product,dk,ds,config,project">>,
        <<"pageNum">> => 1,
        <<"where">> => #{
            <<"status">> => <<"0">>,
            <<"addr">> => Addr
        }
    },
    Cache = lookup(device, Addr),
    case Cache == undefined andalso query_table(<<"sys_device">>, Query) of
        false ->
            Cache;
        {error, Reason} ->
            {error, Reason};
        {ok, Device} ->
            save(device, Addr, Device),
            {ok, Device}
    end.

query_children(Gateway) ->
    Query = #{
        <<"gateway">> => Gateway
    },
    Keys = [<<"addr">>,<<"subAddr">>],
    Cache = lookup(children, Gateway),
    case Cache == undefined andalso alinkdata_dao:query_no_count(select_device_children, Query) of
        false ->
            Cache;
        {error, Reason} ->
            {error, Reason};
        {ok, Children} ->
            Fun =
                fun(#{ <<"addr">> := Addr } = Device) ->
%%                    Device1 = format_query(<<"sys_device">>, Device),
%%                    save(device, Addr, maps:with([<<"product">>,<<"dk">>,<<"ds">>,<<"config">>], Device1)),
                    maps:with(Keys, Device)
                end,
             NewChildren = [Fun(Child) ||
                 Child = #{ <<"status">> := Status } <- Children,
                 Status == <<"0">>],
            save(children, Gateway, NewChildren),
            {ok, NewChildren}
    end.

query_table(Tab, Query) ->
    case alinkdata_mysql:query(default, Tab, Query) of
        {error, Reason} ->
            {error, Reason};
        {ok, _, []} ->
            {error, notfound};
        {ok, _, [Result|_]} ->
            {ok, format_query(Tab, Result)}
    end.

format_query(<<"sys_device">>, Device) ->
    Config = format_config(Device),
    Device#{<<"config">> => Config};
format_query(<<"sys_product">>, Product) ->
    Thing = format_value(<<"thing">>, Product),
    Config = format_config(Product),
    Product#{<<"thing">> => Thing, <<"config">> => Config}.

format_config(Data) ->
    Configs = format_value(<<"config">>, Data),
    lists:foldl(
        fun(#{<<"name">> := Name, <<"value">> := Value}, Acc) ->
            Acc#{
                Name => Value
            }
        end, #{}, Configs).

format_value(Key, Data) ->
    Config =
        case maps:get(Key, Data, <<"[]">>) of
            null ->
                <<"[]">>;
            <<>> ->
                <<"[]">>;
            O ->
                O
        end,
    jiffy:decode(Config, [return_maps]).

save(Tab, Key, Info) ->
    Idx = erlang:phash2(?PRE(Key), get_size(Tab)),
    Table = ?TAB(Tab, Idx),
    ets:insert(Table, {Key, Info}).

delete(Tab, Key) ->
    Idx = erlang:phash2(?PRE(Key), get_size(Tab)),
    Table = ?TAB(Tab, Idx),
    ets:delete(Table, Key).


delete_object(Tab, Key, Object) ->
    Idx = erlang:phash2(?PRE(Key), get_size(Tab)),
    Table = ?TAB(Tab, Idx),
    ets:delete_object(Table, {Key, Object}).


lookup(Tab, Key) ->
    Idx = erlang:phash2(?PRE(Key), get_size(Tab)),
    Table = ?TAB(Tab, Idx),
    case ets:lookup(Table, Key) of
        [] ->
            undefined;
        [{_, Info} | _] ->
            {ok, Info}
    end.


-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


init([]) ->
    create_table(product),
    create_table(device),
    create_table(children),
    create_table(session),
    create_table(device_scene),
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


-spec get_size(Table :: atom()) -> integer().
get_size(Table) ->
    Opts = application:get_env(alinkdata, table_size, []),
    case lists:keyfind(Table, 1, Opts) of
        false -> 32;
        {Table, Size} -> Size
    end.

-spec create_table(Table :: atom()) -> ok.
create_table(Table) ->
    Size = get_size(Table),
    Opts = [public, named_table, ordered_set, {write_concurrency, true}, {read_concurrency, true}],
    lists:foreach(
        fun(Idx) ->
            ets:new(list_to_atom(lists:concat([Table, Idx - 1])), Opts)
        end, lists:seq(1, Size)).

query_device_scene(DeviceId) ->
    Cache = lookup(device_scene, DeviceId),
    case Cache == undefined andalso alinkdata_dao:query_no_count('QUERY_scene_device', #{<<"device">> => DeviceId}) of
        false ->
            Cache;
        {error, Reason} ->
            {error, Reason};
        {ok, Scenes} ->
            save(device_scene, DeviceId, Scenes),
            {ok, Scenes}
    end.