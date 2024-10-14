%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2024, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 02. 10月 2024 下午4:41
%%%-------------------------------------------------------------------
-module(alinkdata_hole_deepth).
-author("yqfclid").

-export([
    calc/2
]).


%% 测斜孔传感器数据
-record(recv_data, {
    id = 1,              % 测斜传感器id
    x_deg = 0.0,                % x方向偏移角度，从传感器获取
    y_deg = 0.0,                % y方向偏移角度，从传感器获取
    length = 0.8,               % 测斜传感器长度（不是轮距，按轮距来计算是错误的），单位：m
    depth = 0.0                 % 设计深度，测斜传感器顶部距孔口深度，单位：m
}).

%% 测斜孔传感器计算后的结果,单位mm
-record(result_data, {
    id,
    x1_disp,        % x方向，不考虑两传感器之间土体变形，比实际值偏小
    y1_disp,        % y方向，不考虑两传感器之间土体变形，比实际值偏小
    x2_disp,        % x方向，考虑两传感器之间土体变形，比实际值偏大
    y2_disp,        % y方向，考虑两传感器之间土体变形，比实际值偏大
    depth           % 设计深度，测斜传感器顶部距孔口深度，单位：m
}).

calc(HoleDeepth, RawData) ->
    A_List =
        lists:map(
            fun({Addr, Data}) ->
                #recv_data{
                    id = Addr,
                    x_deg = get_value_from_origin(<<"sd1">>, Data),
                    y_deg = get_value_from_origin(<<"sd4">>, Data),
                    length = get_value_from_origin(<<"length">>, Data),
                    depth = get_value_from_origin(<<"sd2">>, Data)
                }
            end, RawData),

    % 孔深

    %{ok, Sort_List} = calc_relative_disp({A_List, Hole_Depth}),

    % 计算累计变形
    {ok, C_List} = calc_total_disp({A_List, HoleDeepth}),

    % 显示
    lists:map(
        fun(CalcData) ->
            #result_data{
                id = Addr,
                x1_disp = X1Disp,
                y1_disp = Y1Disp,
                x2_disp = X2Disp,
                y2_disp = Y2Disp,
                depth = Deepth
            } = CalcData,
            #{
                <<"addr">> => Addr,
                <<"x1_disp">> => X1Disp,
                <<"y1_disp">> => Y1Disp,
                <<"x2_disp">> => X2Disp,
                <<"y2_disp">> => Y2Disp,
                <<"depth">> => Deepth
            }

        end, C_List).

get_value_from_origin(K, Data) ->
    maps:get(<<"value">>, maps:get(K, Data, #{}), 0.0).

%% ----------------------------- API ------------------------------------

%%
%% 功能：计算累计变形
%% 参数：List：列表，元素类型为#recv_data{}，测斜孔各个传感器两侧数据
%%          Hole_Depth: 测斜孔孔深
%% 返回值：各个传感器相对竖向位移
%%
calc_total_disp({List, Hole_Depth}) ->
    % 先计算每个传感器的相对位移
    {ok, Sort_List} = calc_relative_disp({List, Hole_Depth}),

    % 取出尾元素（孔底测斜传感器）
    [First | _] = Sort_List,

    % 计算每个传感器的相对位移
    {ok, Result} = calc_total_disp(Sort_List, []),
    io:format("sssss~p", [{Sort_List, Result}]),

    % 最终计算结果
    {ok, lists:flatten([Result | [First]])}.

calc_total_disp([H1, H2 | T], Acc) ->
    % 计算每个传感器相对土体变形
    HH = H2,

    Result = HH#result_data{
        x1_disp = H1#result_data.x1_disp + H2#result_data.x1_disp,
        y1_disp = H1#result_data.y1_disp + H2#result_data.y1_disp,
        x2_disp = H1#result_data.x2_disp + H2#result_data.x2_disp,
        y2_disp = H1#result_data.y2_disp + H2#result_data.y2_disp
    },

    calc_total_disp([Result | T], [Result | Acc]);
calc_total_disp([_], Acc) ->
    {ok, lists:reverse([Acc])}.

%% --------------------------- 内部函数 ----------------------------------

%% 角度转弧度
parse_radian(Deg) ->
    math:pi() * Deg / 180.0.

%% 计算第k个固定式测斜仪相对位移（相对与第k根测斜杆杆底）
calc_single_disp({X_Deg, Y_Deg, Len}) ->
    X_Disp = Len * math:sin(parse_radian(X_Deg)),
    Y_Disp = Len * math:sin(parse_radian(Y_Deg)),

    {X_Disp, Y_Disp}.

%% 按照深度从大到小排序
sort_recv_data(List) ->
    lists:sort(
        fun(X, Y) ->
            R_X = X,
            R_Y = Y,
            R_X#recv_data.depth < R_Y#recv_data.depth
        end, List).

%% 依次计算传感的相对位移，算法：从上到下
calc_relative_disp({List, Hole_Depth}) ->
    % 先排序
    O_List = sort_recv_data(List),

    calc_relative_disp({O_List, Hole_Depth}, []).

calc_relative_disp({[H1, H2 | T], Hole_Depth}, Acc) ->
    % 相邻传感器间距，从上到下
    Interval = H2#recv_data.depth - H1#recv_data.depth,

    % 计算土体变形
    Result = calc_single_relative_disp(H1, Interval),

    calc_relative_disp({[H2 | T], Hole_Depth}, [Result | Acc]);
calc_relative_disp({[R], Hole_Depth}, Acc) ->
    % 相邻传感器间距，从上到下
    Interval = Hole_Depth - R#recv_data.depth,

    % 计算土体变形
    Result = calc_single_relative_disp(R, Interval),

    {ok, [Result | Acc]}.

% 计算每个传感器相对土体变形
calc_single_relative_disp(Recv, Interval) ->

    % 不考虑两传感器之间土体变形
    {X1, Y1} = calc_single_disp({
        Recv#recv_data.x_deg,
        Recv#recv_data.y_deg,
        Recv#recv_data.length * 1000.0
    }),

    % 考虑两传感器之间土体变形
    {X2, Y2} = calc_single_disp({
        Recv#recv_data.x_deg,
        Recv#recv_data.y_deg,
        Interval * 1000.0
    }),

    Result = #result_data{
        id = Recv#recv_data.id,
        x1_disp = X1,
        y1_disp = Y1,
        x2_disp = X2,
        y2_disp = Y2,
        depth = Recv#recv_data.depth
    },

    Result.