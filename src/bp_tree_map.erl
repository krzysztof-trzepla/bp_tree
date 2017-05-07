%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree_map).
-author("Krzysztof Trzepla").

%% API exports
-export([new/1]).
-export([insert/3, erase/2]).
-export([at/2, first/1, last/1, find/2, lower_bound/2, upper_bound/2]).
-export([size/1]).
-export([to_list/1, from_list/1, from_list/2]).

-record(map, {
    size :: non_neg_integer(),
    data :: tuple()
}).

-type key() :: bp_tree:key().
-type value() :: bp_tree:value() | bp_tree_node:id().
-opaque ordered_map() :: #map{}.

-export_type([key/0, value/0, ordered_map/0]).

%%====================================================================
%% API functions
%%====================================================================

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec new(non_neg_integer()) -> ordered_map().
new(MaxSize) ->
    #map{
        size = 0,
        data = erlang:list_to_tuple(lists:duplicate(MaxSize, undefined))
    }.

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec insert(key(), value(), ordered_map()) ->
    {ok, ordered_map()} | {error, out_of_space | already_exists}.
insert(_Key, _Value, #map{size = Size, data = Data})
    when Size =:= erlang:size(Data) ->
    {error, out_of_space};
insert(Key, Value, Map = #map{size = Size, data = Data}) ->
    Pos = lower_bound(Key, Map),
    case at(Pos, Map) of
        {ok, {Key, _Value}} ->
            {error, already_exists};
        {ok, _} ->
            Map2 = #map{data = Data2} = shift_right(Pos, Map),
            {ok, Map2#map{data = erlang:setelement(Pos, Data2, {Key, Value})}};
        {error, out_of_range} ->
            {ok, Map#map{
                size = Size + 1,
                data = erlang:setelement(Size + 1, Data, {Key, Value})
            }}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec erase(key(), ordered_map()) ->
    {ok, ordered_map()} | {error, empty | not_found}.
erase(_Key, #map{size = 0}) ->
    {error, empty};
erase(Key, Map = #map{}) ->
    Pos = lower_bound(Key, Map),
    case at(Pos, Map) of
        {ok, {Key, _Value}} -> {ok, shift_left(Pos, Map)};
        _ -> {error, not_found}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec at(pos_integer(), ordered_map()) ->
    {ok, {key(), value()}} | {error, out_of_range}.
at(Pos, #map{size = Size, data = Data}) when 1 =< Pos andalso Pos =< Size ->
    {ok, erlang:element(Pos, Data)};
at(_Pos, #map{}) ->
    {error, out_of_range}.

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec first(ordered_map()) -> {ok, {key(), value()}} | {error, out_of_range}.
first(Map = #map{}) ->
    at(1, Map).

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec last(ordered_map()) -> {ok, {key(), value()}} | {error, out_of_range}.
last(Map = #map{size = Size}) ->
    at(Size, Map).

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec find(key(), ordered_map()) -> {ok, value()} | {error, not_found}.
find(Key, Map = #map{}) ->
    Pos = lower_bound(Key, Map),
    case at(Pos, Map) of
        {ok, {Key, Value}} -> {ok, Value};
        _ -> {error, not_found}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec lower_bound(key(), ordered_map()) -> non_neg_integer().
lower_bound(Key, Map = #map{size = Size}) ->
    lower_bound(Key, 1, Size, Map).

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec upper_bound(key(), ordered_map()) -> non_neg_integer().
upper_bound(Key, Map = #map{size = Size}) ->
    upper_bound(Key, 1, Size, Map).

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec size(ordered_map()) -> non_neg_integer().
size(#map{size = Size}) ->
    Size.

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec to_list(ordered_map()) -> [{bp_tree:key(), bp_tree:value()}].
to_list(#map{size = Size, data = Data}) ->
    lists:map(fun(Pos) ->
        erlang:element(Pos, Data)
    end, lists:seq(1, Size)).

%%--------------------------------------------------------------------
%% @equiv from_list(List, erlang:length(List))
%% @end
%%--------------------------------------------------------------------
-spec from_list([{bp_tree:key(), bp_tree:value()}]) -> ordered_map().
from_list(List) ->
    from_list(List, erlang:length(List)).

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec from_list([{bp_tree:key(), bp_tree:value()}], non_neg_integer()) ->
    ordered_map().
from_list(List, MaxSize) ->
    Data = erlang:list_to_tuple(lists:duplicate(MaxSize, undefined)),
    {Size, Data3} = lists:foldl(fun({_K, _V} = Pair, {Pos, Data2}) ->
        {Pos + 1, erlang:setelement(Pos + 1, Data2, Pair)}
    end, {0, Data}, List),
    #map{
        size = Size,
        data = Data3
    }.

%%====================================================================
%% Internal functions
%%====================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec lower_bound(key(), pos_integer(), pos_integer(), ordered_map()) ->
    pos_integer().
lower_bound(Key, Lower, Upper, Map) when Lower =< Upper ->
    Mid = (Lower + Upper) div 2,
    {ok, {MidKey, _MidValue}} = at(Mid, Map),
    if
        MidKey < Key -> lower_bound(Key, Mid + 1, Upper, Map);
        true -> lower_bound(Key, Lower, Mid - 1, Map)
    end;
lower_bound(_Key, Lower, _Upper, _Map) ->
    Lower.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec upper_bound(key(), pos_integer(), pos_integer(), ordered_map()) ->
    pos_integer().
upper_bound(Key, Lower, Upper, Map) when Lower =< Upper ->
    Mid = (Lower + Upper) div 2,
    {ok, {MidKey, _MidValue}} = at(Mid, Map),
    if
        MidKey =< Key -> upper_bound(Key, Mid + 1, Upper, Map);
        true -> upper_bound(Key, Lower, Mid - 1, Map)
    end;
upper_bound(_Key, Lower, _Upper, _Map) ->
    Lower.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec shift_right(pos_integer(), ordered_map()) -> ordered_map().
shift_right(StartPos, Map = #map{size = Size, data = Data}) ->
    Data3 = lists:foldl(fun(Pos, Data2) ->
        erlang:setelement(Pos, Data2, erlang:element(Pos - 1, Data2))
    end, Data, lists:seq(Size + 1, StartPos + 1, -1)),
    Map#map{size = Size + 1, data = Data3}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec shift_left(pos_integer(), ordered_map()) -> ordered_map().
shift_left(StartPos, Map = #map{size = Size, data = Data}) ->
    Data3 = lists:foldl(fun(Pos, Data2) ->
        erlang:setelement(Pos, Data2, erlang:element(Pos + 1, Data2))
    end, Data, lists:seq(StartPos, Size - 1)),
    Map#map{size = Size - 1, data = erlang:setelement(Size, Data3, undefined)}.