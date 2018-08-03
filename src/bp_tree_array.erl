%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides an API to a so-called zig-zag array. Zig-zag array
%%% is an array in which two consecutive values are separated with a key that
%%% identifies those values, i.e. each key is associated with two values:
%%% left and right. Moreover keys in zig-zag array are sorted in ascending
%%% order and duplicated keys are not allowed. Example zig-zag array:
%%% v1 k1 v2 k2 v3 k3 v4.
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree_array).
-author("Krzysztof Trzepla").

-include("bp_tree.hrl").

%% API exports
-export([new/1, size/1]).
-export([get/2, update/3, remove/2, remove/3]).
-export([find/2, find_value/2, lower_bound/2]).
-export([insert/3, append/3, prepend/3, split/1, merge/2]).
-export([to_list/1, from_list/1, to_map/1, from_map/1]).

-record(bp_tree_array, {
    size,
    data
}).

-type key() :: any().
-type value() :: any().
-type selector() :: key | left | right | both | lower_bound | lower_bound_key.
-type pos() :: non_neg_integer() | first | last.
-type remove_pred() :: fun((value()) -> boolean()).
-opaque array() :: #bp_tree_array{}.

-export_type([array/0, selector/0]).

%%====================================================================
%% API functions
%%====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a new array.
%% @end
%%--------------------------------------------------------------------
-spec new(pos_integer()) -> array().
new(Size) ->
    #bp_tree_array{
        size = 0,
        data = erlang:make_tuple(2 * Size + 1, ?NIL)
    }.

%%--------------------------------------------------------------------
%% @doc
%% Returns the size of an array.
%% @end
%%--------------------------------------------------------------------
-spec size(array()) -> non_neg_integer().
size(#bp_tree_array{size = Size}) ->
    Size.

%%--------------------------------------------------------------------
%% @doc
%% Returns an item from an array at a selected position.
%% @end
%%--------------------------------------------------------------------
-spec get({selector(), pos()}, array()) ->
    {ok, value() | {value(), value()}} | {error, out_of_range}.
get({lower_bound, Key}, Array) ->
    Pos = lower_bound(Key, Array),
    get({left, Pos}, Array);
get({lower_bound_key, Key}, Array) ->
    Pos = lower_bound(Key, Array),
    get({key, Pos}, Array);
get({Selector, first}, Array = #bp_tree_array{}) ->
    get({Selector, 1}, Array);
get({Selector, last}, Array = #bp_tree_array{size = Size}) ->
    get({Selector, Size}, Array);
get({right, 0}, #bp_tree_array{data = Data}) ->
    {ok, erlang:element(1, Data)};
get({_Selector, Pos}, #bp_tree_array{size = Size})
    when Pos < 1 orelse Pos > Size ->
    {error, out_of_range};
get({left, Pos}, #bp_tree_array{data = Data}) ->
    {ok, erlang:element(2 * Pos - 1, Data)};
get({key, Pos}, #bp_tree_array{data = Data}) ->
    {ok, erlang:element(2 * Pos, Data)};
get({right, Pos}, #bp_tree_array{data = Data}) ->
    {ok, erlang:element(2 * Pos + 1, Data)};
get({both, Pos}, #bp_tree_array{data = Data}) ->
    {ok, {erlang:element(2 * Pos - 1, Data), erlang:element(2 * Pos + 1, Data)}}.

%%--------------------------------------------------------------------
%% @doc
%% Returns an item in an array at a selected position.
%% @end
%%--------------------------------------------------------------------
-spec update({selector(), pos()}, value() | {value(), value()},
    array()) -> {ok, array()} | {error, out_of_range}.
update({Selector, first}, Value, Array = #bp_tree_array{}) ->
    update({Selector, 1}, Value, Array);
update({Selector, last}, Value, Array = #bp_tree_array{size = Size}) ->
    update({Selector, Size}, Value, Array);
update({right, 0}, Value, Array = #bp_tree_array{data = Data}) ->
    {ok, Array#bp_tree_array{data = erlang:setelement(1, Data, Value)}};
update({_Selector, Pos}, _Value, #bp_tree_array{size = Size})
    when Pos < 1 orelse Pos > Size ->
    {error, out_of_range};
update({left, Pos}, Value, Array = #bp_tree_array{data = Data}) ->
    Data2 = erlang:setelement(2 * Pos - 1, Data, Value),
    {ok, Array#bp_tree_array{data = Data2}};
update({key, Pos}, Value, Array = #bp_tree_array{data = Data}) ->
    Data2 = erlang:setelement(2 * Pos, Data, Value),
    {ok, Array#bp_tree_array{data = Data2}};
update({right, Pos}, Value, Array = #bp_tree_array{data = Data}) ->
    Data2 = erlang:setelement(2 * Pos + 1, Data, Value),
    {ok, Array#bp_tree_array{data = Data2}};
update({both, Pos}, {LValue, RValue}, Array = #bp_tree_array{data = Data}) ->
    Data2 = erlang:setelement(2 * Pos - 1, Data, LValue),
    Data3 = erlang:setelement(2 * Pos + 1, Data2, RValue),
    {ok, Array#bp_tree_array{data = Data3}}.

%%--------------------------------------------------------------------
%% @doc
%% Returns position of a key in an array or fails with a missing error.
%% @end
%%--------------------------------------------------------------------
-spec find(key(), array()) -> {ok, pos_integer()} | {error, not_found}.
find(Key, Array = #bp_tree_array{}) ->
    Pos = lower_bound(Key, Array),
    case get({key, Pos}, Array) of
        {ok, Key} -> {ok, Pos};
        {ok, _} -> {error, not_found};
        {error, out_of_range} -> {error, not_found}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns value for a key in an array or fails with a missing error.
%% @end
%%--------------------------------------------------------------------
-spec find_value(key(), array()) -> {ok, value()} | {error, not_found}.
find_value(Key, Array = #bp_tree_array{}) ->
    case find(Key, Array) of
        {ok, Pos} -> get({left, Pos}, Array);
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns a position of a first key in an array that does not compare less
%% than a key.
%% @end
%%--------------------------------------------------------------------
-spec lower_bound(key(), array()) -> pos_integer().
lower_bound(Key, Array = #bp_tree_array{size = Size}) ->
    lower_bound(Key, 1, Size, Array).

%%--------------------------------------------------------------------
%% @doc
%% Inserts a key-value pair into an array.
%% @end
%%--------------------------------------------------------------------
-spec insert({selector(), key()}, value() | {value(), value()}, array()) ->
    {ok, array()} | {error, out_of_space | already_exists}.
insert({_Selector, _Key}, _Value, #bp_tree_array{size = Size, data = Data})
    when Size == erlang:size(Data) div 2 ->
    {error, out_of_space};
insert({Selector, Key}, Value, Array = #bp_tree_array{size = Size}) ->
    Pos = lower_bound(Key, Array),
    case get({key, Pos}, Array) of
        {ok, Key} ->
            {error, already_exists};
        {ok, _} ->
            Array2 = shift_right(Pos, Array),
            {ok, Array3} = update({key, Pos}, Key, Array2),
            {ok, _Array4} = update({Selector, Pos}, Value, Array3);
        {error, out_of_range} ->
            Array2 = Array#bp_tree_array{size = Size + 1},
            {ok, Array3} = update({key, Size + 1}, Key, Array2),
            {ok, _Array4} = update({Selector, Size + 1}, Value, Array3)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Appends a key-value pair to an array.
%% @end
%%--------------------------------------------------------------------
-spec append({selector(), key()}, value() | {value(), value()}, array()) ->
    {ok, array()} | {error, out_of_space}.
append({_Selector, _Key}, _Value, #bp_tree_array{size = Size, data = Data})
    when Size == erlang:size(Data) div 2 ->
    {error, out_of_space};
append({Selector, Key}, Value, Array = #bp_tree_array{size = Size}) ->
    Array2 = Array#bp_tree_array{size = Size + 1},
    {ok, Array3} = update({key, Size + 1}, Key, Array2),
    {ok, _Array4} = update({Selector, Size + 1}, Value, Array3).

%%--------------------------------------------------------------------
%% @doc
%% Prepends a key-value pair to an array.
%% @end
%%--------------------------------------------------------------------
-spec prepend({selector(), key()}, value() | {value(), value()}, array()) ->
    {ok, array()} | {error, out_of_space}.
prepend({_Selector, _Key}, _Value, #bp_tree_array{size = Size, data = Data})
    when Size == erlang:size(Data) div 2 ->
    {error, out_of_space};
prepend({Selector, Key}, Value, Array = #bp_tree_array{}) ->
    Array2 = shift_right(1, Array),
    {ok, Array3} = update({key, 1}, Key, Array2),
    {ok, _Array4} = update({Selector, 1}, Value, Array3).

%%--------------------------------------------------------------------
%% @doc
%% Removes a key and associated value from an array.
%% @end
%%--------------------------------------------------------------------
-spec remove({selector(), key()}, array()) ->
    {ok, array()} | {error, term()}.
remove({Selector, Key}, Array = #bp_tree_array{}) ->
    remove({Selector, Key}, fun(_) -> true end, Array).

%%--------------------------------------------------------------------
%% @doc
%% Removes a key and associated value from an array if predicate is satisfied.
%% @end
%%--------------------------------------------------------------------
-spec remove({selector(), key()}, remove_pred(), array()) ->
    {ok, array()} | {error, term()}.
remove({Selector, Key}, Pred, Array = #bp_tree_array{}) ->
    case find(Key, Array) of
        {ok, Pos} ->
            {ok, Value} = get({Selector, Pos}, Array),
            case Pred(Value) of
                true when Selector =:= left -> {ok, shift_left(Pos, 0, Array)};
                true when Selector =:= right -> {ok, shift_left(Pos, 1, Array)};
                false -> {error, predicate_not_satisfied}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Splits an array in half. Returns left and right parts and a split key.
%% @end
%%--------------------------------------------------------------------
-spec split(array()) -> {array(), key(), array()}.
split(Array = #bp_tree_array{size = Size, data = Data}) ->
    Pivot = Size div 2 + 1,
    Begin = 2 * Pivot,
    SplitKey = element(Begin, Data),
    LData = setelement(Begin, Data, ?NIL),
    RData = erlang:make_tuple(erlang:size(Data), ?NIL),
    {LData3, RData3} = lists:foldl(fun(Pos, {LData2, RData2}) ->
        {
            setelement(Begin + Pos, LData2, ?NIL),
            setelement(Pos, RData2, element(Begin + Pos, LData2))
        }
    end, {LData, RData}, lists:seq(1, Size)),
    {
        Array#bp_tree_array{size = Pivot - 1, data = LData3},
        SplitKey,
        Array#bp_tree_array{size = Pivot - 1, data = RData3}
    }.

%%--------------------------------------------------------------------
%% @doc
%% Merges two arrays into a single array.
%% @end
%%--------------------------------------------------------------------
-spec merge(array(), array()) -> array().
merge(LArray = #bp_tree_array{size = LSize, data = LData},
    #bp_tree_array{size = RSize, data = RData}) ->
    Begin = 2 * LSize,
    LData2 = lists:foldl(fun(Pos, Data) ->
        setelement(Begin + Pos, Data, element(Pos, RData))
    end, LData, lists:seq(1, 2 * RSize + 1)),
    LArray#bp_tree_array{size = LSize + RSize, data = LData2}.

%%--------------------------------------------------------------------
%% @doc
%% Converts an array into a list.
%% @end
%%--------------------------------------------------------------------
-spec to_list(array()) -> list().
to_list(#bp_tree_array{data = Data}) ->
    tuple_to_list(Data).

%%--------------------------------------------------------------------
%% @doc
%% Converts a list into an array.
%% @end
%%--------------------------------------------------------------------
-spec from_list(list()) -> array().
from_list(List) ->
    Len = length(lists:takewhile(fun(X) -> X =/= ?NIL end, List)),
    #bp_tree_array{
        size = Len div 2,
        data = list_to_tuple(List)
    }.

%%--------------------------------------------------------------------
%% @doc
%% Converts an array into a map.
%% @end
%%--------------------------------------------------------------------
-spec to_map(array()) -> #{key() => value()}.
to_map(Array = #bp_tree_array{}) ->
    List = to_list(Array),
    to_map(List, #{?SIZE_KEY => length(List)}).

%%--------------------------------------------------------------------
%% @doc
%% Converts a map into an array.
%% @end
%%--------------------------------------------------------------------
-spec from_map(#{key() => value()}) -> array().
from_map(Map) ->
    Size = maps:get(?SIZE_KEY, Map),
    List = maps:fold(fun
        (?LAST_KEY, _, Acc) -> Acc;
        (?SIZE_KEY, _, Acc) -> Acc;
        (Key, Value, Acc) -> [{Key, Value} | Acc]
    end, [], Map),
    List2 = lists:sort(List),
    List3 = lists:foldl(fun({Key, Value}, Acc) ->
        [Key, Value | Acc]
    end, [], List2),
    List4 = [maps:get(?LAST_KEY, Map, ?NIL) | List3],
    List5 = lists:foldl(fun(_, Acc) ->
        [?NIL | Acc]
    end, List4, lists:seq(1, Size - length(List4))),
    from_list(lists:reverse(List5)).

%%====================================================================
%% Internal functions
%%====================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns position of a first key in range [Lower, Upper] that does not compare
%% less than a key.
%% @end
%%--------------------------------------------------------------------
-spec lower_bound(key(), pos_integer(), pos_integer(), array()) -> pos_integer().
lower_bound(Key, Lower, Upper, Array) when Lower =< Upper ->
    Mid = (Lower + Upper) div 2,
    {ok, MidKey} = get({key, Mid}, Array),
    case MidKey < Key of
        true -> lower_bound(Key, Mid + 1, Upper, Array);
        false -> lower_bound(Key, Lower, Mid - 1, Array)
    end;
lower_bound(_Key, Lower, _Upper, _Children) ->
    Lower.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes key and associated value at Begin position and shifts all following
%% items to the left, so that to fill the hole.
%% @end
%%--------------------------------------------------------------------
-spec shift_left(pos_integer(), non_neg_integer(), array()) -> array().
shift_left(Begin, Offset, Array = #bp_tree_array{size = Size, data = Data}) ->
    Data3 = lists:foldl(fun(Pos, Data2) ->
        setelement(Pos, Data2, element(Pos + 2, Data2))
    end, Data, lists:seq(2 * Begin - 1 + Offset, 2 * Size - 1)),
    Data4 = setelement(2 * Size, Data3, ?NIL),
    Data5 = setelement(2 * Size + 1, Data4, ?NIL),
    Array#bp_tree_array{
        size = Size - 1,
        data = Data5
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Shifts all keys by one position to the right starting from Begin position.
%% @end
%%--------------------------------------------------------------------
-spec shift_right(pos_integer(), array()) -> array().
shift_right(Begin, Array = #bp_tree_array{size = Size, data = Data}) ->
    Data3 = lists:foldl(fun(Pos, Data2) ->
        setelement(Pos, Data2, element(Pos - 2, Data2))
    end, Data, lists:seq(2 * Size + 3, 2 * Begin + 1, -1)),
    Array#bp_tree_array{
        size = Size + 1,
        data = Data3
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts list format of an array into a map.
%% @end
%%--------------------------------------------------------------------
-spec to_map(list(), maps:map()) -> maps:map().
to_map([], Map) ->
    Map;
to_map([?NIL], Map) ->
    Map;
to_map([Value], Map) ->
    Map#{?LAST_KEY => Value};
to_map([?NIL, ?NIL | _], Map) ->
    Map;
to_map([Value, ?NIL | _], Map) ->
    Map#{?LAST_KEY => Value};
to_map([Value, Key | List], Map) ->
    to_map(List, maps:put(Key, Value, Map)).