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

%% API exports
-export([new/1, size/1, full/1]).
-export([key/2, left/2, right/2]).
-export([update_left/3, update_right/3]).
-export([find/2, lower_bound/2]).
-export([insert/3, append/3, erase/2, split/1]).
-export([to_list/1, from_list/1]).

-record(bp_tree_array, {
    size,
    data
}).

-type key() :: any().
-type value() :: any().
-opaque array() :: #bp_tree_array{}.

-export_type([array/0]).

-define(NIL, nil).

%%====================================================================
%% API functions
%%====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates new array.
%% @end
%%--------------------------------------------------------------------
-spec new(pos_integer()) -> array().
new(Size) ->
    #bp_tree_array{
        size = 0,
        data = list_to_tuple(lists:duplicate(2 * Size + 1, ?NIL))
    }.

%%--------------------------------------------------------------------
%% @doc
%% Returns array size.
%% @end
%%--------------------------------------------------------------------
-spec size(array()) -> non_neg_integer().
size(#bp_tree_array{size = Size}) ->
    Size.

%%--------------------------------------------------------------------
%% @doc
%% Returns true if array size reached its maximum, otherwise false.
%% @end
%%--------------------------------------------------------------------
-spec full(array()) -> boolean().
full(#bp_tree_array{size = Size, data = Data}) ->
    Size == (erlang:size(Data) div 2).

%%--------------------------------------------------------------------
%% @doc
%% Returns key at given position.
%% @end
%%--------------------------------------------------------------------
-spec key(pos_integer(), array()) -> {ok, key()} | {error, out_of_range}.
key(Pos, Array = #bp_tree_array{}) ->
    at(Pos, 0, Array).

%%--------------------------------------------------------------------
%% @doc
%% Returns left value at given position.
%% @end
%%--------------------------------------------------------------------
-spec left(pos_integer(), array()) -> {ok, value()} | {error, out_of_range}.
left(Pos, Array = #bp_tree_array{}) ->
    at(Pos, -1, Array).

%%--------------------------------------------------------------------
%% @doc
%% Returns right value at given position.
%% @end
%%--------------------------------------------------------------------
-spec right(pos_integer(), array()) -> {ok, value()} | {error, out_of_range}.
right(Pos, Array = #bp_tree_array{}) ->
    at(Pos, 1, Array).

%%--------------------------------------------------------------------
%% @doc
%% Updates left value at given position.
%% @end
%%--------------------------------------------------------------------
-spec update_left(pos_integer(), value(), array()) ->
    {ok, array()} | {error, out_of_range}.
update_left(Pos, Value, Array = #bp_tree_array{}) ->
    update(Pos, -1, Value, Array).

%%--------------------------------------------------------------------
%% @doc
%% Updates right value at given position.
%% @end
%%--------------------------------------------------------------------
-spec update_right(pos_integer(), value(), array()) ->
    {ok, array()} | {error, out_of_range}.
update_right(Pos, Value, Array = #bp_tree_array{}) ->
    update(Pos, 1, Value, Array).

%%--------------------------------------------------------------------
%% @doc
%% Returns position of given key or fails with missing error.
%% @end
%%--------------------------------------------------------------------
-spec find(key(), array()) -> {ok, pos_integer()} | {error, not_found}.
find(Key, Array = #bp_tree_array{}) ->
    Pos = lower_bound(Key, Array),
    case key(Pos, Array) of
        {ok, Key} -> {ok, Pos};
        {ok, _} -> {error, not_found};
        {error, out_of_range} -> {error, not_found}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns position of a first key that does not compare less than provided key.
%% @end
%%--------------------------------------------------------------------
-spec lower_bound(key(), array()) -> pos_integer().
lower_bound(Key, Array = #bp_tree_array{size = Size}) ->
    lower_bound(Key, 1, Size, Array).

%%--------------------------------------------------------------------
%% @doc
%% Inserts key-value pair into an array.
%% @end
%%--------------------------------------------------------------------
-spec insert(key(), {value(), value()}, array()) ->
    {ok, array()} | {error, out_of_space | already_exists}.
insert(_Key, _Value, #bp_tree_array{size = Size, data = Data})
    when Size == erlang:size(Data) div 2 ->
    {error, out_of_space};
insert(Key, Value, Array = #bp_tree_array{size = Size}) ->
    Pos = lower_bound(Key, Array),
    case key(Pos, Array) of
        {ok, Key} ->
            {error, already_exists};
        {ok, _} ->
            Array2 = shift_right(Pos, Array),
            {ok, Array3} = set_key(Pos, Key, Array2),
            {ok, _Array4} = set_value(Pos, Value, Array3);
        {error, out_of_range} ->
            Array2 = Array#bp_tree_array{size = Size + 1},
            {ok, Array3} = set_key(Size + 1, Key, Array2),
            {ok, _Array4} = set_value(Size + 1, Value, Array3)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Appends key-value pair to an array.
%% @end
%%--------------------------------------------------------------------
-spec append(key(), {value(), value()}, array()) ->
    {ok, array()} | {error, out_of_space}.
append(_Key, _Value, #bp_tree_array{size = Size, data = Data})
    when Size == erlang:size(Data) div 2 ->
    {error, out_of_space};
append(Key, Value, Array = #bp_tree_array{size = Size}) ->
    Array2 = Array#bp_tree_array{size = Size + 1},
    {ok, Array3} = set_key(Size + 1, Key, Array2),
    {ok, _Array4} = set_value(Size + 1, Value, Array3).

%%--------------------------------------------------------------------
%% @doc
%% Removes key-value pair from an array.
%% @end
%%--------------------------------------------------------------------
-spec erase(key(), array()) -> {ok, array()} | {error, not_found}.
erase(Key, Array = #bp_tree_array{}) ->
    case find(Key, Array) of
        {ok, Pos} -> {ok, shift_left(Pos, Array)};
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Splits an array in half. Returns left, right part and a split key.
%% @end
%%--------------------------------------------------------------------
-spec split(array()) -> {array(), key(), array()}.
split(#bp_tree_array{size = Size, data = Data}) ->
    Pivot = Size div 2 + 1,
    {Left, [SplitKey | Right]} = lists:split(2 * Pivot - 1, tuple_to_list(Data)),
    Left2 = Left ++ lists:duplicate(erlang:size(Data) - length(Left), ?NIL),
    Right2 = Right ++ lists:duplicate(erlang:size(Data) - length(Right), ?NIL),
    {
        #bp_tree_array{size = length(Left) div 2, data = list_to_tuple(Left2)},
        SplitKey,
        #bp_tree_array{size = length(Right) div 2, data = list_to_tuple(Right2)}
    }.

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

%%====================================================================
%% Internal functions
%%====================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns key or value at given position + offset.
%% @end
%%--------------------------------------------------------------------
-spec at(pos_integer(), integer(), array()) ->
    {ok, key() | value()} | {error, out_of_range}.
at(Pos, Offset, #bp_tree_array{size = Size, data = Data})
    when 1 =< Pos andalso Pos =< Size ->
    {ok, element(2 * Pos + Offset, Data)};
at(_Pos, _Offset, #bp_tree_array{}) ->
    {error, out_of_range}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates key or value at given position + offset.
%% @end
%%--------------------------------------------------------------------
-spec update(pos_integer(), integer(), key() | value(), array()) ->
    {ok, array()} | {error, out_of_range}.
update(Pos, Offset, Value, Array = #bp_tree_array{size = Size, data = Data})
    when 1 =< Pos andalso Pos =< Size ->
    {ok, Array#bp_tree_array{data = setelement(2 * Pos + Offset, Data, Value)}};
update(_Pos, _Offset, _Value, #bp_tree_array{}) ->
    {error, out_of_range}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets key at given position.
%% @end
%%--------------------------------------------------------------------
-spec set_key(pos_integer(), key(), array()) ->
    {ok, array()} | {error, out_of_range}.
set_key(Pos, Key, Array = #bp_tree_array{}) ->
    update(Pos, 0, Key, Array).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets value at given position.
%% @end
%%--------------------------------------------------------------------
-spec set_value(pos_integer(), {value(), value()}, array()) ->
    {ok, array()} | {error, out_of_range}.
set_value(_Pos, {undefined, undefined}, Array = #bp_tree_array{}) ->
    {ok, Array};
set_value(Pos, {LValue, undefined}, Array = #bp_tree_array{}) ->
    update(Pos, -1, LValue, Array);
set_value(Pos, {undefined, RValue}, Array = #bp_tree_array{}) ->
    update(Pos, 1, RValue, Array);
set_value(Pos, {LValue, RValue}, Array = #bp_tree_array{}) ->
    case update(Pos, -1, LValue, Array) of
        {ok, Array2} -> update(Pos, 1, RValue, Array2);
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns position of a first key that does not compare less than provided key
%% in range [Lower, Upper].
%% @end
%%--------------------------------------------------------------------
-spec lower_bound(key(), pos_integer(), pos_integer(), array()) -> pos_integer().
lower_bound(Key, Lower, Upper, Array) when Lower =< Upper ->
    Mid = (Lower + Upper) div 2,
    {ok, MidKey} = key(Mid, Array),
    case MidKey < Key of
        true -> lower_bound(Key, Mid + 1, Upper, Array);
        false -> lower_bound(Key, Lower, Mid - 1, Array)
    end;
lower_bound(_Key, Lower, _Upper, _Children) ->
    Lower.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes key and its right value at Begin position and shifts all following
%% elements to the left, so that to fill the created hole.
%% @end
%%--------------------------------------------------------------------
-spec shift_left(pos_integer(), array()) -> array().
shift_left(Begin, Array = #bp_tree_array{size = Size, data = Data}) ->
    Data3 = lists:foldl(fun(Pos, Data2) ->
        setelement(Pos, Data2, element(Pos + 2, Data2))
    end, Data, lists:seq(2 * Begin - 1, 2 * Size - 1)),
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