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
-module(bp_tree_children).
-author("Krzysztof Trzepla").

-include("bp_tree.hrl").

%% API exports
-export([new/0, size/1]).
-export([get/2, update_last_value/2, remove/2, remove/3]).
-export([find/2, find_value/2, lower_bound/2]).
-export([insert/3, append/3, prepend/3, split/1, merge/2]).
-export([to_map/1, from_map/1]).
-export([fold/4]).

%% For eunit tests
-export([to_list/1, from_list/1]).

-record(bp_tree_children, {
    last_value = ?NIL :: value(),
    data :: gb_trees:tree()
}).

-type key() :: any().
-type value() :: any().
-type selector() :: key | left | right | both | lower_bound | lower_bound_key.
-type pos() :: non_neg_integer() | first | last.
-type remove_pred() :: fun((value()) -> boolean()).
-opaque children() :: #bp_tree_children{}.

-export_type([children/0, selector/0]).

%%====================================================================
%% API functions
%%====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a new array.
%% @end
%%--------------------------------------------------------------------
-spec new() -> children().
new() ->
    #bp_tree_children{
        data = gb_trees:empty()
    }.

%%--------------------------------------------------------------------
%% @doc
%% Returns the size of an array.
%% @end
%%--------------------------------------------------------------------
-spec size(children()) -> non_neg_integer().
size(#bp_tree_children{data = Tree}) ->
    gb_trees:size(Tree).

%%--------------------------------------------------------------------
%% @doc
%% Returns an item..
%% @end
%%--------------------------------------------------------------------
-spec get({selector(), pos()}, children()) ->
    {ok, value() | {value(), value()}} | {error, out_of_range}.
get({lower_bound, Key}, #bp_tree_children{data = Tree}) ->
    It =  gb_trees:iterator_from(Key, Tree),
    case gb_trees:next(It) of
        {_, Value, _} ->
            {ok, Value};
        none ->
            {error, out_of_range}
    end;
get({lower_bound_key, Key}, #bp_tree_children{data = Tree}) ->
    It =  gb_trees:iterator_from(Key, Tree),
    case gb_trees:next(It) of
        {LKey, _, _} ->
            {ok, LKey};
        none ->
            {error, out_of_range}
    end;
get({left, first}, #bp_tree_children{data = Tree}) ->
    case gb_trees:is_empty(Tree) of
        true ->
            {error, out_of_range};
        _ ->
            {_K, V} = gb_trees:smallest(Tree),
            {ok, V}
    end;
get({key, first}, #bp_tree_children{data = Tree}) ->
    case gb_trees:is_empty(Tree) of
        true ->
            {error, out_of_range};
        _ ->
            {K, _V} = gb_trees:smallest(Tree),
            {ok, K}
    end;
get({left, last}, #bp_tree_children{data = Tree}) ->
    case gb_trees:is_empty(Tree) of
        true ->
            {error, out_of_range};
        _ ->
            {_K, V} = gb_trees:largest(Tree),
            {ok, V}
    end;
get({key, last}, #bp_tree_children{data = Tree}) ->
    case gb_trees:is_empty(Tree) of
        true ->
            {error, out_of_range};
        _ ->
            {K, _V} = gb_trees:largest(Tree),
            {ok, K}
    end;
get({both, last}, #bp_tree_children{data = Tree, last_value = LV}) ->
    case gb_trees:is_empty(Tree) of
        true ->
            {error, out_of_range};
        _ ->
            {_K, V} = gb_trees:largest(Tree),
            {ok, {V, LV}}
    end;
get({right, last}, #bp_tree_children{last_value = LV}) ->
    {ok, LV};
get({right, 0}, #bp_tree_children{data = Tree, last_value = LV}) ->
    case gb_trees:is_empty(Tree) of
        true ->
            {ok, LV};
        _ ->
            {_K, V} = gb_trees:smallest(Tree),
            {ok, V}
    end;
get({left, Pos}, #bp_tree_children{data = Tree}) ->
    case get_pos(Pos, Tree) of
        {_Key, Value, _It} ->
            {ok, Value};
        Error ->
            Error
    end;
get({key, Pos}, #bp_tree_children{data = Tree}) ->
    case get_pos(Pos, Tree) of
        {Key, _Value, _It} ->
            {ok, Key};
        Error ->
            Error
    end;
get({right, Pos}, #bp_tree_children{data = Tree, last_value = LV}) ->
    case get_pos(Pos, Tree) of
        {_Key, _Value, It} ->
            case gb_trees:next(It) of
                none ->
                    {ok, LV};
                {_Key2, Value2, _It2} ->
                    {ok, Value2}
            end;
        Error ->
            Error
    end;
get({both, Pos}, #bp_tree_children{data = Tree, last_value = LV}) ->
    case get_pos(Pos, Tree) of
        {_Key, Value, It} ->
            case gb_trees:next(It) of
                none ->
                    {ok, {Value, LV}};
                {_Key2, Value2, _It2} ->
                    {ok, {Value, Value2}}
            end;
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates value for last key.
%% @end
%%--------------------------------------------------------------------
-spec update_last_value(value(), children()) ->
    {ok, children()} | {error, out_of_range}.
update_last_value(Value, #bp_tree_children{} = Children) ->
    {ok, Children#bp_tree_children{last_value = Value}}.

%%--------------------------------------------------------------------
%% @doc
%% Returns position of a key or fails with a missing error.
%% @end
%%--------------------------------------------------------------------
-spec find(key(), children()) -> {ok, pos_integer()} | {error, not_found}.
find(Key, #bp_tree_children{data = Tree}) ->
    find(Key, gb_trees:iterator(Tree), 1).

%%--------------------------------------------------------------------
%% @doc
%% Returns value for a key or fails with a missing error.
%% @end
%%--------------------------------------------------------------------
-spec find_value(key(), children()) -> {ok, value()} | {error, not_found}.
find_value(Key, #bp_tree_children{data = Tree}) ->
    It = gb_trees:iterator_from(Key, Tree),
    case gb_trees:next(It) of
        {Key, Value, _} -> {ok, Value};
        _ -> {error, not_found}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv lower_bound(Key, Tree, 1)
%% @end
%%--------------------------------------------------------------------
-spec lower_bound(key(), children()) -> pos_integer().
lower_bound(Key, #bp_tree_children{data = Tree}) ->
    lower_bound(Key, Tree, 1).

%%--------------------------------------------------------------------
%% @doc
%% Inserts a key-value pair.
%% @end
%%--------------------------------------------------------------------
-spec insert({selector(), key()}, value() | {value(), value()}, children()) ->
    {ok, children()} | {error, out_of_space | already_exists}.
insert({Selector, Key}, Value0, #bp_tree_children{data = Tree} = Children) ->
    It = gb_trees:iterator_from(Key, Tree),
    case gb_trees:next(It) of
        {Key, _OldValue, _} ->
            {error, already_exists};
        {NextKey, _, _} ->
            case Selector of
                both ->
                    {Value, NextValue} = Value0,
                    Tree2 = gb_trees:insert(Key, Value, Tree),
                    Tree3 = gb_trees:enter(NextKey, NextValue, Tree2),
                    {ok, Children#bp_tree_children{data = Tree3}};
                _ ->
                    Tree2 = gb_trees:insert(Key, Value0, Tree),
                    {ok, Children#bp_tree_children{data = Tree2}}
            end;
        none ->
            case Selector of
                both ->
                    {Value, NextValue} = Value0,
                    Tree2 = gb_trees:insert(Key, Value, Tree),
                    {ok, Children#bp_tree_children{data = Tree2,
                        last_value = NextValue}};
                _ ->
                    Tree2 = gb_trees:insert(Key, Value0, Tree),
                    {ok, Children#bp_tree_children{data = Tree2}}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Appends a key, value or key-value pair.
%% @end
%%--------------------------------------------------------------------
-spec append({selector(), key()}, value() | {value(), value()}, children()) ->
    {ok, children()} | {error, out_of_space}.
append({key, Key}, Key, #bp_tree_children{data = Tree, last_value = LV} = Children) ->
    Tree2 = gb_trees:enter(Key, LV, Tree),
    {ok, Children#bp_tree_children{data = Tree2, last_value = ?NIL}};
append({right, Key}, Value, #bp_tree_children{data = Tree, last_value = LV} = Children) ->
    case gb_trees:is_empty(Tree) of
        true ->
            Tree2 = gb_trees:insert(Key, LV, Tree),
            {ok, Children#bp_tree_children{data = Tree2, last_value = Value}};
        _ ->
            Tree2 = gb_trees:enter(Key, LV, Tree),
            {ok, Children#bp_tree_children{data = Tree2, last_value = Value}}
    end;
append({both, Key}, {Value, Next}, #bp_tree_children{data = Tree} = Children) ->
    Tree2 = gb_trees:enter(Key, Value, Tree),
    {ok, Children#bp_tree_children{data = Tree2, last_value = Next}}.

%%--------------------------------------------------------------------
%% @doc
%% Prepends a key-value pair.
%% @end
%%--------------------------------------------------------------------
-spec prepend(key(), value() | {value(), value()}, children()) ->
    {ok, children()} | {error, out_of_space}.
prepend(Key, Value, #bp_tree_children{data = Tree} = Children) ->
    Tree2 = gb_trees:insert(Key, Value, Tree),
    {ok, Children#bp_tree_children{data = Tree2}}.

%%--------------------------------------------------------------------
%% @doc
%% Removes a key and associated value.
%% @end
%%--------------------------------------------------------------------
-spec remove({selector(), key()}, children()) ->
    {ok, children()} | {error, term()}.
remove({Selector, Key}, #bp_tree_children{} = Children) ->
    remove({Selector, Key}, fun(_) -> true end, Children).

%%--------------------------------------------------------------------
%% @doc
%% Removes a key and associated value if predicate is satisfied.
%% @end
%%--------------------------------------------------------------------
-spec remove({selector(), key()}, remove_pred(), children()) ->
    {ok, children()} | {error, term()}.
remove({Selector, Key}, Pred, #bp_tree_children{data = Tree} = Children) ->
    It = gb_trees:iterator_from(Key, Tree),
    case gb_trees:next(It) of
        {Key, Value, It2} ->
            case Pred(Value) of
                true ->
                    Tree2 = gb_trees:delete(Key, Tree),
                    case Selector of
                        left ->
                            {ok, Children#bp_tree_children{data = Tree2}};
                        right ->
                            case gb_trees:next(It2) of
                                {Key2, _, _} ->
                                    Tree3 = gb_trees:enter(Key2, Value, Tree2),
                                    {ok, Children#bp_tree_children{data = Tree3}};
                                _ ->
                                    {ok, Children#bp_tree_children{data = Tree2,
                                        last_value = Value}}
                            end
                    end;
                false ->
                    {error, predicate_not_satisfied}
            end;
        _ ->
            {error, not_found}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Splits children record in half. Returns left and right parts and a split key.
%% @end
%%--------------------------------------------------------------------
-spec split(children()) -> {children(), key(), children()}.
split(#bp_tree_children{data = Tree} = Children) ->
    Size = gb_trees:size(Tree),
    SplitBase = Size div 2,
    SplitSize = SplitBase + 1,
    List = gb_trees:to_list(Tree),
    Left = lists:sublist(List, SplitBase),
    [{SplitKey, SplitValue} | Right] = lists:sublist(List, SplitSize, SplitSize),
    {
        #bp_tree_children{data = gb_trees:from_orddict(Left), last_value = SplitValue},
        SplitKey,
        Children#bp_tree_children{data = gb_trees:from_orddict(Right)}
    }.

%%--------------------------------------------------------------------
%% @doc
%% Merges two children records into a single array.
%% @end
%%--------------------------------------------------------------------
-spec merge(children(), children()) -> children().
merge(#bp_tree_children{data = LTree}, #bp_tree_children{data = RTree} = Children) ->
    LList = gb_trees:to_list(LTree),
    RList = gb_trees:to_list(RTree),
    Children#bp_tree_children{data = gb_trees:from_orddict(LList ++ RList)}.

%%--------------------------------------------------------------------
%% @doc
%% Converts an array into a map.
%% @end
%%--------------------------------------------------------------------
-spec to_map(children()) -> #{key() => value()}.
to_map(#bp_tree_children{data = Tree, last_value = LV}) ->
    Map1 = maps:from_list(gb_trees:to_list(Tree)),
    case LV of
        ?NIL -> Map1;
        _ -> Map1#{?LAST_KEY => LV}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Converts a map into an array.
%% @end
%%--------------------------------------------------------------------
-spec from_map(#{key() => value()}) -> children().
from_map(Map) ->
    LV = maps:get(?LAST_KEY, Map, ?NIL),
    Map2 = maps:remove(?LAST_KEY, Map),
    Tree = gb_trees:from_orddict(lists:sort(maps:to_list(Map2))),
    #bp_tree_children{data = Tree, last_value = LV}.

%%--------------------------------------------------------------------
%% @doc
%% Folds children.
%% @end
%%--------------------------------------------------------------------
-spec fold(bp_tree:fold_start_spec(), children(),
    bp_tree:fold_fun(), bp_tree:fold_acc()) -> bp_tree:fold_acc().
fold({key, Key}, #bp_tree_children{data = Tree}, Fun, Acc) ->
    It = gb_trees:iterator_from(Key, Tree),
    fold_helper(It, Fun, Acc);
fold({pos, Pos}, #bp_tree_children{data = Tree}, Fun, Acc) ->
    case get_pos(Pos, Tree) of
        {Key, Value, It} ->
            Acc2 = Fun(Key, Value, Acc),
            fold_helper(It, Fun, Acc2);
        Error ->
            Error
    end;
fold(all, #bp_tree_children{data = Tree}, Fun, Acc) ->
    It = gb_trees:iterator(Tree),
    fold_helper(It, Fun, Acc).

%%====================================================================
%% Functions for eunit tests
%%====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Converts an array into a list.
%% @end
%%--------------------------------------------------------------------
-spec to_list(children()) -> list().
to_list(#bp_tree_children{data = Tree, last_value = LV}) ->
    List1 = lists:foldl(fun({K, V}, Acc) ->
        [K, V | Acc]
    end, [], gb_trees:to_list(Tree)),
    case LV of
        ?NIL -> lists:reverse(List1);
        _ -> lists:reverse([LV | List1])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Converts a list into an array.
%% @end
%%--------------------------------------------------------------------
-spec from_list(list()) -> children().
from_list(List) ->
    {List2, LV} = lists:foldl(fun
        (Element, {Acc, ?NIL}) ->
            {Acc, Element};
        (Key, {Acc, Value}) ->
            {[{Key, Value} | Acc], ?NIL}
    end, {[], ?NIL}, List),
    Tree = gb_trees:from_orddict(lists:reverse(List2)),
    #bp_tree_children{data = Tree, last_value = LV}.

%%====================================================================
%% Internal functions
%%====================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Folds children using iterator.
%% @end
%%--------------------------------------------------------------------
-spec fold_helper(gb_trees:iter(),
    bp_tree:fold_fun(), bp_tree:fold_acc()) -> bp_tree:fold_acc().
fold_helper(It, Fun, Acc) ->
    case gb_trees:next(It) of
        {Key, Value, It2} ->
            Acc2 = Fun(Key, Value, Acc),
            fold_helper(It2, Fun, Acc2);
        _ ->
            Acc
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns position of a key or fails with a missing error.
%% @end
%%--------------------------------------------------------------------
-spec find(key(), gb_trees:iter(), pos_integer()) ->
    {ok, pos_integer()} | {error, not_found}.
find(Key, It, Pos) ->
    case gb_trees:next(It) of
        none ->
            {error, not_found};
        {Key2, _Value, It2} ->
            case {Key2 =:= Key, Key2 < Key} of
                {true, _} -> {ok, Pos};
                {_, true} -> find(Key, It2, Pos + 1);
                _ -> {error, not_found}
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a position of a first key that does not compare less
%% than a key.
%% @end
%%--------------------------------------------------------------------
-spec lower_bound(key(), gb_trees:tree(), pos_integer()) -> pos_integer().
lower_bound(Key, Tree, Pos) ->
    case gb_trees:is_empty(Tree) of
        true ->
            Pos;
        _ ->
            {Key2, _Value, Tree2} = gb_trees:take_smallest(Tree),
            case Key2 >= Key of
                true -> Pos;
                _ -> lower_bound(Key, Tree2, Pos + 1)
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns {Key, Value, Iterator} for position.
%% @end
%%--------------------------------------------------------------------
-spec get_pos(pos_integer(), gb_trees:tree()) ->
    {key(), value(), gb_trees:iter()} | {error, out_of_range}.
get_pos(Pos, Tree) ->
    get_pos(gb_trees:iterator(Tree), Pos, 0, {error, out_of_range}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns {Key, Value, Iterator} for position.
%% @end
%%--------------------------------------------------------------------
-spec get_pos(gb_trees:iter(), pos_integer(), non_neg_integer(),
    {key(), value(), gb_trees:iter()} | {error, out_of_range}) ->
    {key(), value(), gb_trees:iter()} | {error, out_of_range}.
get_pos(_It, Pos, CurrentPos, Ans) when CurrentPos >= Pos ->
    Ans;
get_pos(It, Pos, CurrentPos, _TmpAns) ->
    case gb_trees:next(It) of
        none ->
            {error, out_of_range};
        {_Key, _Value, It2} = Ans ->
            get_pos(It2, Pos, CurrentPos + 1, Ans)
    end.