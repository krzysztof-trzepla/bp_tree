%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides B+ tree node management functions.
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree_node).
-author("Krzysztof Trzepla").

-include("bp_tree.hrl").

%% API exports
-export([new/2]).
-export([key/2, value/2, size/1]).
-export([next/2, next_leftmost/1, right_sibling/1, find/2, lower_bound/2]).
-export([insert/3, set_next/2]).

-type id() :: any().

-export_type([id/0]).

%%====================================================================
%% API functions
%%====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates B+ tree node.
%% @end
%%--------------------------------------------------------------------
-spec new(bp_tree:order(), boolean()) -> bp_tree:tree_node().
new(Order, Leaf) ->
    #bp_tree_node{
        leaf = Leaf,
        children = bp_tree_array:new(2 * Order + 1)
    }.

%%--------------------------------------------------------------------
%% @doc
%% Returns next node on path from root to leaf associated with provided key.
%% @end
%%--------------------------------------------------------------------
-spec next(bp_tree:key(), bp_tree:tree_node()) ->
    {ok, bp_tree_node:id()} | {error, not_found}.
next(_Key, #bp_tree_node{leaf = true}) ->
    {error, not_found};
next(Key, #bp_tree_node{leaf = false, children = Children}) ->
    Pos = bp_tree_array:lower_bound(Key, Children),
    case bp_tree_array:left(Pos, Children) of
        {ok, NodeId} ->
            {ok, NodeId};
        {error, out_of_range} ->
            Size = bp_tree_array:size(Children),
            {ok, NodeId} = bp_tree_array:right(Size, Children),
            {ok, NodeId}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns next leftmost node on path from root to leaf.
%% @end
%%--------------------------------------------------------------------
-spec next_leftmost(bp_tree:tree_node()) ->
    {ok, bp_tree_node:id()} | {error, not_found}.
next_leftmost(#bp_tree_node{leaf = true}) ->
    {error, not_found};
next_leftmost(#bp_tree_node{leaf = false, children = Children}) ->
    bp_tree_array:left(1, Children).

%%--------------------------------------------------------------------
%% @doc
%% Returns right sibling of a leaf node.
%% @end
%%--------------------------------------------------------------------
-spec right_sibling(bp_tree:tree_node()) ->
    {ok, bp_tree_node:id()} | {error, not_found}.
right_sibling(#bp_tree_node{leaf = true, children = Children}) ->
    Size = bp_tree_array:size(Children),
    case bp_tree_array:right(Size, Children) of
        {ok, NodeId} -> {ok, NodeId};
        {error, out_of_range} -> {error, not_found}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns value at given position or ID of right sibling that holds next
%% values.
%% @end
%%--------------------------------------------------------------------
-spec key(pos_integer(), bp_tree:tree_node()) ->
    {ok, bp_tree:value()} | {next, bp_tree_node:id()} | {error, not_found}.
key(Pos, #bp_tree_node{leaf = true, children = Children}) ->
    get(fun bp_tree_array:key/2, Pos, Children).

%%--------------------------------------------------------------------
%% @doc
%% Returns value at given position or ID of right sibling that holds next
%% values.
%% @end
%%--------------------------------------------------------------------
-spec value(pos_integer(), bp_tree:tree_node()) ->
    {ok, bp_tree:value()} | {next, bp_tree_node:id()} | {error, not_found}.
value(Pos, #bp_tree_node{leaf = true, children = Children}) ->
    get(fun bp_tree_array:left/2, Pos, Children).

%%--------------------------------------------------------------------
%% @doc
%% Returns number of node's children.
%% @end
%%--------------------------------------------------------------------
-spec size(bp_tree:tree_node()) -> non_neg_integer().
size(#bp_tree_node{children = Children}) ->
    bp_tree_array:size(Children).

%%--------------------------------------------------------------------
%% @doc
%% Returns value associated with provided key from leaf node or fails with
%% a missing error.
%% @end
%%--------------------------------------------------------------------
-spec find(bp_tree:key(), bp_tree:tree_node()) ->
    {ok, bp_tree:value()} | {error, not_found}.
find(Key, #bp_tree_node{leaf = true, children = Children}) ->
    case bp_tree_array:find(Key, Children) of
        {ok, Pos} -> bp_tree_array:left(Pos, Children);
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns position of a first key that does not compare less than provided key.
%% @end
%%--------------------------------------------------------------------
-spec lower_bound(bp_tree:key(), bp_tree:tree_node()) -> pos_integer().
lower_bound(Key, #bp_tree_node{leaf = true, children = Children}) ->
    bp_tree_array:lower_bound(Key, Children).

%%--------------------------------------------------------------------
%% @doc
%% Inserts key-value pair into a node. Returns {ok, Node} if insert operation
%% succeeded and didn't cause node split. In case of node split
%% {ok, LeftNode, Key, RightNode} is returned, where LeftNode is updated
%% original node, RightNode is newly created one and Key is a key that should be
%% inserted into parent node.
%% @end
%%--------------------------------------------------------------------
-spec insert(bp_tree:key(), {bp_tree:value(), bp_tree:value()},
    bp_tree:tree_node()) -> {ok, bp_tree:tree_node()} |
    {ok, bp_tree:tree_node(), bp_tree:key(), bp_tree:tree_node()} |
    {error, term()}.
insert(Key, Value, Node = #bp_tree_node{children = Children}) ->
    case bp_tree_array:insert(Key, Value, Children) of
        {ok, Children2} -> split(Node#bp_tree_node{children = Children2});
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% In case of leaf sets the ID of its right sibling, otherwise does nothing.
%% @end
%%--------------------------------------------------------------------
-spec set_next(bp_tree_node:id(), bp_tree:tree_node()) -> bp_tree:tree_node().
set_next(NodeId, Node = #bp_tree_node{leaf = true, children = Children}) ->
    Size = bp_tree_array:size(Children),
    {ok, Children2} = bp_tree_array:update_right(Size, NodeId, Children),
    Node#bp_tree_node{children = Children2};
set_next(_NodeId, Node = #bp_tree_node{leaf = false}) ->
    Node.

%%====================================================================
%% Internal functions
%%====================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Splits node in half if it reached its maximum size. Returns updated
%% original node, newly created one and a key that should be inserted into
%% parent node.
%% @end
%%--------------------------------------------------------------------
-spec split(bp_tree:tree_node()) -> {ok, bp_tree:tree_node()} |
    {ok, bp_tree:tree_node(), bp_tree:key(), bp_tree:tree_node()}.
split(LNode = #bp_tree_node{leaf = true, children = Children}) ->
    case bp_tree_array:full(Children) of
        true ->
            {LChildren, Key, RChildren} = bp_tree_array:split(Children),
            {ok, LChildren2} = bp_tree_array:append(
                Key, {undefined, undefined}, LChildren
            ),
            RNode = #bp_tree_node{leaf = true, children = RChildren},
            {ok, LNode#bp_tree_node{children = LChildren2}, Key, RNode};
        false ->
            {ok, LNode}
    end;
split(LNode = #bp_tree_node{leaf = false, children = Children}) ->
    case bp_tree_array:full(Children) of
        true ->
            {LChildren, Key, RChildren} = bp_tree_array:split(Children),
            RNode = #bp_tree_node{leaf = false, children = RChildren},
            {ok, LNode#bp_tree_node{children = LChildren}, Key, RNode};
        false ->
            {ok, LNode}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns key/value at given position or ID of right sibling that holds next
%% keys/values.
%% @end
%%--------------------------------------------------------------------
-spec get(fun(), pos_integer(), bp_tree_array:array()) ->
    {ok, bp_tree:value()} | {next, bp_tree_node:id()} | {error, not_found}.
get(Fun, Pos, Children) ->
    case Fun(Pos, Children) of
        {ok, Value} -> {ok, Value};
        {error, out_of_range} ->
            Size = bp_tree_array:size(Children),
            case bp_tree_array:right(Size, Children) of
                {ok, NodeId} -> {next, NodeId};
                {error, out_of_range} -> {error, not_found}
            end
    end.
