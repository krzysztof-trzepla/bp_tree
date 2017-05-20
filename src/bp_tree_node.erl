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
-export([new/2, next/2, find/2, insert/3, set_next/2]).

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
