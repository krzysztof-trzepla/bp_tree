%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides search functionality for leaf nodes of B+ tree.
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree_leaf).
-author("Krzysztof Trzepla").

-include("bp_tree.hrl").

%% API exports
-export([find_leftmost/1, find_key/2, find_next/3]).

-type find_next_result() :: {ok, pos_integer(), bp_tree:tree_node()} |
                            {error, not_found}.

%%====================================================================
%% API functions
%%====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns leftmost leaf in a B+ tree.
%% @end
%%--------------------------------------------------------------------
-spec find_leftmost(bp_tree:tree()) ->
    {{ok, bp_tree_node:id(), bp_tree:tree_node()} | {error, term()},
        bp_tree:tree()}.
find_leftmost(Tree) ->
    case bp_tree_store:get_root_id(Tree) of
        {{ok, RootId}, Tree2} -> find_leftmost(RootId, Tree2);
        {{error, Reason}, Tree2} -> {{error, Reason}, Tree2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns key at given offset starting from leftmost node. Along with the key
%% a node holding it and its position within this node is returned.
%% @end
%%--------------------------------------------------------------------
-spec find_key(non_neg_integer(), bp_tree:tree()) -> {
    {ok, bp_tree:key(), pos_integer(), bp_tree_node:id(), bp_tree:tree_node()} |
    {error, term()
    }, bp_tree:tree()}.
find_key(Offset, Tree) ->
    case find_leftmost(Tree) of
        {{ok, NodeId, Node}, Tree2} ->
            find_key(Offset, NodeId, Node, Tree2);
        {{error, Reason}, Tree2} ->
            {{error, Reason}, Tree2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns node and position of next key in B+ tree.
%% @end
%%--------------------------------------------------------------------
-spec find_next(bp_tree:key(), bp_tree_node:id(), bp_tree:tree()) ->
    {find_next_result(), bp_tree:tree()}.
find_next(Key, NodeId, Tree) ->
    find_next(Key, NodeId, Tree, []).

%%====================================================================
%% Internal functions
%%====================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns leftmost leaf in a B+ tree rooted in a node.
%% @end
%%--------------------------------------------------------------------
-spec find_leftmost(bp_tree_node:id(), bp_tree:tree()) ->
    {{ok, bp_tree_node:id(), bp_tree:tree_node()}, bp_tree:tree()}.
find_leftmost(NodeId, Tree) ->
    {{ok, Node}, Tree2} = bp_tree_store:get_node(NodeId, Tree),
    case bp_tree_node:leftmost_child(Node) of
        {ok, NodeId2} -> find_leftmost(NodeId2, Tree2);
        {error, not_found} -> {{ok, NodeId, Node}, Tree2}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns key at given offset starting from a node. Along with the key
%% a node holding it and its position within this node is returned.
%% @end
%%--------------------------------------------------------------------
-spec find_key(non_neg_integer(), bp_tree_node:id(), bp_tree:tree_node(),
    bp_tree:tree()) -> {{ok, bp_tree:key(), pos_integer(), bp_tree_node:id(),
    bp_tree:tree_node()} | {error, term()}, bp_tree:tree()}.
find_key(Offset, NodeId, Node, Tree) ->
    Size = bp_tree_node:size(Node),
    case Size =< Offset of
        true ->
            case bp_tree_node:right_sibling(Node) of
                {ok, NodeId2} ->
                    {{ok, Node2}, Tree2} = bp_tree_store:get_node(NodeId2, Tree),
                    find_key(Offset - Size, NodeId2, Node2, Tree2);
                {error, Reason} ->
                    {{error, Reason}, Tree}
            end;
        false ->
            {ok, Key} = bp_tree_node:key(Offset + 1, Node),
            {{ok, Key, Offset + 1, NodeId, Node}, Tree}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns node and position of next key in B+ tree. Builds a path of right
%% siblings of nodes along the path from a root node to a leaf associated
%% with a key.
%% @end
%%--------------------------------------------------------------------
-spec find_next(bp_tree:key(), bp_tree_node:id(), bp_tree:tree(),
    [bp_tree_node:id()]) -> {find_next_result(), bp_tree:tree()}.
find_next(Key, NodeId, Tree, Path) ->
    {{ok, Node}, Tree2} = bp_tree_store:get_node(NodeId, Tree),
    case bp_tree_node:child_with_right_sibling(Key, Node) of
        {ok, NodeId2, RNodeId} ->
            find_next(Key, NodeId2, Tree2, [RNodeId | Path]);
        {error, not_found} ->
            find_next_in_leaf(Key, Node, Tree, Path)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns node and position of next key in B+ tree leaf.
%% @end
%%--------------------------------------------------------------------
-spec find_next_in_leaf(bp_tree:key(), bp_tree:tree_node(), bp_tree:tree(),
    [bp_tree_node:id()]) -> {find_next_result(), bp_tree:tree()}.
find_next_in_leaf(Key, Node, Tree, Path) ->
    Pos = bp_tree_node:lower_bound(Key, Node),
    case bp_tree_node:value(Pos + 1, Node) of
        {ok, _} ->
            {{ok, Pos + 1, Node}, Tree};
        {error, out_of_range} ->
            Path2 = lists:dropwhile(fun(SNodeId) -> SNodeId == ?NIL end, Path),
            case Path2 of
                [] ->
                    {{error, not_found}, Tree};
                [SNodeId | _] ->
                    {{ok, _, NextNode}, Tree2} = find_leftmost(SNodeId, Tree),
                    {{ok, 1, NextNode}, Tree2}
            end
    end.