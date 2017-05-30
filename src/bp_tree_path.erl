%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides path search functionality for B+ tree.
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree_path).
-author("Krzysztof Trzepla").

-include("bp_tree.hrl").

%% API exports
-export([find/3, find_with_sibling/3]).

-type path() :: [{bp_tree_node:id(), bp_tree:tree_node()}].
-type path_with_sibling() :: [{
    {bp_tree_node:id(), bp_tree:tree_node()}, bp_tree:key(), bp_tree_node:id()
}].

-export_type([path/0, path_with_sibling/0]).

%%====================================================================
%% API functions
%%====================================================================

%%--------------------------------------------------------------------
%% @equiv find(Key, NodeId, Tree, [])
%% @end
%%--------------------------------------------------------------------
-spec find(bp_tree:key(), bp_tree_node:id(), bp_tree:tree()) -> 
    {path(), bp_tree:tree()}.
find(Key, NodeId, Tree) ->
    find(Key, NodeId, Tree, []).

%%--------------------------------------------------------------------
%% @equiv find_with_sibling(Key, NodeId, Tree, nil, nil, [])
%% @end
%%--------------------------------------------------------------------
-spec find_with_sibling(bp_tree:key(), bp_tree_node:id(), bp_tree:tree()) ->
    {path_with_sibling(), bp_tree:tree()}.
find_with_sibling(Key, NodeId, Tree) ->
    find_with_sibling(Key, NodeId, Tree, ?NIL, ?NIL, []).

%%====================================================================
%% Internal functions
%%====================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns leaf-root path for a key.
%% @end
%%--------------------------------------------------------------------
-spec find(bp_tree:key(), bp_tree_node:id(), bp_tree:tree(), path()) ->
    {path(), bp_tree:tree()}.
find(Key, NodeId, Tree, Path) ->
    {{ok, Node}, Tree2} = bp_tree_store:get_node(NodeId, Tree),
    Path2 = [{NodeId, Node} | Path],
    case bp_tree_node:child(Key, Node) of
        {ok, NodeId2} -> find(Key, NodeId2, Tree2, Path2);
        {error, not_found} -> {Path2, Tree2}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns leaf-root path for a key. Each node is associated with its sibling
%% and a parent key that both share.
%% @end
%%--------------------------------------------------------------------
-spec find_with_sibling(bp_tree:key(), bp_tree_node:id(), bp_tree:tree(),
    bp_tree:key(), bp_tree_node:id(), path_with_sibling()) ->
    {path_with_sibling(), bp_tree:tree()}.
find_with_sibling(Key, NodeId, Tree, ParentKey, SNodeId, Path) ->
    {{ok, Node}, Tree2} = bp_tree_store:get_node(NodeId, Tree),
    Path2 = [{{NodeId, Node}, ParentKey, SNodeId} | Path],
    case bp_tree_node:child_with_sibling(Key, Node) of
        {ok, LNodeId, Key2, RNodeId} when Key =< Key2 ->
            find_with_sibling(Key, LNodeId, Tree2, Key2, RNodeId, Path2);
        {ok, LNodeId, Key2, RNodeId} ->
            find_with_sibling(Key, RNodeId, Tree2, Key2, LNodeId, Path2);
        {error, not_found} ->
            {Path2, Tree2}
    end.