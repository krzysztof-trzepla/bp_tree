%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides an API for a B+ tree.
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree).
-author("Krzysztof Trzepla").

-include("bp_tree.hrl").

%% API exports
-export([new/0, new/1]).
-export([find/2, insert/3]).

-type key() :: term().
-type value() :: term().
-type tree() :: #bp_tree{}.
-type tree_node() :: #bp_tree_node{}.
-type order() :: pos_integer().
-type option() :: {order, order()} |
                  {store_module, module()} |
                  {store_args, bp_tree_store:args()}.
-type path() :: [{bp_tree_node:id(), tree_node()}].
-type error() :: {error, Reason :: term()}.
-type error_stacktrace() :: {error, Reason :: term(), [erlang:stack_item()]}.

-export_type([key/0, value/0, tree/0, tree_node/0, order/0, option/0]).

%%====================================================================
%% API functions
%%====================================================================

%%--------------------------------------------------------------------
%% @equiv new([])
%% @end
%%--------------------------------------------------------------------
-spec new() -> {ok, tree()} | error().
new() ->
    new([]).

%%--------------------------------------------------------------------
%% @doc
%% Creates new B+ tree.
%% @end
%%--------------------------------------------------------------------
-spec new([option()]) -> {ok, tree()} | error().
new(Opts) ->
    Order = proplists:get_value(order, Opts, 50),
    Module = proplists:get_value(store_module, Opts, bp_tree_map_store),
    Args = proplists:get_value(store_args, Opts, []),
    case Module:init(Args) of
        {ok, State} ->
            {ok, #bp_tree{
                order = Order,
                store_module = Module,
                store_state = State
            }};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns a value associated with a key from a B+ tree.
%% @end
%%--------------------------------------------------------------------
-spec find(key(), tree()) ->
    {{ok, value()} | error() | error_stacktrace(), tree()}.
find(Key, Tree = #bp_tree{}) ->
    try
        {{ok, RootId}, Tree2} = bp_tree_store:get_root_id(Tree),
        {[{_, Leaf} | _], Tree3} = find_path(Key, RootId, Tree2),
        {bp_tree_node:find(Key, Leaf), Tree3}
    catch
        _:{badmatch, Reason} -> {{error, Reason}, Tree};
        _:Reason -> {{error, Reason, erlang:get_stacktrace()}, Tree}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Inserts a key-value pair into a B+ tree.
%% @end
%%--------------------------------------------------------------------
-spec insert(key(), value(), tree()) ->
    {ok | error() | error_stacktrace(), tree()}.
insert(Key, Value, Tree = #bp_tree{order = Order}) ->
    try
        case bp_tree_store:get_root_id(Tree) of
            {{ok, RootId}, Tree2} ->
                {Path, Tree3} = find_path(Key, RootId, Tree2),
                insert(Key, {Value, undefined}, Path, Tree3);
            {{error, not_found}, Tree2} ->
                Root = bp_tree_node:new(Order, true),
                {{ok, RootId}, Tree3} = bp_tree_store:create_node(Root, Tree2),
                {ok, Tree4} = bp_tree_store:set_root_id(RootId, Tree3),
                {Path, Tree5} = find_path(Key, RootId, Tree4),
                insert(Key, {Value, undefined}, Path, Tree5)
        end
    catch
        _:{badmatch, Reason} -> {{error, Reason}, Tree};
        _:Reason -> {{error, Reason, erlang:get_stacktrace()}, Tree}
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%%--------------------------------------------------------------------
%% @private
%% @equiv find_path(Key, NodeId, Tree, [])
%% @end
%%--------------------------------------------------------------------
-spec find_path(key(), bp_tree_node:id(), tree()) -> {path(), tree()}.
find_path(Key, NodeId, Tree) ->
    find_path(Key, NodeId, Tree, []).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns leaf-root path for a given key.
%% @end
%%--------------------------------------------------------------------
-spec find_path(key(), bp_tree_node:id(), tree(), path()) -> {path(), tree()}.
find_path(Key, NodeId, Tree, Path) ->
    {{ok, Node}, Tree2} = bp_tree_store:get_node(NodeId, Tree),
    Path2 = [{NodeId, Node} | Path],
    case bp_tree_node:next(Key, Node) of
        {ok, NodeId2} -> find_path(Key, NodeId2, Tree2, Path2);
        {error, not_found} -> {Path2, Tree2}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Inserts a key-value pair into a B+ tree along the provided leaf-root path.
%% Stops when insert does not cause node split, otherwise inserts new key-value
%% pair into next node on path.
%% @end
%%--------------------------------------------------------------------
-spec insert(key(), value(), path(), tree()) -> {ok | error(), tree()}.
insert(Key, Value, [], Tree = #bp_tree{order = Order}) ->
    Root = bp_tree_node:new(Order, false),
    {ok, Root2} = bp_tree_node:insert(Key, Value, Root),
    {{ok, RootId}, Tree2} = bp_tree_store:create_node(Root2, Tree),
    bp_tree_store:set_root_id(RootId, Tree2);
insert(Key, Value, [{NodeId, Node} | Path], Tree) ->
    case bp_tree_node:insert(Key, Value, Node) of
        {ok, Node2} ->
            bp_tree_store:update_node(NodeId, Node2, Tree);
        {ok, LNode, Key2, RNode} ->
            {{ok, RNodeId}, Tree2} = bp_tree_store:create_node(RNode, Tree),
            LNode2 = bp_tree_node:set_next(RNodeId, LNode),
            {ok, Tree3} = bp_tree_store:update_node(NodeId, LNode2, Tree2),
            insert(Key2, {NodeId, RNodeId}, Path, Tree3);
        {error, Reason} ->
            {error, Reason}
    end.