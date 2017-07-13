%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for storing B+ tree nodes. It uses custom storage
%%% backend, which must implement bp_tree_store behaviour.
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree_store).
-author("Krzysztof Trzepla").

-include("bp_tree.hrl").

-type args() :: list().
-type state() :: any().

-export_type([args/0, state/0]).

-export([init/2, terminate/1]).
-export([get_root_id/1, set_root_id/2, unset_root_id/1]).
-export([create_node/2, get_node/2, update_node/3, delete_node/2]).

%%====================================================================
%% Callbacks
%%====================================================================

-callback init(args()) -> {ok, state()} | {error, term()}.

-callback set_root_id(bp_tree_node:id(), state()) ->
    {ok | {error, term()}, state()}.

-callback unset_root_id(state()) ->
    {ok | {error, term()}, state()}.

-callback get_root_id(state()) ->
    {{ok, bp_tree_node:id()} | {error, term()}, state()}.

-callback create_node(bp_tree:tree_node(), state()) ->
    {{ok, bp_tree_node:id()} | {error, term()}, state()}.

-callback get_node(bp_tree_node:id(), state()) ->
    {{ok, bp_tree:tree_node()} | {error, term()}, state()}.

-callback update_node(bp_tree_node:id(), bp_tree:tree_node(), state()) ->
    {ok | {error, term()}, state()}.

-callback delete_node(bp_tree_node:id(), state()) ->
    {ok | {error, term()}, state()}.

-callback terminate(state()) -> any().

%%====================================================================
%% API functions
%%====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes B+ tree store.
%% @end
%%--------------------------------------------------------------------
-spec init(args(), bp_tree:tree()) -> {ok, bp_tree:tree()} | {error, term()}.
init(Args, Tree = #bp_tree{store_module = Module}) ->
    case Module:init(Args) of
        {ok, State} -> {ok, Tree#bp_tree{store_state = State}};
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Cleans up B+ tree store.
%% @end
%%--------------------------------------------------------------------
-spec terminate(bp_tree:tree()) -> any().
terminate(#bp_tree{store_module = Module, store_state = State}) ->
    Module:terminate(State).

%%--------------------------------------------------------------------
%% @doc
%% Updates root node ID of B+ tree.
%% @end
%%--------------------------------------------------------------------
-spec set_root_id(bp_tree_node:id(), bp_tree:tree()) ->
    {ok | {error, term()}, bp_tree:tree()}.
set_root_id(RootId, Tree = #bp_tree{
    store_module = Module,
    store_state = State
}) ->
    case Module:set_root_id(RootId, State) of
        {ok, State2} ->
            {ok, Tree#bp_tree{store_state = State2}};
        {{error, Reason}, State2} ->
            {{error, Reason}, Tree#bp_tree{store_state = State2}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Clears root node ID of B+ tree.
%% @end
%%--------------------------------------------------------------------
-spec unset_root_id(bp_tree:tree()) -> {ok | {error, term()}, bp_tree:tree()}.
unset_root_id(Tree = #bp_tree{
    store_module = Module,
    store_state = State
}) ->
    case Module:unset_root_id(State) of
        {ok, State2} ->
            {ok, Tree#bp_tree{store_state = State2}};
        {{error, Reason}, State2} ->
            {{error, Reason}, Tree#bp_tree{store_state = State2}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns root node ID of B+ tree.
%% @end
%%--------------------------------------------------------------------
-spec get_root_id(bp_tree:tree()) ->
    {{ok, bp_tree_node:id()} | {error, term()}, bp_tree:tree()}.
get_root_id(Tree = #bp_tree{store_module = Module, store_state = State}) ->
    case Module:get_root_id(State) of
        {{ok, RootId}, State2} ->
            {{ok, RootId}, Tree#bp_tree{store_state = State2}};
        {{error, Reason}, State2} ->
            {{error, Reason}, Tree#bp_tree{store_state = State2}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Saves new B+ tree node.
%% @end
%%--------------------------------------------------------------------
-spec create_node(bp_tree:tree_node(), bp_tree:tree()) ->
    {{ok, bp_tree_node:id()} | {error, term()}, bp_tree:tree()}.
create_node(Node, Tree = #bp_tree{
    store_module = Module,
    store_state = State
}) ->
    case Module:create_node(Node, State) of
        {{ok, NodeId}, State2} ->
            {{ok, NodeId}, Tree#bp_tree{store_state = State2}};
        {{error, Reason}, State2} ->
            {{error, Reason}, Tree#bp_tree{store_state = State2}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns B+ tree node associated with provided ID.
%% @end
%%--------------------------------------------------------------------
-spec get_node(bp_tree_node:id(), bp_tree:tree()) ->
    {{ok, bp_tree:tree_node()} | {error, term()}, bp_tree:tree()}.
get_node(NodeId, Tree = #bp_tree{
    store_module = Module,
    store_state = State
}) ->
    case Module:get_node(NodeId, State) of
        {{ok, Node}, State2} ->
            {{ok, Node}, Tree#bp_tree{store_state = State2}};
        {{error, Reason}, State2} ->
            {{error, Reason}, Tree#bp_tree{store_state = State2}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates B+ tree node associated with provided ID.
%% @end
%%--------------------------------------------------------------------
-spec update_node(bp_tree_node:id(), bp_tree:tree_node(), bp_tree:tree()) ->
    {ok | {error, term()}, bp_tree:tree()}.
update_node(NodeId, Node, Tree = #bp_tree{
    store_module = Module,
    store_state = State
}) ->
    case Module:update_node(NodeId, Node, State) of
        {ok, State2} ->
            {ok, Tree#bp_tree{store_state = State2}};
        {{error, Reason}, State2} ->
            {{error, Reason}, Tree#bp_tree{store_state = State2}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes node associated with provided ID from B+ tree.
%% @end
%%--------------------------------------------------------------------
-spec delete_node(bp_tree_node:id(), bp_tree:tree()) ->
    {ok | {error, term()}, bp_tree:tree()}.
delete_node(NodeId, Tree = #bp_tree{
    store_module = Module,
    store_state = State
}) ->
    case Module:delete_node(NodeId, State) of
        {ok, State2} ->
            {ok, Tree#bp_tree{store_state = State2}};
        {{error, Reason}, State2} ->
            {{error, Reason}, Tree#bp_tree{store_state = State2}}
    end.