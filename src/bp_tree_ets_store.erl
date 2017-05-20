%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides B+ tree store with Erlang map as a backend.
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree_ets_store).
-author("Krzysztof Trzepla").

-behaviour(bp_tree_store).

%% bp_tree_store callbacks
-export([init/1, terminate/1]).
-export([set_root_id/2, get_root_id/1]).
-export([create_node/2, get_node/2, update_node/3, delete_node/2]).

-record(state, {
    root_id :: undefined | bp_tree_node:id(),
    next_node_id = 1 :: pos_integer(),
    table_id :: ets:tid()
}).

-type state() :: #state{}.

%%====================================================================
%% bp_tree_store callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes B+ tree store.
%% @end
%%--------------------------------------------------------------------
-spec init(bp_tree_store:args()) -> {ok, state()}.
init(_Args) ->
    {ok, #state{table_id = ets:new(store, [public])}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link bp_tree_store:set_root_id/2}
%% @end
%%--------------------------------------------------------------------
-spec set_root_id(bp_tree_node:id(), state()) ->
    {ok | {error, term()}, state()}.
set_root_id(NodeId, State = #state{}) ->
    {ok, State#state{root_id = NodeId}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link bp_tree_store:get_root_id/1}
%% @end
%%--------------------------------------------------------------------
-spec get_root_id(state()) ->
    {{ok, bp_tree_node:id()} | {error, not_found | term()}, state()}.
get_root_id(State = #state{root_id = undefined}) ->
    {{error, not_found}, State};
get_root_id(State = #state{root_id = RootId}) ->
    {{ok, RootId}, State}.

%%--------------------------------------------------------------------
%% @doc
%% {@link bp_tree_store:create_node/2}
%% @end
%%--------------------------------------------------------------------
-spec create_node(bp_tree:tree_node(), state()) ->
    {{ok, bp_tree_node:id()} | {error, term()}, state()}.
create_node(Node, State = #state{
    next_node_id = NextNodeId,
    table_id = TableId
}) ->
    NodeId = integer_to_binary(NextNodeId),
    ets:insert(TableId, {NodeId, Node}),
    {{ok, NodeId}, State#state{next_node_id = NextNodeId + 1}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link bp_tree_store:get_node/2}
%% @end
%%--------------------------------------------------------------------
-spec get_node(bp_tree_node:id(), state()) ->
    {{ok, bp_tree:tree_node()} | {error, term()}, state()}.
get_node(NodeId, State = #state{table_id = TableId}) ->
    case ets:lookup(TableId, NodeId) of
        [{NodeId, Node}] -> {{ok, Node}, State};
        [] -> {{error, not_found}, State}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link bp_tree_store:update_node/3}
%% @end
%%--------------------------------------------------------------------
-spec update_node(bp_tree_node:id(), bp_tree:tree_node(), state()) ->
    {ok | {error, term()}, state()}.
update_node(NodeId, Node, State  = #state{table_id = TableId}) ->
    case ets:lookup(TableId, NodeId) of
        [{NodeId, _}] ->
            ets:insert(TableId, {NodeId, Node}),
            {ok, State};
        [] ->
            {{error, not_found}, State}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link bp_tree_store:delete_node/2}
%% @end
%%--------------------------------------------------------------------
-spec delete_node(bp_tree_node:id(), state()) ->
    {ok | {error, term()}, state()}.
delete_node(NodeId, State = #state{table_id = TableId}) ->
    case ets:lookup(TableId, NodeId) of
        [{NodeId, _}] ->
            ets:delete(TableId, NodeId),
            {ok, State};
        [] ->
            {{error, not_found}, State}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Performs B+ tree store cleanup.
%% @end
%%--------------------------------------------------------------------
-spec terminate(state()) -> ok.
terminate(#state{table_id = TableId}) ->
    ets:delete(TableId),
    ok.
