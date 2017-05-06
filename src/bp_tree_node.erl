%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree_node).
-author("Krzysztof Trzepla").

-include("bp_tree.hrl").

%% API exports
-export([]).

-type id() :: binary().
-type children() :: any().

-export_type([id/0, children/0]).

%%====================================================================
%% API functions
%%====================================================================

%%====================================================================
%% Internal functions
%%====================================================================
