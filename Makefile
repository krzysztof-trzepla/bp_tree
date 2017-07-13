##
## Rebar targets
##

.PHONY: all
all: compile

.PHONY: compile
compile:
	./rebar3 compile

.PHONY: clean
clean:
	./rebar3 clean

.PHONY: dialyzer
dialyzer:
	./rebar3 dialyzer

.PHONY: test
test:
	./rebar3 eunit --cover

.PHONY: cover
cover:
	./rebar3 cover --reset

.PHONY: doc
doc:
	./rebar3 edoc