MODULE=man

MANDIR_FILES=$(wildcard docs/man/man1/*.pod) $(wildcard docs/man/man1/*.pod.in)

include Makefiles/shared/shared.mk

clean: clean_shared

build: build_shared

.PHONY: install setup uninstall build clean
