MODULE=common-libs
SRCDIR=.

include Makefiles/shared/shared.mk

clean: clean_shared
# Nothing to do

build:  build_shared
# Nothing to do

install: install_shared
# Nothing to do

uninstall: uninstall_shared
# Nothing to do


.PHONY: install setup uninstall build clean
