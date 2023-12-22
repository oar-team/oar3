MODULE=api

SRCDIR=oar

# OAR_PERLLIB = $(SRCDIR)/lib

# OARDIR_BINFILES = $(SRCDIR)/oarapi.pl

RESTAPI_ROOTPATH=/oarapi

SHAREDIR_FILES = $(SRCDIR)/../setup/apache2/apache2.conf.in \
		   $(SRCDIR)/../setup/uvicorn/main.py.in\
		   $(SRCDIR)/../setup/uvicorn/log.ini.in\
		   $(SRCDIR)/../setup/uvicorn/oarapi.service.in\
		   $(SRCDIR)/tools/stress_factor.sh

include Makefiles/shared/shared.mk

clean: clean_shared

build: build_shared

install: install_shared

uninstall: uninstall_shared

.PHONY: install setup uninstall build clean


