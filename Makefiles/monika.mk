MODULE=monika
SRCDIR=visualization_interfaces/Monika

OAR_PERLLIB=$(SRCDIR)/lib

SHAREDIR_FILES= $(SRCDIR)/monika.conf.in \
		  $(SRCDIR)/userInfos.cgi

CGIDIR_FILES = $(SRCDIR)/monika.cgi.in


WWWDIR_FILES = $(SRCDIR)/monika.css

include Makefiles/shared/shared.mk

clean: clean_shared
# Nothing to do

build: build_shared
# Nothing to do

install: install_shared
	install -m 0755 -d $(DESTDIR)$(PERLLIBDIR)
	cp -r $(OAR_PERLLIB)/* $(DESTDIR)$(PERLLIBDIR)/

uninstall: uninstall_shared
	(cd $(OAR_PERLLIB) && find . -type f -exec rm -f $(DESTDIR)$(PERLLIBDIR)/{} \;)

.PHONY: install setup uninstall build clean
