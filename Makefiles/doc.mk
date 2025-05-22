MODULE=doc
SRCDIR=oar

include Makefiles/shared/shared.mk

clean: clean_shared 
	$(MAKE) -C docs clean

build: build_shared

install: build  build-html-doc install_shared
	install -d $(DESTDIR)$(DOCDIR)/html
	install -d $(DESTDIR)$(DOCDIR)/scripts/prologue_epilogue
	install -d $(DESTDIR)$(DOCDIR)/scripts
	install -d $(DESTDIR)$(DOCDIR)/scripts/job_resource_manager

	install -m 0644 $(SRCDIR)/tools/job_resource_manager_systemd.pl $(DESTDIR)$(DOCDIR)/scripts/job_resource_manager/

	install -m 0644 $(SRCDIR)/../scripts/oar_prologue $(DESTDIR)$(DOCDIR)/scripts/prologue_epilogue/
	install -m 0644 $(SRCDIR)/../scripts/oar_epilogue $(DESTDIR)$(DOCDIR)/scripts/prologue_epilogue/
	install -m 0644 $(SRCDIR)/../scripts/oar_prologue_local $(DESTDIR)$(DOCDIR)/scripts/prologue_epilogue/
	install -m 0644 $(SRCDIR)/../scripts/oar_epilogue_local $(DESTDIR)$(DOCDIR)/scripts/prologue_epilogue/
	install -m 0644 $(SRCDIR)/../scripts/oar_diffuse_script $(DESTDIR)$(DOCDIR)/scripts/prologue_epilogue/
	install -m 0644 $(SRCDIR)/../scripts/lock_user.sh $(DESTDIR)$(DOCDIR)/scripts/prologue_epilogue/
	install -m 0644 $(SRCDIR)/../scripts/oar_server_proepilogue.pl $(DESTDIR)$(DOCDIR)/scripts/prologue_epilogue/

uninstall: uninstall_shared
	rm -rf \
	    $(DESTDIR)$(DOCDIR)/html \
	    $(DESTDIR)$(DOCDIR)/scripts/job_resource_manager/ \
	    $(DESTDIR)$(DOCDIR)/scripts/prologue_epilogue/

build-html-doc:
	$(MAKE) -C docs html BUILDDIR=$(DESTDIR)$(DOCDIR)

.PHONY: install setup uninstall build clean

