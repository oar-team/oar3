MODULE=common
SRCDIR=oar

OARDIR_BINFILES = $(SRCDIR)/tools/oarsh/oarsh_shell.in \
	              $(SRCDIR)/tools/oarsh/oarsh.in \
		          $(SRCDIR)/tools/sentinelle.pl

SHAREDIR_FILES = $(SRCDIR)/tools/oar.conf.in \
                   $(SRCDIR)/tools/oarnodesetting_ssh.in \
		   $(SRCDIR)/tools/update_cpuset_id.sh.in

LOGROTATEDIR_FILES = setup/logrotate.d/oar-common.in

PROCESS_TEMPLATE_FILES = $(SRCDIR)/tools/oarsh/oarcp.in \
			 $(SRCDIR)/tools/oardodo.c.in \
			 $(SRCDIR)/tools/oardo.c.in

include Makefiles/shared/shared.mk

clean: clean_shared
	$(OARDO_CLEAN) CMD_WRAPPER=$(OARDIR)/oarsh CMD_TARGET=$(DESTDIR)$(BINDIR)/oarsh
	$(OARDO_CLEAN) CMD_WRAPPER=$(OARDIR)/oarnodesetting CMD_TARGET=$(DESTDIR)$(SBINDIR)/oarnodesetting
	-rm -f $(SRCDIR)/tools/oardodo

build: build_shared
	pip install .
	$(OARDO_BUILD) CMD_WRAPPER=$(OARDIR)/oarsh CMD_TARGET=$(DESTDIR)$(BINDIR)/oarsh
	#$(OARDO_BUILD) CMD_WRAPPER=$(OARDIR)/oarnodesetting CMD_TARGET=$(DESTDIR)$(SBINDIR)/oarnodesetting
	$(CC) $(CPPFLAGS) $(CFLAGS) $(LDFLAGS) -o $(SRCDIR)/tools/oardodo $(SRCDIR)/tools/oardodo.c

install: install_shared
	if [ ! -f $(DESTDIR)$(OARDIR)/oarnodesetting3 ]; then \
		mv $(DESTDIR)$(BINDIR)/oarnodesetting3 $(DESTDIR)$(OARDIR)/oarnodesetting3;\
	fi
	$(OARDO_INSTALL) CMD_WRAPPER=$(OARDIR)/oarsh CMD_TARGET=$(DESTDIR)$(BINDIR)/oarsh
	$(OARDO_INSTALL) CMD_WRAPPER=$(OARDIR)/oarnodesetting3 CMD_TARGET=$(DESTDIR)$(SBINDIR)/oarnodesetting

	install -d $(DESTDIR)$(BINDIR)
	install -m 0755 $(SRCDIR)/tools/oarsh/oarcp $(DESTDIR)$(BINDIR)/
    # install -m 0755 $(SRCDIR)/qfunctions/oarprint $(DESTDIR)$(BINDIR)

	install -d $(DESTDIR)$(OARDIR)/oardodo
	install -m 0754 $(SRCDIR)/tools/oardodo $(DESTDIR)$(OARDIR)/oardodo

uninstall: uninstall_shared
	$(OARDO_UNINSTALL) CMD_WRAPPER=$(OARDIR)/oarsh CMD_TARGET=$(DESTDIR)$(BINDIR)/oarsh
	$(OARDO_UNINSTALL) CMD_WRAPPER=$(OARDIR)/oarnodesetting CMD_TARGET=$(DESTDIR)$(SBINDIR)/oarnodesetting
	rm -rf $(DESTDIR)$(OARDIR)/oardodo
	rm -rf $(DESTDIR)$(EXAMPLEDIR)

.PHONY: install setup uninstall build clean
