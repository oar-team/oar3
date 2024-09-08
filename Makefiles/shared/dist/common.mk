export CFLAGS?=-g -Wall

export PREFIX?=/usr/local

export RUNDIR?=/var/run
export LOGDIR?=/var/log
export INITDIR?=$(ETCDIR)/init.d
export SYSTEMDIR?=$(ETCDIR)/systemd/system
export CRONDIR?=$(ETCDIR)/cron.d
export CRONHOURLYDIR?=$(ETCDIR)/cron.hourly
export LOGROTATEDIR?=$(ETCDIR)/logrotate.d
export DEFAULTDIR?=$(ETCDIR)/default
export SHAREDIR?=$(PREFIX)/share/oar/oar-$(strip $(MODULE))
export MANDIR?=$(PREFIX)/share/man
export BINDIR?=$(PREFIX)/bin
export SBINDIR?=$(PREFIX)/sbin
export DOCDIR?=$(PREFIX)/share/doc/oar-$(strip $(MODULE))
export EXAMPLEDIR?=$(DOCDIR)/examples
export WWWDIR?=$(PREFIX)/share/oar-web-status
export PERLLIBDIR?=$(PREFIX)/lib/site_perl
export VARLIBDIR?=/var/lib
export ETCDIR?=/etc

export OARHOMEDIR?=$(VARLIBDIR)/oar
export OARDIR?=$(PREFIX)/lib/oar
export OARCONFDIR?=$(ETCDIR)/oar

export SETUP_TYPE?=tgz
export TARGET_DIST?=debian

export ROOTUSER?=root
export ROOTGROUP?=root

export OARUSER?=oar
export OAROWNER?=$(OARUSER)
export OAROWNERGROUP?=$(OAROWNER)

export OARDO_DEFAULTUSER?=$(ROOTUSER)
export OARDO_DEFAULTGROUP?=$(OAROWNERGROUP)

export WWWUSER?=www-data
export APACHECONFDIR?=$(ETCDIR)/apache2
export WWWROOTDIR?=/var/www
export CGIDIR?=$(WWWROOTDIR)/cgi-bin
export XAUTHCMDPATH?=/usr/bin/xauth
export OARSHCMD?=oarsh_oardo

export RESTAPI_ROOTPATH?=/oarapi
export RESTAPI_PORT?=6665
export SYSTEMD_CONFDIR?=/etc/systemd/system
