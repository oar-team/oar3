#!/bin/bash
set -e

OAR_VERSION=$1
DB=$2

TMPDIR=$(mktemp -d --tmpdir install_oar.XXXXXXXX)
SRCDIR="$TMPDIR/src"

OAR_TARBALL_URL="https://github.com/oar-team/oar/archive/${OAR_VERSION}.tar.gz"


fail() {
    echo $@ 1>&2
    exit 1
}

echo "Download oar from $OAR_TARBALL_URL"
mkdir -p $SRCDIR
wget  --no-check-certificate "$OAR_TARBALL_URL" -O $TMPDIR/oar-tarball.tar.gz
TARBALL_FILE=$TMPDIR/oar-tarball.tar.gz

echo "Extract OAR $OAR_VERSION"
tar xf "$TARBALL_FILE" -C $SRCDIR
SRCDIR=$SRCDIR/oar-${OAR_VERSION}

echo "Install OAR ${OAR_VERSION}"
make -C $SRCDIR PREFIX=/usr/local user-build tools-build node-build server-build
make -C $SRCDIR PREFIX=/usr/local user-install tools-install node-install server-install
make -C $SRCDIR PREFIX=/usr/local user-setup tools-setup node-setup server-setup

echo "Copy initd scripts"
if [ -f /usr/local/share/oar/oar-server/init.d/oar-server ]; then
    cat /usr/local/share/oar/oar-server/init.d/oar-server > /etc/init.d/oar-server
    chmod +x  /etc/init.d/oar-server
fi

if [ -f /usr/local/share/doc/oar-server/examples/init.d/oar-server ]; then
    cat /usr/local/share/doc/oar-server/examples/init.d/oar-server > /etc/init.d/oar-server
    chmod +x  /etc/init.d/oar-server
fi


if [ -f /usr/local/share/oar/oar-server/default/oar-server ]; then
    cat /usr/local/share/oar/oar-server/default/oar-server > /etc/default/oar-server
fi

if [ -f /usr/local/share/doc/oar-server/examples/default/oar-server ]; then
    cat /usr/local/share/doc/oar-server/examples/default/oar-server > /etc/default/oar-server
fi

if [ -f /usr/local/share/oar/oar-node/init.d/oar-node ]; then
    cat /usr/local/share/oar/oar-node/init.d/oar-node > /etc/init.d/oar-node
    chmod +x  /etc/init.d/oar-node
fi

if [ -f /usr/local/share/doc/oar-node/examples/init.d/oar-node ]; then
    cat /usr/local/share/doc/oar-node/examples/init.d/oar-node > /etc/init.d/oar-node
    chmod +x  /etc/init.d/oar-node
fi


if [ -f /usr/local/share/oar/oar-node/default/oar-node ]; then
    cat /usr/local/share/oar/oar-node/default/oar-node > /etc/default/oar-node
fi

if [ -f /usr/local/share/doc/oar-node/examples/default/oar-node ]; then
    cat /usr/local/share/doc/oar-node/examples/default/oar-node > /etc/default/oar-node
fi


echo "Edit oar.conf"
sed -e 's/^LOG_LEVEL\=\"2\"/LOG_LEVEL\=\"3\"/' -i /etc/oar/oar.conf
sed -e 's/^DB_HOSTNAME\=.*/DB_HOSTNAME\=\"server\"/' -i /etc/oar/oar.conf
sed -e 's/^SERVER_HOSTNAME\=.*/SERVER_HOSTNAME\=\"server\"/' -i /etc/oar/oar.conf
sed -e 's/^#\(TAKTUK_CMD\=\"\/usr\/bin\/taktuk \-t 30 \-s\".*\)/\1/' -i /etc/oar/oar.conf
sed -e 's/^#\(PINGCHECKER_TAKTUK_ARG_COMMAND\=\"broadcast exec timeout 5 kill 9 \[ true \]\".*\)/\1/' -i /etc/oar/oar.conf

sed -e 's/^\(DB_TYPE\)=.*/\1="Pg"/' -i /etc/oar/oar.conf
sed -e 's/^\(DB_PORT\)=.*/\1="5432"/' -i /etc/oar/oar.conf


sed -e 's/^#\(JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD\=\"cpuset\".*\)/\1/' -i /etc/oar/oar.conf
sed -e 's/^#\(CPUSET_PATH\=\"\/oar\".*\)/\1/' -i /etc/oar/oar.conf
sed -e 's/^\(DB_BASE_PASSWD\)=.*/\1="oar"/' -i /etc/oar/oar.conf
sed -e 's/^\(DB_BASE_LOGIN\)=.*/\1="oar"/' -i /etc/oar/oar.conf
sed -e 's/^\(DB_BASE_PASSWD_RO\)=.*/\1="oar_ro"/' -i /etc/oar/oar.conf
sed -e 's/^\(DB_BASE_LOGIN_RO\)=.*/\1="oar_ro"/' -i /etc/oar/oar.conf


# Fix permissions
chmod a+r /etc/oar/oar.conf

#This line must be uncommented if the mount_cgroup.sh script is not used
#sed -e 's/#exit/exit/' -i /etc/oar/job_resource_manager_cgroups.pl

echo "Init database"

/usr/local/sbin/oar-database --create --db-admin-user postgres --db-is-local
