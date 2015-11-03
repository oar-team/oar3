#!/usr/bin/perl
# coding: utf-8

use IO::Socket::UNIX;
use strict;
use warnings;
use DBI();
use OAR::IO;
use OAR::Modules::Judas qw(oar_debug oar_warn oar_error set_current_log_category);

# Log category
set_current_log_category('judas wrapper');

if (!defined($ENV{OARCONFFILE})) {
    $ENV{OARCONFFILE} = "/etc/oar/oar.conf";
}

my $binpath;

if (defined($ENV{OARDIR})){
    $binpath = $ENV{OARDIR}."/";
}else{
    $binpath = "/usr/local/lib/oar/";
}

my $SOCK_PATH = "/tmp/judas_notify_user.sock";

my $base = OAR::IO::connect();

# Server:
my $server = IO::Socket::UNIX->new(
        Type => SOCK_STREAM(),
        Local => $SOCK_PATH,
        Listen => 1,
    );

while (my $socket = $server->accept()) {
    #print "wait new connection\n";
    my $line;
    while($line = <$socket>) {
        chomp($line);
        my @v = split(/°/, $line);
        #notify,$addr,$user,$jid,$name,$state,$msg
        OAR::Modules::Judas::notify_user($base,$v[0],$v[1],$v[2],$v[3],$v[4],$v[5],$v[6]);

    }
}
