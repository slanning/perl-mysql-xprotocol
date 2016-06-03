#!/usr/bin/env perl
# a proxy to put between a MySQL X Protocol client and MySQL server
# ./xproto-proxy.pl localhost:33059 serverhost:33060
# mysqlsh -u test_user -h localhost --js -P 33059
# mysql-js>

use strict;
use warnings;
use Data::Dumper; { package Data::Dumper; our ($Indent, $Sortkeys, $Terse, $Useqq) = (1)x4 }

use Net::Proxy;


package Decoder;

use strict;
use warnings;
use Data::Dumper; { package Data::Dumper; our ($Indent, $Sortkeys, $Terse, $Useqq) = (1)x4 }
use File::Spec;

# Ugh... (number are from mysqlx.proto , packages inferred manually)
my %MESSAGE = (
    client => {
        1  => 'Mysqlx.Connection.CapabilitiesGet',         # CON_CAPABILITIES_GET
        2  => 'Mysqlx.Connection.CapabilitiesSet',         # CON_CAPABILITIES_SET
        3  => 'Mysqlx.Connection.Close',                   # CON_CLOSE

        4  => 'Mysqlx.Session.AuthenticateStart',          # SESS_AUTHENTICATE_START
        5  => 'Mysqlx.Session.AuthenticateContinue',       # SESS_AUTHENTICATE_CONTINUE 
        6  => 'Mysqlx.Session.Reset',                      # SESS_RESET
        7  => 'Mysqlx.Session.Close',                      # SESS_CLOSE

        12 => 'Mysqlx.Sql.StmtExecute',                    # SQL_STMT_EXECUTE

        17 => 'Mysqlx.Crud.Find',                          # CRUD_FIND
        18 => 'Mysqlx.Crud.Insert',                        # CRUD_INSERT
        19 => 'Mysqlx.Crud.Update',                        # CRUD_UPDATE
        20 => 'Mysqlx.Crud.Delete',                        # CRUD_DELETE

        24 => 'Mysqlx.Expect.Open',                        # EXPECT_OPEN
        25 => 'Mysqlx.Expect.Close',                       # EXPECT_CLOSE
    },
    server => {
        0  => 'Mysqlx.Ok',                                 # OK
        1  => 'Mysqlx.Error',                              # ERROR

        2  => 'Mysqlx.Connection.Capabilities',            # CONN_CAPABILITIES

        3  => 'Mysqlx.Session.AuthenticateContinue',       # SESS_AUTHENTICATE_CONTINUE
        4  => 'Mysqlx.Session.AuthenticateOk',             # SESS_AUTHENTICATE_OK

        11 => 'Mysqlx.Notice.Frame',                       # NOTICE

        12 => 'Mysqlx.Resultset.ColumnMetaData',           # RESULTSET_COLUMN_META_DATA
        13 => 'Mysqlx.Resultset.Row',                      # RESULTSET_ROW
        14 => 'Mysqlx.Resultset.FetchDone',                # RESULTSET_FETCH_DONE
        # Note: there is no FetchSuspended message in the .proto files
        15 => 'Mysqlx.Resultset.FetchSuspended',           # RESULTSET_FETCH_SUSPENDED
        16 => 'Mysqlx.Resultset.FetchDoneMoreResultsets',  # RESULTSET_FETCH_DONE_MORE_RESULTSETS

        17 => 'Mysqlx.Sql.StmtExecuteOk',                  # SQL_STMT_EXECUTE_OK
        18 => 'Mysqlx.Resultset.FetchDoneMoreOutParams',   # RESULTSET_FETCH_DONE_MORE_OUT_PARAMS
    },
);

sub new {
    my ($class, %p) = @_;
    die "missing param 'endpoint' (can be 'client' or 'server')"
      unless $p{endpoint} and ($p{endpoint} eq 'client' or $p{endpoint} eq 'server');

    my $self = bless {
        debug        => 0,
        endpoint     => undef, # required: client or server
        protoc       => 'protoc',
        proto_dir    => '../proto',
        %p,

        _bytes     => [],
    }, $class;
    $self->reset();

    return($self);
}

sub decode_message {
    my ($self) = @_;

    my $package = $MESSAGE{$self->{endpoint}}{$self->{_type}}
      // die("unhandled type '$self->{_type}'");

    printf("%s: %s\n", uc($self->{endpoint}), $package);

    my $proto_file = proto_file_for($package);
    my $proto_cmd = sprintf("%s --decode='%s' -I'%s' '%s'",
                            $self->{protoc},
                            $package,
                            $self->{proto_dir},
                            File::Spec->catfile($self->{proto_dir}, $proto_file),
    );

    # otherwise there can be apostrophes and so on
    my $hex = _bytes_to_hex($self->{_payload});

    my $out = `perl -e'print pack("H*", q{$hex})' | $proto_cmd`;
    print $out . $/;

    $self->reset();
}

sub _bytes_to_hex {
    my ($bytes) = @_;
    my $len = bytes::length($bytes);
    return join('', map(sprintf('%02x', $_), unpack("C$len", $bytes)));
}


sub proto_file_for {
    my ($package) = @_;
    (my $file = lc($package)) =~ s/\.[a-z]+$//;
    $file =~ tr/./_/;
    return($file . '.proto');
}

sub decode {
    my ($self, $dataref) = @_;

    $self->debug("DECODE!");

    my @bytes = unpack('C*', $$dataref);
    return unless @bytes;

    # there may be _bytes left over from previous call
    push @{ $self->{_bytes} }, @bytes;
    my $bytes = $self->{_bytes};

    use bytes;

    while (@$bytes) {
        $self->debug("bytes remaining: " . scalar(@$bytes));

        my $done = 0;
        while ($self->{_len_bytes_remaining}) {
            my $byte = shift(@$bytes);

            --$self->{_len_bytes_remaining};

            $self->{_len} += $byte << $self->{_shift};
            $self->{_shift} += 8;

            return unless @$bytes;
        }
        $self->debug("len: $self->{_len}");

        if ($self->{_type} < 0) {
            $self->debug("raw type: " . $bytes->[0] . " slice: " . ($bytes->[0] & 0x7F));

            $self->{_type} = shift(@$bytes) & 0x7F;

            --$self->{_len};

            # sometimes there can be no payload
            return if $self->{_len} and not @$bytes;
        }
        $self->debug("type: $self->{_type}");

        if ($self->{_len}) {
            my $payload_len = length($self->{_payload});

            $self->{_payload} .= pack('C*', splice(@$bytes, 0, $self->{_len}));

            my $payload_bytes_gotten = length($self->{_payload}) - $payload_len;
            $self->{_len} -= $payload_bytes_gotten;

            $self->debug("payload bytes gotten: $payload_bytes_gotten len: $self->{_len}");
        }
        if ($self->{_len}) {
            $self->debug("not enough payload yet");
            return;
        }

        $self->decode_message();
    }
}

sub debug {
    my ($self, $str) = @_;
    return unless $self->{debug};
    print uc($self->{endpoint}) . ": $str\n";
}

sub reset {
    my ($self) = @_;

    $self->{_len}                  = 0;     # _len must be at least 1 and needs to get 4 bytes
    $self->{_len_bytes_remaining}  = 4;
    $self->{_type}                 = -1;    # _type can be 0
    $self->{_payload}              = '';
    $self->{_shift}                = 0;

    return;
}


package main;

my $DEBUG = 0;
$|++;

if (!caller) {
    # $0 proxyhost:proxyport serverhost:serverport
    main(@ARGV);
    exit;
}

sub main {
    my ($p, $s) = @_;
    unless ($p) {
        print STDERR "usage: $0 proxyhost:proxyport [serverhost:serverport]\n";
        exit;
    }
    my ($phost, $pport) = split(/:/, $p);
    my ($shost, $sport);
    if ($s) {
        ($shost, $sport) = split(/:/, $s);
    }
    $shost //= 'localhost';
    $sport //= 33060;

    my $client_decoder = Decoder->new(endpoint => 'client', debug => $DEBUG);
    my $server_decoder = Decoder->new(endpoint => 'server', debug => $DEBUG);

    my $proxy = Net::Proxy->new({
        in => {
            type => 'tcp',
            host => $phost,
            port => $pport,
            hook => sub { $client_decoder->decode(@_) },
        },
        out => {
            type => 'tcp',
            host => $shost,
            port => $sport,
            hook => sub { $server_decoder->decode(@_) },
        },
    });
    $proxy->register();

    Net::Proxy->mainloop();
}


1;
__END__
