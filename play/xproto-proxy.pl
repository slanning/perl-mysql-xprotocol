#!/usr/bin/env perl

package Decoder;

use strict;
use warnings;
use Data::Dumper; { package Data::Dumper; our ($Indent, $Sortkeys, $Terse, $Useqq) = (1)x4 }
use File::Spec;
use Time::HiRes qw/gettimeofday/;

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
        endpoint     => $p{endpoint},

        debug        => $p{debug},
        protoc       => $p{protoc},
        proto_dir    => $p{'proto-dir'},
        add_timestamp => $p{'add-timestamp'},

        _bytes     => [],
    }, $class;
    $self->reset();

    return($self);
}

sub decode_message {
    my ($self) = @_;

    my $package = $MESSAGE{$self->{endpoint}}{$self->{_type}}
      // die("unhandled type '$self->{_type}'");

    my $time = '';
    if ($self->{add_timestamp}) {
        my ($sec, $usec) = gettimeofday();
        my @t = localtime($sec);
        @t = reverse @t[0..5];
        $t[0] += 1900;
        $time = sprintf("%d-%02d-%02d %02d:%02d:%02d.%06d", @t, $usec) . ' ';
    }
    printf("%s%s: %s\n", $time,  uc($self->{endpoint}), $package);

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
    $out =~ s/^/  /mg;
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

use strict;
use warnings;
use Data::Dumper; { package Data::Dumper; our ($Indent, $Sortkeys, $Terse, $Useqq) = (1)x4 }
use File::Spec;
use Getopt::Long;
use Net::Proxy;
use Pod::Usage;

$|++;

if (!caller) {
    main();
    exit;
}

sub main {
    my $opt = cli_params();

    my @opts = map { $_ => $opt->{$_} } qw/debug protoc proto-dir add-timestamp/;
    my $client_decoder = Decoder->new(endpoint => 'client', @opts);
    my $server_decoder = Decoder->new(endpoint => 'server', @opts);

    my $proxy = Net::Proxy->new({
        in => {
            type => $opt->{'connection-type'},
            host => $opt->{proxyhost},
            port => $opt->{proxyport},
            hook => sub { $client_decoder->decode(@_) },
        },
        out => {
            type => $opt->{'connection-type'},
            host => $opt->{serverhost},
            port => $opt->{serverport},
            hook => sub { $server_decoder->decode(@_) },
        },
    });
    $proxy->register();

    Net::Proxy->mainloop();
}

sub cli_params {
    my %opt;

    $opt{'connection-type|t'}    = { default => 'tcp',     type => ':s'  };
    $opt{'protoc|p'}             = { default => 'protoc',  type => ':s'  };
    $opt{'proto-dir|d'}          = { default => '.',       type => ':s'  };
    $opt{'add-timestamp|T'}      = { default => 0,         type => '!'   };
    $opt{'forwarding|L'}         = { default => '',        type => ':s'  };
    $opt{'debug|D'}              = { default => 0,         type => '!'   };

    $opt{help}                   = { default => 0, type => '|?'  };
    $opt{man}                    = { default => 0, type => ''    };
    GetOptions(
        map {
            ( "$_$opt{$_}{type}" => \ ($opt{$_} = $opt{$_}{default}) );    # attn: evil
        } keys %opt
    ) or pod2usage(2);
    pod2usage(1) if $opt{help};
    pod2usage(-exitstatus => 0, -verbose => 2) if $opt{man};

    # get rid of shortcuts
    my $regex = qr/\|[a-zA-Z]$/;
    foreach my $key (grep { $_ =~ $regex } keys %opt) {
        (my $simple_key = $key) =~ s/$regex//;
        $opt{$simple_key} = delete($opt{$key});
    }

    if ($opt{forwarding}) {
        # [bind_address:] port:host:hostport
        my @a = split(/:/, delete($opt{forwarding}));
        @a && ($opt{$_} = pop(@a))
          for qw/serverport serverhost proxyport proxyhost/;
    }
    else {
        my ($p, $s) = @ARGV;   # leftover after flag processing
        unless ($p) {
            print STDERR "usage: $0 [options] proxyhost:proxyport [serverhost:serverport]\n";
            exit;
        }
        ($opt{proxyhost}, $opt{proxyport}) = split(/:/, $p);

        ($opt{serverhost}, $opt{serverport}) = split(/:/, $s)
          if $s;
    }

    $opt{proxyhost}  //= 'localhost';
    $opt{proxyport}  //= 33059;
    $opt{serverhost} //= 'localhost';
    $opt{serverport} //= 33060;

    # sanity checks
    _protodir_looks_reasonable($opt{'proto-dir'});

    return \%opt;
}

sub _protodir_looks_reasonable {
    my ($dir) = @_;
    my $absdir = File::Spec->rel2abs($dir);

    opendir(my $dh, $absdir) || die("Error opening '$absdir': $!");
    my @proto = grep {
        -f File::Spec->catfile($absdir, $_) && /\Amysqlx.*\.proto\z/;
    } readdir($dh);
    closedir($dh);
    die("--proto-dir '$absdir' doesn't contain .proto files") unless @proto;
}

1;
__END__

=head1 NAME

xproto-proxy.pl - decoding proxy for MySQL's X Protocol

=head1 SYNOPSIS

 xproto-proxy.pl [options] proxyhost:proxyport [serverhost:serverport]

 # in one terminal
 ./xproto-proxy.pl --proxydir='proto' localhost:33059 yourmysqlserver:33060

 # in another terminal
 mysqlsh -u test_user -h localhost --js -P 33059
 mysql-js>

 # back to the proxy to see the decoded messages

=head1 OPTIONS

Defaults for server hosts are 'localhost' and default ports are
server: 33060 and proxy: 33059

=over 4

=item B<--connection-type> | B<-t>

Type of network connection. Defaults to 'tcp'. (Others are untested.)

=item B<--protoc> | B<-p>

Command to run `protoc`. Defaults to 'protoc'.

=item B<--proto-dir> | B<-d>

Directory holding MySQL's .proto files needed by `protoc`. Defaults to '.'.

=item B<--add-timestamp> | B<-T>

Prepend a timestamp to each message header. Defaults to false.

=item B<--forwarding> | B<-L>

For consistency with `ssh -L [bind_address:] port:host:hostport`, this parameter
can replace "proxyhost:proxyport [serverhost:serverport]".

=item B<--debug> | B<-D>

Enable debug output.

=item B<--help>

Prints a brief help message and exits.

=item B<--man>

Prints the manual page and exits.

=back

=head1 DESCRIPTION

This proxy sits between a MySQL server and a client using the X Protocol.
As messages pass from client to server and back, the messages are decoded
by `protoc` in a human-readable form such as:

 CLIENT: Mysqlx.Connection.CapabilitiesGet

 SERVER: Mysqlx.Connection.Capabilities
 capabilities {
  name: "authentication.mechanisms"
  value {
    type: ARRAY
    array {
      value {
        type: SCALAR
        scalar {
          type: V_STRING
          v_string {
            value: "MYSQL41"
          }
        }
      }
    }
  }
 }
 ....

This way you can debug your own client code, or see how for example mysqlsh
does certain things.

=cut
