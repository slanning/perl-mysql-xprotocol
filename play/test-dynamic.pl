#!/usr/bin/env perl
$0 = 'test-dynamic.pl';
# https://dev.mysql.com/worklog/task/?id=8639
use strict;
use warnings;
use autodie;
use Data::Dumper; { package Data::Dumper; our ($Indent, $Sortkeys, $Terse, $Useqq) = (1)x4 }
use DBI;
use Digest::SHA1;
use Dumbbench;
use Getopt::Long;
use IO::Socket::INET qw//;
use Term::ReadKey;

# This is how Mysqlx.pm was generated (I think Google::ProtocolBuffers::Generated needs to be installed):
# The .proto files were from:
# https://github.com/mysql/mysql-server/tree/5.7/rapid/plugin/x/protocol
# protoc --perl-gpd_out=package=Mysqlx,pb_prefix=Mysqlx,prefix=Mysqlx:lib --proto_path=/home/slanning/xprotocol /home/slanning/xprotocol/*.proto
# (Note: protoc is too dumb to deal with relative paths)
# perl -Ilib -MMysqlx -E 'Mysqlx::Connection::Capability->new'
use lib "./lib"; use Mysqlx;

# filled in in load_protobuf:
my (%FIELD_TYPE, %SCALAR_TYPE, %ANY_TYPE, %CLIENT_MESSAGE, %SERVER_MESSAGE,
    %NOTICE_FRAME_SCOPE, %NOTICE_WARNING_LEVEL, %NOTICE_SESSIONSTATECHANGED_PARAMETER,
    );

# copied from the mysqlx_resultset.proto documentation...
my %CONTENT_TYPE = (
    1 => 'GEOMETRY',
    2 => 'JSON',
    3 => 'XML',
);
# copied from the mysqlx_notice.proto documentation...
my %NOTICE_TYPE = (
    1 => 'Warning',
    2 => 'SessionVariableChanged',
    3 => 'SessionStateChanged',
);
# found in mysql-server/rapid/plugin/x/src/expect.cc
# and copied+used in rapid/unittest/gunit/xplugin/expect_noerror_t.cc
# no idea if/where it's documented
my $EXPECT_NO_ERROR = 1;

my $AUTHENTICATION_MECH_NAME = 'MYSQL41';
my $BYTES_FIELD_SEPARATOR = "\0";

my $COMPARE_WITH_OLDSCHOOL = 1;

$|++;
my $OPT = cli_params();
main();
exit;

sub main {
    load_protobuf();

    if ($OPT->{benchmark}) {
        run_benchmark();
    }
    else {
        my $host = $OPT->{hostname}->[0];
        my $sock = make_socket($host, $OPT->{port});

        #capabilities($sock);
        authenticate_mysql41($sock);
        #stmt_execute($sock);
        #pipeline($sock);
        #expect($sock);
        #expect_pipeline($sock);
        crud($sock);

        $sock->close();
    }
}


sub crud {
    my ($sock) = @_;

    die "implement me!";
}

sub make_socket {
    my ($host, $port) = @_;

    my $sock = IO::Socket::INET->new(
        PeerHost => $host,
        PeerPort => $port,
        Proto    => 'tcp',
    ) or die "Couldn't create socket: $!";
    return $sock;
}

sub run_benchmark {
    my $iterations          = 1000;
    my $rejectoutliers      = 0;

    my $bench = Dumbbench->new(
        target_rel_precision => 0.005,          # seek ~0.5%
        initial_runs         => $iterations,    # the higher the more reliable
        ($rejectoutliers) ? () : ( outlier_rejection => 1e9 ),
    );

    foreach my $host (@{ $OPT->{hostname} }) {
        my $sock = make_socket($host, $OPT->{port});

        authenticate_mysql41($sock);

        $bench->add_instances(
            Dumbbench::Instance::PerlSub->new(
                name => sprintf("%30s:%s", "pipeline", $host),
                code => sub {
                    my $result = test_cross_dc_pipeline($sock);
                    1;
                }
            )
            );
        $bench->add_instances(
            Dumbbench::Instance::PerlSub->new(
                name => sprintf("%30s:%s", "non_pipelined", $host),
                code => sub {
                    my $result = test_cross_dc_non_pipelined($sock);
                    1;
                }
            )
        );
        if ($COMPARE_WITH_OLDSCHOOL) {
            my $dsn = "dbi:mysql:host=$host;database=$OPT->{database}";
            my $dbh = DBI->connect($dsn, $OPT->{username}, $OPT->{password});
            $dbh->{mysql_auto_reconnect} = 1;   # O.O

            $bench->add_instances(
                Dumbbench::Instance::PerlSub->new(
                    name => sprintf("%30s:%s", "DBD::mysql", $host),
                    code => sub {
                        my $result = test_cross_dc_oldschool($dbh);
                        1;
                    }
                )
            );
        }
    }


    $bench->run;
    # overridden to display "unscientific notation"
    #$bench->report();
    report($bench);
}

# modified from Dumbbench
sub report {
  my $self = shift;
  my $raw = shift;
  foreach my $instance ($self->instances) {
    my $result = $instance->result;
    my $str_result = unscientific_notation( $result );

    if (not $raw) {
      my $mean = $result->raw_number;
      my $sigma = $result->raw_error->[0];
      my $name = $instance->_name_prefix;
      printf(
        "%sRan %u iterations (%u outliers).\n",
        $name,
        scalar(@{$instance->timings}),
        scalar(@{$instance->timings})-$result->nsamples
      );
      printf(
        "%sRounded run time per iteration: %s (%.1f%%)\n",
        $name,
        "$str_result",
        $sigma/$mean*100
      );
      if ($self->verbosity) {
        printf("%sRaw:                            $mean +/- $sigma\n", $name);
      }
    }
    else {
      print $result, "\n";
    }
  }
}

sub unscientific_notation {
    sprintf("%f %s %f", split(/ /, $_[0]));
}

sub expect_pipeline {
    my ($sock) = @_;

    my $callback = sub {
        send_message_stmt_execute($sock);
        send_message_stmt_execute($sock);
    };

    expect($sock, $callback);
    handle_stmt_execute($sock, 2);
}

sub handle_expect {
    my ($sock) = @_;

    my $ok = 0;
    while (my $recv = receive_message($sock)) {
        my $type = $recv->{type};
        if (server_type_is('ERROR', $type)) {
            my $decoded_payload = decode_payload('Mysqlx::Error', $recv->{payload});
            die "Server error (ERROR): ", Dumper($decoded_payload);
        }
        elsif (server_type_is('OK', $type)) {
            my $decoded_payload = decode_payload('Mysqlx::Ok', $recv->{payload});
            print "OK\n";
            $ok = 1;
            last;
        }
        elsif (server_type_is('NOTICE', $type)) {
            my $decoded_payload = decode_payload('Mysqlx::Notice::Frame', $recv->{payload});
            print "Notice frame: ", Dumper($decoded_payload) if $OPT->{debug};
            my $notice_payload = _decode_notice($decoded_payload);
            print Dumper($notice_payload) if $OPT->{debug};
        }
        else {
            die "unhandled Expect message '$type'";
        }
    }

    return $ok;
}

sub expect {
    my ($sock, $callback) = @_;

    my $cond_open = Mysqlx::Expect::Open::Condition->new({
        condition_key => $EXPECT_NO_ERROR,
        # condition_value => 10,   # presumably for unimplemented EXPECT_GTID_WAIT_LESS_THAN ?
        op => Mysqlx::Expect::Open::Condition::ConditionOperation::EXPECT_OP_SET,
    });
    my $open = Mysqlx::Expect::Open->new({cond => [ $cond_open ]});

    send_message_object($sock, $open, 'EXPECT_OPEN');

    $callback->() if $callback;

    my $close = Mysqlx::Expect::Close->new();

    send_message_object($sock, $close, 'EXPECT_CLOSE');
    handle_expect($sock);
}

sub pipeline {
    my ($sock) = @_;

    send_message_stmt_execute($sock);
    send_message_stmt_execute($sock);

    # How do you properly distinguish what's returned by the server?
    # Otherwise, it's handled the same.

    handle_stmt_execute($sock, 2);
}

sub handle_stmt_execute {
    my ($sock, $number_queries) = @_;

    my $resultsets_received = 0;
    my @column_metadata;
    while (my $recv = receive_message($sock)) {
        # FIXME: need to improve this..

        if (server_type_is('RESULTSET_COLUMN_META_DATA', $recv->{type})) {
            my $decoded_payload = decode_payload('Mysqlx::Resultset::ColumnMetaData', $recv->{payload});
            print Dumper($decoded_payload) if $OPT->{debug};
            # this could be further processed, esp. flags

            push @column_metadata, $decoded_payload;
        }
        elsif (server_type_is('RESULTSET_ROW', $recv->{type})) {
            my $decoded_payload = decode_payload('Mysqlx::Resultset::Row', $recv->{payload});

            my $row = $decoded_payload->get_field_list;
            my $decoded_columns = _decode_columns($row, \@column_metadata);
            print "DECODED FIELDS: ", Dumper($decoded_columns) if $OPT->{debug};
        }
        elsif (server_type_is('RESULTSET_FETCH_DONE', $recv->{type})) {
            my $decoded_payload = decode_payload('Mysqlx::Resultset::FetchDone', $recv->{payload});
            # ...
        }
        elsif (server_type_is('NOTICE', $recv->{type})) {
            my $decoded_payload = decode_payload('Mysqlx::Notice::Frame', $recv->{payload});
            print "Notice frame: ", Dumper($decoded_payload) if $OPT->{debug};
            my $notice_payload = _decode_notice($decoded_payload);
            print Dumper($notice_payload) if $OPT->{debug};
        }
        elsif (server_type_is('ERROR', $recv->{type})) {
            my $decoded_payload = decode_payload('Mysqlx::Error', $recv->{payload});
            die "Server error (ERROR): ", Dumper($decoded_payload);
        }
        elsif (server_type_is('SQL_STMT_EXECUTE_OK', $recv->{type})) {
            my $decoded_payload = decode_payload('Mysqlx::Sql::StmtExecuteOk', $recv->{payload});
            print "OK\n" if $OPT->{debug};

            ++$resultsets_received;
            @column_metadata = ();

            last if $resultsets_received >= $number_queries;
        }
        else {
            die "unknown message in stmt_execute";
        }
    }
}

sub test_cross_dc_oldschool {
    my ($dbh) = @_;

    my @rows;
    for (1 .. 100) {
        my $id = 1 + int(rand(6));  # there are six rows in the table
        my $row = $dbh->selectall_arrayref(
            "select * from site_type where id = $id",
        );
        push @rows, $row->[0];
    }

    #print Dumper(\@rows) ;
    #die sprintf("not enough rows (%s)", scalar(@rows)) unless @rows == $n;
}

sub test_cross_dc_non_pipelined {
    my ($sock) = @_;

    # FIXME:
    my %scalar_type_by_name = reverse %SCALAR_TYPE;
    my %any_type_by_name = reverse %ANY_TYPE;

    my $n = 100;
    for (1 .. $n) {
        my $id = 1 + int(rand(6));  # there are six rows in the table
        send_message_payload($sock, 'Mysqlx::Sql::StmtExecute', 'SQL_STMT_EXECUTE', {
            stmt => "select * from site_type where id = $id",
        });

        handle_test_pipeline($sock, 1);
    }
}


sub test_cross_dc_pipeline {
    my ($sock) = @_;

    # FIXME:
    my %scalar_type_by_name = reverse %SCALAR_TYPE;
    my %any_type_by_name = reverse %ANY_TYPE;

    my $n = 100;
    for (1 .. $n) {
        my $id = 1 + int(rand(6));  # there are six rows in the table
        send_message_payload($sock, 'Mysqlx::Sql::StmtExecute', 'SQL_STMT_EXECUTE', {
            stmt => "select * from site_type where id = $id",
        });
    }

    handle_test_pipeline($sock, $n);
}

sub handle_test_pipeline {
    my ($sock, $n) = @_;

    my $queries_received = 0;
    my @column_metadata;
    my @rows;
    while (my $recv = receive_message($sock)) {
        if (server_type_is('RESULTSET_COLUMN_META_DATA', $recv->{type})) {
            my $decoded_payload = decode_payload('Mysqlx::Resultset::ColumnMetaData', $recv->{payload});
            print Dumper($decoded_payload) if $OPT->{debug};
            # this could be further processed, esp. flags

            push @column_metadata, $decoded_payload;
        }
        elsif (server_type_is('RESULTSET_ROW', $recv->{type})) {
            my $decoded_payload = decode_payload('Mysqlx::Resultset::Row', $recv->{payload});

            my $row = $decoded_payload->get_field_list;
            my $decoded_columns = _decode_columns($row, \@column_metadata);
            print "DECODED FIELDS: ", Dumper($decoded_columns) if $OPT->{debug};
            push @rows, $decoded_columns;
        }
        elsif (server_type_is('RESULTSET_FETCH_DONE', $recv->{type})) {
            my $decoded_payload = decode_payload('Mysqlx::Resultset::FetchDone', $recv->{payload});
            # ...
        }
        elsif (server_type_is('NOTICE', $recv->{type})) {
            my $decoded_payload = decode_payload('Mysqlx::Notice::Frame', $recv->{payload});
            print "Notice frame: ", Dumper($decoded_payload) if $OPT->{debug};
            my $notice_payload = _decode_notice($decoded_payload);
            print Dumper($notice_payload) if $OPT->{debug};
        }
        elsif (server_type_is('ERROR', $recv->{type})) {
            my $decoded_payload = decode_payload('Mysqlx::Error', $recv->{payload});
            die "Server error (ERROR): ", Dumper($decoded_payload);
        }
        elsif (server_type_is('SQL_STMT_EXECUTE_OK', $recv->{type})) {
            my $decoded_payload = decode_payload('Mysqlx::Sql::StmtExecuteOk', $recv->{payload});
            print "OK\n" if $OPT->{debug};

            ++$queries_received;
            @column_metadata = ();

                print "queries_received==$n\n" if $OPT->{debug};
            if ($queries_received >= $n) {
                last;
            }
        }
        else {
            die "unknown message in stmt_execute";
        }
    }

    print Dumper(\@rows) if $OPT->{debug};
    die sprintf("not enough rows (%s)", scalar(@rows)) unless @rows == $n;
}

# FIXME: there's also a CapabilitiesSet
sub capabilities {
    my ($sock) = @_;

    send_message_payload($sock, 'Mysqlx::Connection::CapabilitiesGet', 'CON_CAPABILITIES_GET', {});

    while (my $recv = receive_message($sock)) {
        if (server_type_is('CONN_CAPABILITIES', $recv->{type})) {
            my $cap = decode_payload('Mysqlx::Connection::Capabilities', $recv->{payload});
            my $capa = $cap->get_capabilities_list();
            foreach my $capability (@$capa) {
                my $name = $capability->get_name();
                my $value = $capability->get_value();

                # FIXME: for some reason, value is randomly missing....
                my $decoded_value = decode_any($value);
                print Dumper({ name => $name, decoded_value => decode_any($value) });
                #printf("name: %s, value: %s\n", $name, $decoded_value->{value}{value}{value}{value});
                # yay?
            }

            last;
        }
        elsif (server_type_is('ERROR', $recv->{type})) {
            my $decoded_payload = decode_payload('Mysqlx::Error', $recv->{payload});
            die "Server error (ERROR): ", Dumper($decoded_payload);
        }
        else {
            die "unhandled message type '$recv->{type}'";
        }
    }
}

sub load_protobuf {
    # FIXME: should make something more general to introspect all enums
    %FIELD_TYPE                           = _map_enum_id_to_name('Mysqlx::Resultset::ColumnMetaData', 'type');
    %SCALAR_TYPE                          = _map_enum_id_to_name('Mysqlx::Datatypes::Scalar', 'type');
    %ANY_TYPE                             = _map_enum_id_to_name('Mysqlx::Datatypes::Any', 'type');
    %NOTICE_FRAME_SCOPE                   = _map_enum_id_to_name('Mysqlx::Notice::Frame', 'scope');
    %NOTICE_WARNING_LEVEL                 = _map_enum_id_to_name('Mysqlx::Notice::Warning', 'level');
    %NOTICE_SESSIONSTATECHANGED_PARAMETER = _map_enum_id_to_name('Mysqlx::Notice::SessionStateChanged', 'param');

    my $client_messages_ed = Mysqlx::ClientMessages::Type->enum_descriptor;
    %CLIENT_MESSAGE = reverse %{ $client_messages_ed->values };
    my $server_messages_ed = Mysqlx::ServerMessages::Type->enum_descriptor;
    %SERVER_MESSAGE = reverse %{ $server_messages_ed->values };

    return;
}

sub _map_enum_id_to_name {
    my ($class, $field_name) = @_;

    my $md = $class->message_descriptor;
    my $fd = $md->find_field_by_name($field_name);
    my $ed = $fd->enum_type;
    return reverse %{ $ed->values };
}

# scraped together from mysqlx_resultset.proto
# and mysql-connector-nodejs/lib/Protocol/Datatype.js
# https://developers.google.com/protocol-buffers/docs/encoding
sub _decode_columns {
    my ($row, $meta) = @_;
    return unless @$row;

    my @decoded;

    for (my $i = 0; $i < @$meta; ++$i) {
        my $col = $row->[$i];
        my $col_meta = $meta->[$i];

        my $type = $col_meta->get_type();
        die sprintf("invalid column type '%s'", $type)
          unless exists $FIELD_TYPE{$type};
        my $type_name = $FIELD_TYPE{$type};

        # FIXME: float and double don't display the right number of decimal places
        # example float: -43.390998840332 instead of -43.391
        # I know floats are like that but the mysql client displays it correctly...
        if ($type_name eq 'FLOAT') {
            my ($f) = unpack('f', $col);   # how to decode "Protobuf's float"?
            # $f = sprintf('%.' . $col_meta->{fractional_digits} . 'f', $f);
            push @decoded, $f;
        }
        elsif ($type_name eq 'DOUBLE') {
            my $d = unpack('d', $col);   # how to decode "Protobuf's double"?
            # $d = sprintf('%.' . $col_meta->{fractional_digits} . 'f', $d);
            push @decoded, $d;
        }
        elsif ($type_name eq 'SINT') {
            my @bytes = unpack('C*', $col);
            my $current = _read_varint64(\@bytes, 0, 1);
            push @decoded, $current->{value};
        }
        elsif ($type_name eq 'UINT') {
            my @bytes = unpack('C*', $col);
            my $current = _read_varint64(\@bytes, 0, 0);
            push @decoded, $current->{value};
        }
        elsif ($type_name eq 'BIT') {
            my @bytes = unpack('C*', $col);

            my $current = _read_varint64(\@bytes, 0, 0);

            # this makes it a string like "10101010101"
            # but maybe we should return $current->{value} directly
            push @decoded, sprintf('%0' . $col_meta->{length} . 'b', $current->{value});
        }
        elsif ($type_name =~ /^(BYTES|ENUM)$/) {
            chop($col) if defined $col;

            # FIXME: is ENUM handled correctly?

            # FIXME: should handle also GEOMETRY and XML type
            my $type = $col_meta->get_content_type();
            if (content_type_is('JSON', $type)) {
                # nodejs connector parses the JSON in this case
            }

            # FIXME: I guess I need to check the 'charset' part of the 'type';
            # I'm sure that will be fun....
            # FIXME: I also didn't handle "rightpad"
            push @decoded, $col;
        }
        elsif ($type_name eq 'TIME') {
            # FIXME: it displays "-08:43:54" instead of "-32:43:54"
            # (like it's modulo 24h); mysql client displays it properly...
            # http://dev.mysql.com/doc/refman/5.7/en/time.html
            push @decoded, _decode_datetime_or_time($col, 1);
        }
        elsif ($type_name =~ 'DATETIME') {
            my $datetime = _decode_datetime_or_time($col, 0);
            # DATE needs length in order to not display ' 00:00:00'
            $datetime = bytes::substr($datetime, 0, $col_meta->get_length());
            push @decoded, $datetime;
        }
        elsif ($type_name =~ 'DECIMAL') {
            push @decoded, _decode_decimal($col);
        }
        elsif ($type_name =~ 'SET') {
            push @decoded, _decode_set($col);
        }
        else {
            die "unknown type for column $i";
        }
    }

    return \@decoded;
}

sub _decode_set {
    my ($value) = @_;
    my @bytes = unpack("C*", $value);
    return 'NULL' unless @bytes;
    return '' if @bytes == 1 and $bytes[0] == 1;   # empty set

    my @element;
    while (@bytes) {
        my $len = shift(@bytes);

        if ($len == 0) {
            push @element, "''";   # empty string
        }
        else {
            push @element, pack("C$len", splice(@bytes, 0, $len));
        }
    }

    return join(',', @element);
}

sub _decode_decimal {
    my ($value) = @_;
    my @bytes = unpack('C*', $value);

    my ($even, $sign, $retval);
    if ($bytes[-1] & 0x0F) {
        $even = 0;
        $retval = (($bytes[-1] & 0x0F) == 0x0C) ? '' : '-';
    }
    else {
        $even = 1;
        $retval = (($bytes[-1] & 0xF0) == 0xC0) ? '' : '-';
    }

    my $total = 2 * (@bytes - 2) + ($even ? 0 : 1);
    my $comma_pos = $total - $bytes[0];
    for (my $digit = 0; $digit < $total; ++$digit) {
        my $offset = 1 + ($digit >> 1);
        $retval .= '.' if $digit == $comma_pos;
        $retval .= ($digit & 0x01) ? ($bytes[$offset] & 0x0F) : ($bytes[$offset] >> 4);
    }

    # FIXME: for some reason Dumper thinks this is a string
    return( 0.0 + $retval );
}

sub _decode_datetime_or_time {
    my ($value, $timeonly) = @_;
    return unless defined $value;

    my @bytes = unpack('C*', $value);
    my $pos = 0;
    my $retval = '';
    $retval = '-' if $timeonly and $bytes[$pos++] != 0;

    foreach my $prefix (@{ $timeonly ? ["", ":", ":"] : ["", "-", "-", " ", ":", ":"] }) {
        return if @bytes < $pos;

        # FIXME: according to mysqlx_resultset.proto DATETIME spec
        # "hour, minutes, seconds, useconds are optional if all the values to the right are 0"
        # though that seems bogus... (seems to be for DATE, but doesn't that count "00:00:00.000000"?
        #last if $prefix eq " " and all {$_ == 0} @bytes[$pos .. $#bytes];

        my $current = _read_varint64(\@bytes, $pos, 0);
        my $currentv = $current->{value};
        $retval .= $prefix . ($currentv < 10 ? ('0' . $currentv) : $currentv);
        $pos += $current->{length};
    }

    if (@bytes > $pos) {
        # usec padding
        my $current = _read_varint64(\@bytes, $pos, 0);
        my $currentv = $current->{value};

        $retval .= sprintf(".%06d", $currentv);
    }

    return $retval;
}

sub _decode_notice {
    my ($notice) = @_;

    my $scope = $notice->{scope};
    my $scope_name = defined($scope)
      ? $NOTICE_FRAME_SCOPE{$scope}
      : 'GLOBAL';
    die "unknown Notice scope '$scope'" unless defined $scope_name;

    my $type = $notice->{type};
    my $type_name = $NOTICE_TYPE{$type}
      or die "unknown Notice type '$type'";

    my %processed_payload;
    my $payload = $notice->{payload};
    $processed_payload{raw_payload} = $payload;
    if ($payload) {
        if ($type_name eq 'Warning') {                     # local or global
            my $decoded_payload = decode_payload('Mysqlx::Notice::Warning', $payload);
            $processed_payload{$_} = $decoded_payload->{$_}
              for qw/code msg/;

            $processed_payload{level} //= 2;
            $processed_payload{level_name} = $NOTICE_WARNING_LEVEL{$processed_payload{level}}
              or die sprintf("unknown Notice Warning level '%s'", $processed_payload{level});
        }
        elsif ($type_name eq 'SessionVariableChanged') {   # local
            my $decoded_payload = decode_payload('Mysqlx::Notice::SessionVariableChanged', $payload);

            $processed_payload{param} = $decoded_payload->{param};

            $processed_payload{value} = decode_scalar($decoded_payload->{value})
              if $decoded_payload->{value};
        }
        elsif ($type_name eq 'SessionStateChanged') {      # local
            my $decoded_payload = decode_payload('Mysqlx::Notice::SessionStateChanged', $payload);

            $processed_payload{param} = $decoded_payload->{param};

            $processed_payload{param_name} = $NOTICE_SESSIONSTATECHANGED_PARAMETER{$processed_payload{param}}
              or die sprintf("unknown Notice SessionStateChanged param '%s'", $processed_payload{param});

            $processed_payload{value} = decode_scalar($decoded_payload->{value})
              if $decoded_payload->{value};
        }
        else {
            die "unhandled Notice type '$type_name'";
        }
    }

    return {
        scope      => $scope,
        scope_name => $scope_name,
        type       => $type,
        type_name  => $type_name,
        payload    => \%processed_payload,
    };
}

sub decode_any {
    my ($obj) = @_;

    my $type = $obj->get_type();
    my $type_name = $ANY_TYPE{$type}
      or die "invalid Any type '$type'";
    my %ret = (
        type => $type,
        type_name => $type_name,
    );

    my $class = ref($obj);
    my $md = $class->message_descriptor;
    my $value_field = $md->find_field_by_number($type + 1);

    if ($value_field) {
        $ret{value_name} = $value_field->name;
        my $get_field = "get_" . $ret{value_name};

        my $value = $obj->$get_field;

        # FIXME: should make a dispatch hash
        # FIXME: why is there sometimes only 'type' sent...?
        if ($value) {
            no strict 'refs';
            my $decoder = "decode_" . $ret{value_name};

            print "decode_any decoder: $decoder get_field: $get_field value: $value obj: ", Dumper($obj);

            $ret{value} = &$decoder($value);
        }

        # value might need to be further decoded?
    }

    return \%ret;
}

# similar to decode_any
sub decode_scalar {
    my ($obj) = @_;

    my $type = $obj->get_type();
    my $type_name = $SCALAR_TYPE{$type}
      or die "invalid Scalar type '$type'";
    my %ret = (
        type => $type,
        type_name => $type_name,
    );

    my $class = ref($obj);
    my $md = $class->message_descriptor;
    my $value_field = $md->find_field_by_number($type + 1);

    if ($value_field) {
        $ret{value_name} = $value_field->name;
        my $get_field = "get_" . $ret{value_name};

        # value might need to be further decoded
        $ret{value} = $obj->$get_field;
    }

    return \%ret;
}

# FIXME: not tested
sub decode_object {
    my ($obj) = @_;

    my $fields = $obj->get_fld_list();

    my @ret;

    foreach my $field (@$fields) {
        my $any = decode_any($field);

        push @ret, {
            key => $field->get_key(),
            value => $any,
        };
    }

    return \@ret;
}

# FIXME: not tested
sub decode_array {
    my ($obj) = @_;

    print "decode_array: ", join(':',caller()), $/;
    my $any_fields = $obj->get_value_list();

    my @ret;

    foreach my $any_field (@$any_fields) {
        my $any = decode_any($any_field);
        push @ret, $any;
    }

    return \@ret;
}

sub _read_varint64 {
    my ($bytes, $offset, $signed) = @_;
    my $pos = $offset || 0;

    my $result = 0;

    my $shift = 0;
    my $byte;
    LOOP: {
        do {
            $byte = $bytes->[$pos];
            last unless defined $byte;
            $result += ($byte & 0x7F) << $shift;
            $shift += 7;
            ++$pos;
        } while ($byte >= 0x80);
    }

    # http://stackoverflow.com/questions/19758270/read-varint-from-linux-sockets
    if ($signed) {
        use integer;  # makes perl interpret $result as a signed int
        $result = ($result & 1) ? ~($result >> 1) : ($result >> 1);
    }

    return { length => ($pos - $offset), value => $result };
}

sub encode_stmt_args {
    my ($args) = @_;

    #my %scalar_type_by_name = reverse %SCALAR_TYPE;
    my %any_type_by_name = reverse %ANY_TYPE;

    my @encoded;

    foreach my $arg (@$args) {
        # mysqlx_datatypes.proto
        my ($type, $field, $val);
        if (defined($arg)) {
            $type  = 8;      # V_STRING
            $field = 'v_string';
            $val   = Mysqlx::Datatypes::Scalar::String->new({
                value => "$arg",
            });
        }
        else {
            $type  = 3;      # V_NULL
            $field = undef;
        }
        my $scalar  = Mysqlx::Datatypes::Scalar->new({
            type => $type,
            (defined($field) ? ($field => $val) : ()),
        });
        my $any = Mysqlx::Datatypes::Any->new({
            type => $any_type_by_name{SCALAR},
            scalar => $scalar,
        });

        push @encoded, $any;
    }

    return \@encoded;
}

sub send_message_stmt_execute {
    my ($sock) = @_;

    # FIXME: how to deal with string vs number? (maybe a parameter; here V_UINT and v_unsigned_int are hardcoded)
    my $args = encode_stmt_args([1234567890, 234567890]);

    # FIXME: test SQL error and so on

    my $stmtexec = Mysqlx::Sql::StmtExecute->new({
        stmt => "select * from field_test where my_int_u in (?, ?)",
        # using aref works for 'repeated'! (can also use add_args)
        args => $args,

        # namespace => 'bleh',
        # compact_metadata => 0,
    });

    send_message_object($sock, $stmtexec, 'SQL_STMT_EXECUTE');
}

sub stmt_execute {
    my ($sock) = @_;

    send_message_stmt_execute($sock);
    handle_stmt_execute($sock, 1);

    print "done with stmt_execute\n";
}

# is there any way to set the schema
# other than authentication?
sub authenticate_mysql41 {
    my ($sock) = @_;

    # sucks that there's not a message (class) to message_type mapping
    send_message_payload($sock, 'Mysqlx::Session::AuthenticateStart', 'SESS_AUTHENTICATE_START', {
        mech_name => $AUTHENTICATION_MECH_NAME,
    });

    # https://dev.mysql.com/doc/internals/en/x-protocol-authentication-mysql41-authentication.html
    # https://github.com/go-sql-driver/mysql/blob/master/utils.go#L88
    # http://www.hiemalis.org/~keiji/Network/userdb-test-cram-sha1

    # obviously need some improved handling here :P
    my $response;

  RECV:
    my $recv = receive_message($sock);
    if ($recv) {
        if (server_type_is('SESS_AUTHENTICATE_CONTINUE', $recv->{type})) {
            my $decoded_payload = decode_payload('Mysqlx::Session::AuthenticateContinue', $recv->{payload});
            $response = _response_to_challenge($decoded_payload->{auth_data}, $OPT->{username}, $OPT->{password}, $OPT->{database});
            send_message_payload($sock, 'Mysqlx::Session::AuthenticateContinue', 'SESS_AUTHENTICATE_CONTINUE', {
                auth_data => $response,
            });

            goto RECV;
        }
        elsif (server_type_is('NOTICE', $recv->{type})) {
            my $decoded_payload = decode_payload('Mysqlx::Notice::Frame', $recv->{payload});
            print "Notice frame: ", Dumper($decoded_payload) if $OPT->{debug};
            my $notice_payload = _decode_notice($decoded_payload);
            print Dumper($notice_payload) if $OPT->{debug};

            goto RECV;
        }
        elsif (server_type_is('SESS_AUTHENTICATE_OK', $recv->{type})) {
            my $decoded_payload = decode_payload('Mysqlx::Session::AuthenticateOk', $recv->{payload});
            print "AuthenticateOk: ", Dumper($decoded_payload)
              if $OPT->{debug};
        }
        elsif (server_type_is('ERROR', $recv->{type})) {
            my $decoded_payload = decode_payload('Mysqlx::Error', $recv->{payload});
            die "Server error (ERROR): ", Dumper($decoded_payload);
        }
        else {
            die "server message type '$recv->{type}' unknown";
        }
    }
    else {
        print "didn't recv from socket\n";
        # retry?
    }
}

sub _response_to_challenge {
    my ($challenge, $username, $password, $database) = @_;

    my $sha = Digest::SHA1->new();

    $sha->add($password);

    my $hash1 = $sha->digest();

    $sha->add($hash1);
    my $hash2 = $sha->digest();

    $sha->add($challenge);
    $sha->add($hash2);
    my $hash3 = $sha->digest();

    my $xor = "$hash3" ^ "$hash1";

    my $hex = _bytes_to_hex($xor);

    # '*' is some MySQL server thing, PVERSION41_CHAR
    my $resp = $database ? $database : '';
    $resp .= $BYTES_FIELD_SEPARATOR . $username . $BYTES_FIELD_SEPARATOR . '*' . $hex;
    return $resp;
}

sub _bytes_to_hex {
    my ($bytes) = @_;
    my $len = bytes::length($bytes);
    return join('', map(sprintf('%02x', $_), unpack("C$len", $bytes)));
}

sub _bytes_to_bits {
    my ($bytes) = @_;
    my $len = bytes::length($bytes);
    return join('', map(sprintf('%08b', $_), unpack("C$len", $bytes)));
}

sub send_message_object {
    my ($sock, $obj, $type) = @_;

    print "send_message_object: ", Dumper({type => $type, obj => $obj})
      if $OPT->{debug};

    my $class = ref($obj);
    my $payload = $class->encode($obj);
    my $message = encode_message($type, $payload);
    my $num_sent = $sock->send($message);
    return $num_sent;
}

sub send_message_payload {
    my ($sock, $message_class, $type, $payload) = @_;
    $payload //= {};

    print "send_message_payload: $message_class $type ", Dumper($payload)
      if $OPT->{debug};

    my $encoded_payload = $message_class->encode($payload);
    my $encoded_message = encode_message($type, $encoded_payload);

    my $num_sent = $sock->send($encoded_message);
    return $num_sent;
}

sub encode_message {
    my ($type, $payload) = @_;
    my $length       = pack('L', bytes::length($payload) + 1);
    my $message_type = pack('C', client_message_type($type));

    return( $length . $message_type . $payload );
}

sub decode_payload {
    my ($message_class, $payload) = @_;
    my $decoded_payload = $message_class->decode($payload);
    return $decoded_payload;
}

sub client_message_type {
    my ($type) = @_;
    return _message_constant('Mysqlx::ClientMessages::Type', $type);
}

sub server_message_type {
    my ($type) = @_;
    return _message_constant('Mysqlx::ServerMessages::Type', $type);
}

sub content_type_is {
    my ($expected_type, $type_id) = @_;
    return unless $type_id;   # most of the time $type_id == 0
    my $type_name = $CONTENT_TYPE{$type_id};
    die "unknown ColumnMetaData content_type '$type_id'" unless defined $type_name;
    return($type_name eq $expected_type);
}

sub notice_type_is {
    my ($expected_type, $type_id) = @_;
    my $type_name = $NOTICE_TYPE{$type_id};
    die "unknown Notice type '$type_id'" unless defined $type_name;
    return($type_name eq $expected_type);
}

sub _message_constant {
    my ($message_class, $type) = @_;
    no strict 'refs';
    my $const_name = $message_class . '::' . $type;
    return &$const_name();
}

sub server_type_is {
    my ($expected_type, $got_type) = @_;
    return( $got_type == server_message_type($expected_type) );
}

sub receive_message {
    my ($sock) = @_;

    # Message header (length: uint32, type: uint8)
    my $header  = _read_bytes_from_socket($sock, 5);
    my ($length, $type) = unpack('LC', $header);

    # $length - 1 because $length includes $type
    my $payload = '';
    $payload = _read_bytes_from_socket($sock, $length - 1)
      if $length > 1;    # don't read if there's no payload

    my $type_text = $SERVER_MESSAGE{$type}
      or die "server message type '$type' unknown";
    my $ret = {
        len            => $length,
        type           => $type,
        type_text      => $type_text,
        payload        => $payload,
        payload_length => bytes::length($payload),
        payload_hex    => _bytes_to_hex($payload),
    };
    print "receive_message: ", Dumper($ret)
      if $OPT->{debug};
    return $ret;
}

sub _read_bytes_from_socket {
    my ($sock, $num_bytes_to_read) = @_;

    my $max_tries = 10;

    my $bytes;
    while ($num_bytes_to_read > 0) {
        my $buf;
        my $num_read = $sock->read($buf, $num_bytes_to_read);
        if (defined $num_read) {
            $bytes .= $buf;
            $num_bytes_to_read -= $num_read;
        }
        else {
            die "error reading from socket: $!";
        }

        if (--$max_tries <= 0) {
            die "max tries exceeding reading from socket";
        }
    }

    return $bytes;
}

# copied from nodejs-connector tests
sub test_auth {
    my $challenge1 = pack('C20',
                         0x0a, 0x35, 0x42, 0x1a,
                         0x43, 0x47, 0x6d, 0x65,
                         0x01, 0x4a, 0x0f, 0x4c,
                         0x09, 0x5c, 0x32, 0x61,
                         0x64, 0x3c, 0x13, 0x06,);
    my $username = 'root';
    my $password = 'fff';

    my $data1 = _response_to_challenge($challenge1, $username, $password);
    print "data1: ", $data1, $/;

    use bytes;

    my @bytes1 = unpack('C*', $data1);

    # \0 + USERNAME + \0 + * + 2*20 (2 hex chars per byte)
    die("response should have the right size")
        if length($data1) != (1 + length($username) + 1 + 1 + 40);

    die("response should begin with 0 byte")
        if $bytes1[0] != 0;

    die("response should contain username")
        if substr($data1, 1, length($username)) ne $username;

    die("response should have 0 byte after username")
        if $bytes1[length($username) + 1] != 0;

    my $expected1 = "*34439ed3004cf0e6030a9ec458338151bfb4e22d";
    my $got1 = substr($data1, 1 + length($username) + 1, 1 + 2*20);
    die("response should be hashed properly for hash 1")
        if $got1 ne $expected1;


    my $challenge2 = pack('C20',
                          0x41, 0x43, 0x56, 0x6e,
                          0x78, 0x19, 0x2c, 0x2c,
                          0x19, 0x6f, 0x18, 0x29,
                          0x05, 0x52, 0x3c, 0x62,
                          0x39, 0x3d, 0x5c, 0x77,);
    my $data2 = _response_to_challenge($challenge2, $username, $password);
    print "data2: ", $data2, $/;

    my $expected2 = "*af1ef523d254181abb1155c1fbc933b80c2ec853";
    my $got2 = substr($data2, 1 + length($username) + 1, 1 + 2*20);
    die("response should be hashed properly for hash 2")
        if $got2 ne $expected2;

    my $challenge3 = pack('C20',
                          0x7a, 0x59, 0x6b, 0x6e,
                          0x19, 0x7f, 0x44, 0x01,
                          0x6f, 0x4a, 0x0f, 0x0f,
                          0x3e, 0x19, 0x50, 0x4c,
                          0x4f, 0x47, 0x53, 0x5b,);
    my $data3 = _response_to_challenge($challenge3, $username, $password);
    print "data3: ", $data2, $/;

    my $expected3 = "*950d944626109ab5bce8bc56a4e78a296e34271d";
    my $got3 = substr($data3, 1 + length($username) + 1, 1 + 2*20);
    die("response should be hashed properly for hash 3")
        if $got3 ne $expected3;
}

sub cli_params {
    my %opt;
    $opt{'debug|D'}         = { default => 0,       type => '!'   };
    $opt{'hostname|h'}      = { default => '',      type => ':s@' };
    $opt{'username|u'}      = { default => '',      type => '=s'  };
    $opt{'password|p'}      = { default => '',      type => ':s'  };
    $opt{'database|d'}      = { default => '',      type => ':s'  };
    $opt{'port|P'}          = { default => 33060,   type => ':i'  };
    $opt{'benchmark|b'}     = { default => 0,       type => '!'   };
    $opt{'authfile|f'}      = { default => '',      type => ':s'  };
    $opt{help}              = { default => 0,       type => '|?'  };
    $opt{man}               = { default => 0,       type => ''    };
    GetOptions(
        map {
            ( "$_$opt{$_}{type}" => \ ($opt{$_} = $opt{$_}{default}) );    # attn: evil
        } keys %opt
    ) or pod2usage(2);
    pod2usage(1) if $opt{help};
    pod2usage(-exitstatus => 0, -verbose => 2) if $opt{man};

    # get rid of shortcut
    my $regex = qr/\|[a-zA-Z]$/;
    foreach my $key (grep { $_ =~ $regex } keys %opt) {
        (my $simple_key = $key) =~ s/$regex//;
        $opt{$simple_key} = delete($opt{$key});
    }

    if ($opt{authfile}) {
        # has to be exact
        open(my $fh, $opt{authfile});
        while (<$fh>) {
            chomp;
            my ($opt, $value) = split(/\A\s*([^\s=])+\s*=\s*([^\s=])+\s*$/);

            if (defined($opt) and defined($value)) {
                $opt{$opt} = $value
                    unless defined($opt{$opt});
            }
            else {
                die "opt? opt:$opt value:$value";
            }
        }
        close($fh);
    }

    $opt{hostname} ||= [ 'localhost' ];

    unless ($opt{password}) {
        $opt{password} = _get_password();
    }

    return \%opt;
}

sub _get_password {
    my $pass;

    ReadMode(3);
    while (ord(my $key = ReadKey(0)) != 10) {
        if (ord($key) eq 8 || ord($key) == 127) {
            chop($pass);
            print "\b \b";
        }
        elsif (ord($key) < 32) {
            # skip
        }
        else {
            $pass .= $key;
        }
    }
    ReadMode(0);

    unless ($pass) {
        print "\n";
        exit;
    }

    return $pass;
}



__END__

sub encode_stmt_arg {
    my ($arg, $typename) = @_;
    $typename //= 'V_UINT';  # ?

    my %scalar_type_by_name = reverse %SCALAR_TYPE;
    my %any_type_by_name    = reverse %ANY_TYPE;

    # convenience shortcuts
    $typename = 'V_' . $typename unless $typename =~ /^V_/;
    $typename = uc($typename);
    $typename = 'V_NULL' unless defined $arg;
    my $type = $scalar_type_by_name{$typename};

    my $field_name;
    unless ($typename eq 'V_NULL') {
        my $class = ref($scalar);
        my $md = $class->message_descriptor;
        my $value_field = $md->find_field_by_number($type + 1);
        $field_name = $value_field->name;
    }
    my $scalar  = Mysqlx::Datatypes::Scalar->new({
        type => $type,
        ($field_name ? ($field_name => $arg) : ()),
    });
    my $any = Mysqlx::Datatypes::Any->new({
        type => $any_type_by_name{SCALAR},
        scalar => $scalar,
    });

    return $any;
}
