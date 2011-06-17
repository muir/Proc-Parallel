
package RPC::ToWorker::Callback;

use strict;
use warnings;
require Exporter;
use Storable qw(freeze thaw);

our @EXPORT = qw(master_call);
our @ISA = qw(Exporter);

our $master;

sub master_call
{
	my ($packages, $func, $with, @args) = @_;

	local($0) = $0;

	$0 =~ s/: RUNNING/: making RPC call on master to $func/g;

	my $pkgs = ref($packages)
		? $packages
		: [ split(' ', $packages) ];
	$with = ref($with)
		? $with
		: [ split(' ', $with) ];

	my $args = freeze(\@args);
	printf $master "DATA %d CALL %s with %s after loading %s\n%s", length($args), $func, "@$with", "@$pkgs", $args
		or die "print to master: $!";

	die if $func =~ /\s/;
	die unless $func =~ /\S/;

	my $ds =<$master>;
	die unless $ds =~ /^DATA (\d+) DONE_RESPONSE\n/;
	my $amt = $1;
	my $buf = '';
	while (length($buf) < $amt) {
		read($master, $buf, $amt - length($buf), length($buf)) or die;
	}
	my $ret = thaw($buf);
	return @$ret;
}

1;

__END__

=head1 SYNOPSIS

 use RPC::ToWorker::Callback;

 @return_values = master_call('Packages To::Preload', 'remove_function_name', 'remote local data keys', $data);

=head1 DESCRIPTION

Make a remote call to a function on the master node from a 
slave process started with L<RPC::ToWorker>.

The slaves are running sychronously, but the master is asychronous.

=head1 LICENSE

This package may be used and redistributed under the terms of either
the Artistic 2.0 or LGPL 2.1 license.

