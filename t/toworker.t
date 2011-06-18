#!/usr/bin/perl

use strict;
use warnings;
use FindBin;
use Test::More;
use File::Temp;
use File::Slurp;
use RPC::ToWorker;
use IO::Event qw(emulate_Event);
use Scalar::Util qw(reftype);
use Cwd;

my $finished = 0;
my $skip = 0;

END { ok($finished, 'finished') unless $skip }

use File::Slurp::Remote;

my $rhost = `$File::Slurp::Remote::SmartOpen::ssh localhost -n hostname`;
my $lhost = `hostname`;

unless ($lhost eq $rhost) {
	$skip = 1;
	plan skip_all => 'Cannot ssh to localhost';
	exit;
}

import Test::More qw(no_plan);

my $timer;
sub set_bomb 
{
	$timer = IO::Event->timer(
		after	=> 10,
		cb	=> sub {
			ok(0, "bomb timer went off, something failed");
			exit 0;
		},
	);
}

sub clear_bomb
{
	$timer->cancel;
	undef $timer;
}

my $test_sets_done = 0;
my $tests_expected = 0;

sub run_test
{
	set_bomb();
	IO::Event::loop();
	clear_bomb();
	$tests_expected++;
}
	
do_remote_job(
	host		=> 'localhost',
	eval		=> 'return (13, 7)',
	when_done	=> sub {
		my (@retval) = @_;
		is(scalar(@retval), 2, "basic test returned two values");
		is($retval[0], 13, "first value right");
		is($retval[1], 7, "second value right");
		$test_sets_done++;
		IO::Event::unloop_all();
	},
);

run_test();

do_remote_job(
	data		=> [ 3, 7, 22 ],
	preload		=> ['List::Util', 'Scalar::Util', 'Cwd'],
	host		=> 'localhost',
	chdir		=> cwd(),
	eval		=> <<'REMOTE_CODE',
				my (@values) = @{$_[0]};
				my $sum = List::Util::sum(@values);
				my $is_num = Scalar::Util::looks_like_number($sum);
				return [$sum, $is_num, cwd()]
REMOTE_CODE
	when_done	=> sub {
		my (@retval) = @_;
		is(scalar(@retval), 1, "round trip values");
		is(reftype($retval[0]), 'ARRAY', 'got an array back');
		eval {
			is($retval[0][0], 32, "sum of data");
			ok($retval[0][1], "is a num");
			is($retval[0][2], cwd(), "cwd");
		};
		ok(! $@, "no eval errors");
		$test_sets_done++;
		IO::Event::unloop_all();
	},
);

run_test();

is($test_sets_done, $tests_expected, "all tests run");

$finished = 1;

