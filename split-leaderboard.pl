#!/usr/bin/perl
#
# Usage: ./circle.py leaderboard 1000 --full-urls | ./split-leaderboard.pl
#
use warnings;
use strict;

my @names = ('active.md', 'betrayed.md', 'most-circles.md');
my $i = -1;
my $fh = undef;
while (<>) {
    # Switch to a new file at each leaderboard heading
    if ( /^#/ ) {
        $i++;
        close($fh) if $fh;
        my $fn = "leaderboards/$names[$i]";
        open($fh, '>', $fn) or die "$fn: $!";
    }
    print $fh $_ if $fh;
}
