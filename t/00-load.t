use strict;
use warnings;
use Test::Lib;
use Test2::V0;

ok(lives { require PAGI::Channels }, 'require PAGI::Channels') or diag($@);
ok(lives { require PAGI::Middleware::Channels::Backend }, 'require PAGI::Middleware::Channels::Backend') or diag($@);

done_testing;
