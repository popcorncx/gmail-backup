#!/usr/bin/perl -w

use strict;
use warnings;

use autodie;

use Data::Dumper;
use Config::Tiny;
use Mail::IMAPClient;
use File::Slurp qw{ write_file };
use Text::CSV;
use Carp;
use File::Basename qw{ dirname };
use IO::Compress::Gzip qw{ gzip $GzipError };
use Number::Format qw{ format_number };

STDOUT->autoflush( 1 );

my ($name) = @ARGV
	or die "no name\n";

my $config = Config::Tiny->read( dirname($0) . '/' . $name . '.ini' )
	or die Config::Tiny->errstr();

print "Username: $config->{'_'}->{'username'}\n";

my $OUTPUT_DIR = $config->{'_'}->{'directory'};

if ( not -d $OUTPUT_DIR )
{
	die "$OUTPUT_DIR is not a directory\n";
}

print "Directory: $OUTPUT_DIR\n";

my $imap = Mail::IMAPClient->new(
	'Server'           => $config->{'_'}->{'server'},
	'Port'             => 993,
	'Ssl'              => 1,
	'User'             => $config->{'_'}->{'username'},
	'Password'         => $config->{'_'}->{'password'},
	'Peek'             => 1,
	'IgnoreSizeErrors' => 1,
) or die $@;

print "Connected to $config->{'_'}->{'server'}\n";

#initialise seperation operator
my $LABEL_PARSER = Text::CSV->new(
	{
		'binary'   => 1,
		'sep_char' => q{ },
	}
);

# 1. retrieve headers for all messages, keyed on Google MSGID

# xlist will get us the names for the folders regardless of the language
my %xlist_folder = $imap->xlist_folders;

my %Message = ();

foreach my $folder ( @xlist_folder{ 'AllMail', 'Sent' } )
{
	print "Processing $folder\n";

	$imap->select( $folder )
		or croak $imap->LastError();

	# to get transparency into the header retrieval we get the all ids
	# and then request them in batches instead of having fetch_hash() do
	# that for us.

	# get reference return to be able to distinguish between no messages
	# and error searching for messages
	my $folder_ids = $imap->search( 'ALL' )
		or croak '$imap->search(\'ALL\'): ', $imap->LastError();

	print 'Messages in folder: ', format_number( scalar @{ $folder_ids }), "\n";;

	while ( @{ $folder_ids } )
	{
		my @batch_ids = splice @{ $folder_ids }, 0, 2000;

		printf(
			"Retrieving %s headers (%s remain)\n",
			format_number( scalar @batch_ids ),
			format_number( scalar @{ $folder_ids } ),
		);

		my $TYUI = $imap->fetch_hash(
			\@batch_ids,
			'RFC822.SIZE',
			'FLAGS',
			'INTERNALDATE',
			'X-GM-THRID',
			'X-GM-LABELS',
			'X-GM-MSGID',
		) or croak $imap->LastError();

		foreach my $id ( keys %{ $TYUI } )
		{
			if ( exists $Message{ $TYUI->{ $id }->{ 'X-GM-MSGID' } } )
			{
				delete $TYUI->{ $id };
			}
		}

		MESSAGE: while ( my ( $id, $msg ) = each %{ $TYUI } )
		{
			$msg->{ 'imap_id' }     = $id;
			$msg->{ 'imap_folder' } = $folder;

			# always include folder as a label
			$msg->{ 'labels' } = [ $folder ];

			if ( my $labels = $msg->{ 'X-GM-LABELS' } )
			{
				$LABEL_PARSER->parse( $imap->Unescape( $labels ) )
					or croak $LABEL_PARSER->error_diag();

				push @{ $msg->{ 'labels' } }, $LABEL_PARSER->fields();
			}

			$Message{ $msg->{ 'X-GM-MSGID' } } = $msg;
		}
	}

	print "Completed $folder\n";
}

print 'Total messages: ', format_number( scalar keys %Message ), "\n";

# 3. download messages to disk

my %Download = ();

foreach my $msg ( values %Message )
{
	my $subdir = substr $msg->{'X-GM-MSGID'}, -2;

	$msg->{ 'filename' } = "$OUTPUT_DIR/$subdir/$msg->{'X-GM-MSGID'}.msg.gz";

	if ( not -s $msg->{ 'filename' } )
	{
		push @{ $Download{ $msg->{ 'imap_folder' } } }, $msg;
	}
}

print 'Messages to download: ', format_number( scalar( map { @{ $_ } } values %Download) ), "\n";

my $downloaded_count = 0;

my $downloaded_size = 0;
my $stored_size = 0;

while ( my ( $folder, $messages ) = each %Download )
{
	print 'Downloading from ', $folder, ' messages: ', format_number( scalar @{$messages} ), "\n";

	my $folder_count = 0;

	$imap->select( $folder )
		or croak $imap->LastError();

	while ( my $msg = shift @{$messages} )
	{
		my $message_string = $imap->message_string( $msg->{ 'imap_id' } );

		if ( not $message_string )
		{
			print Dumper $msg;
			croak 'No message content, ', $imap->LastError(), "\n";
		}

		my $subdir = dirname( $msg->{ 'filename' } );

		if ( not ( -d $subdir || mkdir $subdir ) )
		{
			croak "could not create subdir, $!\n";
		}

#		write_file( $msg->{ 'filename' }, $message_string )
#			or croak "could not write $msg->{'filename'}, $!\n";

		gzip( \$message_string => $msg->{ 'filename' } )
			or croak "could not write $msg->{'filename'}, $GzipError\n";

		
		$downloaded_size += length $message_string;
		$stored_size     += -s $msg->{ 'filename' };

		$downloaded_count++;
		$folder_count++;

		if ( $folder_count % 100 == 0 )
		{
			printf(
				"%s messages, %s bytes downloaded, %s bytes stored, %s remain\n",
				format_number($folder_count), format_number($downloaded_size),
				format_number($stored_size), format_number(scalar @{ $messages }),
			);
		}
	}

	print "Completed downloading from $folder\n";
}

print "Downloaded $downloaded_count messages\n";

$imap->logout()
	or croak $imap->LastError();

# write out metadata

write_file(
	"$OUTPUT_DIR/meta.data",
	Dumper( \%Message )
) or croak "Could not write meta data, $!\n";

print "Metadata written\n";

# clean up messages on disk that are no longer in metadata

my %File = ();

foreach my $file ( glob( "$OUTPUT_DIR/*/*.msg.gz" ) )
{
	$File{$file} = 1;
}

foreach my $msg ( values %Message )
{
	if ( exists $File{ $msg->{ 'filename' } } )
	{
		delete $File{ $msg->{ 'filename' } };
	}
	else
	{
		die "Missing file: $msg->{ 'filename' }\n";
	}
}

if ( my @files = sort keys %File )
{
	printf( "Files to delete: %d\n", scalar @files );

	my $deleted = unlink @files;
	printf( "Files deleted: %d\n", $deleted );
}

