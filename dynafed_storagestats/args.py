"""Creates Help/Usage deals with arguments.

The parse_args() calls any other functions defined here to add the necessary
parser as sub-parsers. Any new sub-commands should be  a sub-parser.

"""

import argparse
import sys


#############
# Functions #
#############


def parse_args():
    """Generate Usage/Help with help from functions and parse the CLI arguments.

    Returns:
    argparse object.

    """
    # Get CLI arguments
    args = sys.argv[1:]

    # Initiate parser and subparser objects
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers()

    # Version
    parser.add_argument(
        '--version',
        action='store_true',
        default=False,
        dest='version',
        help="Print current installed version."
    )

    # Sub-command options argument sub-parsers
    add_checksums_subparser(subparser)
    add_reports_subparser(subparser)
    add_stats_subparser(subparser)

    # Print help if no arguments were passed
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    return parser.parse_args(args)


def add_general_options(parser):
    """Add general optional arguments used by any subcommand.

    Arguments:
    parser -- Object form argparse.ArgumentParser()

    """
    parser.add_argument(
        '-c', '--config',
        action='store',
        default=['/etc/ugr/conf.d'],
        dest='config_path',
        nargs='*',
        help="Path to UGRs endpoint .conf files or directories. "
             "Accepts any number of arguments. "
             "Default: '/etc/ugr/conf.d'."
    )
    parser.add_argument(
        '-f', '--force',
        action='store_true',
        default=False,
        dest='force',
        help="Force command execution."
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        default=False,
        dest='verbose',
        help="Show on stderr events according to loglevel."
    )


def add_checksums_subparser(subparser):
    """Add optional arguments for the 'checksums' sub-command.

    Arguments:
    subparser -- Object form argparse.ArgumentParser().add_subparsers()

    """
    # Initiate parser.
    parser = subparser.add_parser(
        'checksums',
        help="Obtain and output object/file checksums.."
    )
    subparser = parser.add_subparsers()

    # Set the sub-command routine to run.
    parser.set_defaults(cmd='checksums')

    # Add Sub-sub commands
    add_checkusms_get_subparser(subparser)
    add_checkusms_put_subparser(subparser)


def add_checkusms_get_subparser(subparser):
    """Add optional arguments for the 'checksums get' sub-command.

    Arguments:
    subparser -- Object form argparse.ArgumentParser().add_subparsers()

    """
    # Initiate parser.
    parser = subparser.add_parser(
        'get',
        help="Get object/file checksums."
    )

    # Set the sub-command routine to run.
    parser.set_defaults(sub_cmd='get')

    # General options
    add_general_options(parser)

    # Checksum options
    group_checksum = parser.add_argument_group("Checksum options. Required!")
    group_checksum.add_argument(
        '-e', '--endpoint',
        action='store',
        default=False,
        dest='endpoint',
        help="Choose endpoint containing desired object. "
             "Required."
    )
    group_checksum.add_argument(
        '-t', '--hash_type',
        action='store',
        default=False,
        dest='hash_type',
        type=str.lower,
        help="Type of checksum hash. ['adler32', md5] "
             "Required."
    )
    group_checksum.add_argument(
        '-u', '--url',
        action='store',
        default=False,
        dest='url',
        help="URL of object/file to request checksum of. "
             "Required."
    )

    # Logging options
    add_logging_options(parser)

    # Output Options
    group_output = parser.add_argument_group("Output options")

    group_output.add_argument(
        '--stdout',
        action='store_true',
        default=False,
        dest='output_stdout',
        help="Set to output stats on stdout."
    )


def add_checkusms_put_subparser(subparser):
    """Add optional arguments for the 'checksums put' sub-command.

    Arguments:
    subparser -- Object form argparse.ArgumentParser().add_subparsers()

    """
    # Initiate parser.
    parser = subparser.add_parser(
        'put',
        help="Set object/file checksums.."
    )

    # Set the sub-command routine to run.
    parser.set_defaults(sub_cmd='put')

    # General options
    add_general_options(parser)

    # Checksum options
    group_checksum = parser.add_argument_group("Checksum options. Required!")
    group_checksum.add_argument(
        '--checksum',
        action='store',
        default=False,
        dest='checksum',
        type=str.lower,
        help="String with checksum to set. ['adler32', md5] "
             "Required"
    )
    group_checksum.add_argument(
        '-e', '--endpoint',
        action='store',
        default=False,
        dest='endpoint',
        help="Choose endpoint containing desired object. "
             "Required."
    )
    group_checksum.add_argument(
        '-t', '--hash_type',
        action='store',
        default=False,
        dest='hash_type',
        type=str.lower,
        help="Type of checksum hash. ['adler32', md5] "
             "Required."
    )
    group_checksum.add_argument(
        '-u', '--url',
        action='store',
        default=False,
        dest='url',
        help="URL of object/file to request checksum of. "
             "Required."
    )

    # Logging options
    add_logging_options(parser)

    # Output Options
    group_output = parser.add_argument_group("Output options")

    group_output.add_argument(
        '--stdout',
        action='store_true',
        default=False,
        dest='output_stdout',
        help="Set to output stats on stdout."
    )


def add_logging_options(parser):
    """Add logging optional arguments.

    Arguments:
    parser -- Object form argparse.ArgumentParser()

    """
    # Logging options
    group_logging = parser.add_argument_group("Logging options")
    group_logging.add_argument(
        '--logfile',
        action='store',
        default='/tmp/dynafed_storagestats.log',
        dest='logfile',
        help="Set logfiles path. "
             "Default: /tmp/dynafed_storagestats.log"
    )
    group_logging.add_argument(
        '--logid',
        action='store',
        default=False,
        dest='logid',
        help='Add this log id to every log line.'
    )
    group_logging.add_argument(
        '--loglevel',
        action='store',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='WARNING',
        dest='loglevel',
        help="Set log output level. "
        "Default: WARNING."
    )


def add_reports_subparser(subparser):
    """Add optional arguments for the 'reports' sub-command.

    Arguments:
    subparser -- Object form argparse.ArgumentParser().add_subparsers()

    """
    # Initiate parser.
    parser = subparser.add_parser(
        'reports',
        help="Generate report files."
    )
    subparser = parser.add_subparsers()

    # Set the sub-command routine to run.
    parser.set_defaults(cmd='reports')

    # Add Sub-sub commands
    add_reports_filelist_subparser(subparser)
    add_reports_storage_subparser(subparser)


def add_reports_filelist_subparser(subparser):
    """Add optional arguments for the 'reports filelist' sub-command.

    Arguments:
    subparser -- Object form argparse.ArgumentParser().add_subparsers()

    """
    # Initiate parser.
    parser = subparser.add_parser(
        'filelist',
        help="Generate file-list related report."
    )

    # Set the sub-command routine to run.
    parser.set_defaults(sub_cmd='filelist')

    # General options
    add_general_options(parser)

    parser.add_argument(
        '-e', '--endpoint',
        action='store',
        default=[],
        dest='endpoint',
        nargs='*',
        help="Choose endpoint(s) to check. "
             "Accepts any number of arguments. "
             "If not present, all endpoints will be checked."
    )

    # Logging options
    add_logging_options(parser)

    # Reports options
    group_reports = parser.add_argument_group("Reports options")
    group_reports.add_argument(
        '--delta',
        action='store',
        default=1,
        dest='delta',
        type=int,
        help="Mask for Last Modified Date of files. Integer in days. "
             "Default: 1"
    )
    group_reports.add_argument(
        '--rucio',
        action='store_true',
        default=False,
        dest='rucio',
        help="Use to create rucio file dumps for consitency checks. "
             "Same as: --delta 1 --prefix rucio"
    )

    # Output Options
    group_output = parser.add_argument_group("Output options")
    # group_output.add_argument(
    #     '--debug',
    #     action='store_true',
    #     default=False,
    #     dest='debug',
    #     help="Declare to enable debug output on stdout."
    # )

    # group_output.add_argument(
    #     '-f', '--filename',
    #     action='store',
    #     default='report.txt',
    #     dest='report_filename',
    #     help="Set output filename. "
    #          "Default: 'report_filename'"
    # )

    group_output.add_argument(
        '-o', '--output-dir',
        action='store',
        default='.',
        dest='output_path',
        help="Set output directory. "
             "Default: '.'"
    )
    group_output.add_argument(
        '-p', '--path', '--prefix',
        action='store',
        default='',
        dest='prefix',
        help="Set the prefix/path from where to start the recursive list. "
             "The prefix is excluded from the resulting paths. Default: ''"
    )


def add_reports_storage_subparser(subparser):
    """Add optional arguments for the 'reports storage' sub-command.

    Arguments:
    subparser -- Object form argparse.ArgumentParser().add_subparsers()

    """
    # Initiate parser.
    parser = subparser.add_parser(
        'storage',
        help="Generate storage related report."
    )

    # Set the sub-command routine to run.
    parser.set_defaults(sub_cmd='storage')

    # General options
    add_general_options(parser)

    parser.add_argument(
        '-e', '--endpoint',
        action='store',
        default=[],
        dest='endpoint',
        nargs='*',
        help="Choose endpoint(s) to check. "
             "Accepts any number of arguments. "
             "If not present, all endpoints will be checked."
    )

    # Logging options
    add_logging_options(parser)

    # Memcached Options
    group_memcached = parser.add_argument_group("Memcached Options")
    group_memcached.add_argument(
        '--memhost',
        action='store',
        default='127.0.0.1',
        dest='memcached_ip',
        help="IP or hostname of memcached instance."
             "Default: 127.0.0.1"
    )
    group_memcached.add_argument(
        '--memport',
        action='store',
        default='11211',
        dest='memcached_port',
        help="Port of memcached instance. "
             "Default: 11211"
    )

    # Reports options
    group_reports = parser.add_argument_group("Reports options")
    group_reports.add_argument(
        '-s', '--schema',
        action='store',
        default=False,
        dest='schema',
        help="YAML file containing site schema. Required."
    )
    group_reports.add_argument(
        '--wlcg',
        action='store_true',
        default=False,
        dest='wlcg',
        help="Produces WLCG JSON output file. Requires setup file."
    )

    # Output Options
    group_output = parser.add_argument_group("Output options")
    # group_output.add_argument(
    #     '--debug',
    #     action='store_true',
    #     default=False,
    #     dest='debug',
    #     help="Declare to enable debug output on stdout."
    # )

    # group_output.add_argument(
    #     '-f', '--filename',
    #     action='store',
    #     default='report.txt',
    #     dest='report_filename',
    #     help="Set output filename. "
    #          "Default: 'report_filename'"
    # )

    group_output.add_argument(
        '-o', '--output-dir',
        action='store',
        default='.',
        dest='output_path',
        help="Set output directory. "
             "Default: '.'"
    )


def add_stats_subparser(subparser):
    """Add optional arguments for the 'stats' sub-command.

    Arguments:
    subparser -- Object form argparse.ArgumentParser().add_subparsers()

    """
    # Initiate parser.
    parser = subparser.add_parser(
        'stats',
        help="Obtain and output storage stats."
    )

    # Set the sub-command routine to run.
    parser.set_defaults(cmd='stats')

    # General options
    add_general_options(parser)

    parser.add_argument(
        '-e', '--endpoint',
        action='store',
        default=[],
        dest='endpoint',
        nargs='*',
        help="Choose endpoint(s) to check. "
             "Accepts any number of arguments. "
             "If not present, all endpoints will be checked."
    )

    # Logging options
    add_logging_options(parser)

    # Memcached Options
    group_memcached = parser.add_argument_group("Memcached Options")
    group_memcached.add_argument(
        '--memhost',
        action='store',
        default='127.0.0.1',
        dest='memcached_ip',
        help="IP or hostname of memcached instance."
             "Default: 127.0.0.1"
    )
    group_memcached.add_argument(
        '--memport',
        action='store',
        default='11211',
        dest='memcached_port',
        help="Port of memcached instance. "
             "Default: 11211"
    )

    # Output Options
    group_output = parser.add_argument_group("Output options")
    group_output.add_argument(
        '--debug',
        action='store_true',
        default=False,
        dest='debug',
        help="Declare to enable debug output on stdout."
    )
    group_output.add_argument(
        '-m', '--memcached',
        action='store_true',
        default=False,
        dest='output_memcached',
        help="Declare to enable uploading storage stats to memcached."
    )
    group_output.add_argument(
        '-o', '--output-dir',
        action='store',
        default='.',
        dest='output_path',
        help="Set output directory for flags -j, -x and -p. "
             "Default: '.'"
    )
    group_output.add_argument(
        '-p', '--plain',
        action='store',
        const="dynafed_storagestats.txt",
        default=False,
        dest='to_plaintext',
        nargs='?',
        help="Set to output stats to plain txt file. Add argument to set filename."
             "Default: dynafed_storagestats.txt"
    )
    group_output.add_argument(
        '--stdout',
        action='store_true',
        default=False,
        dest='output_stdout',
        help="Set to output stats on stdout."
    )

    # group_output.add_argument(
    #     '-x', '--xml',
    #     action='store',
    #     const="dynafed_storagestats.xml",
    #     default=False,
    #     dest='output_xml',
    #     nargs='?',
    #     help="Set to output stats to json file. Add argument to set filename."
    #          "Default: dynafed_storagestats.json"
    #          "!!In development!!"
    # )
