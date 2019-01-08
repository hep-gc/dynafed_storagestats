"""
Example for function to add in args.py to add more sub-commands.
"""

def add_example_subparser(subparser):
    """
    Optional arguments for the 'example' sub-command.
    """
    # Initiate parser.
    parser = subparser.add_parser(
        'example',
        help="Some useful help."
    )

    # Set the cmd to execute so the runner what the user is trying to run.
    parser.set_defaults(cmd='example')

    # Add any argparse statements for any options required by this sub-command.
    # Example
    parser.add_argument(
        '-c', '--config',
        action='store',
        default=['/etc/ugr/conf.d'],
        dest='config_path',
        nargs='*',
        help="Path to UGR's endpoint .conf files or directories. " \
             "Accepts any number of arguments. " \
             "Default: '/etc/ugr/conf.d'."
    )
