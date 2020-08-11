#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Description of the module
:param -f OR --first_parameter: The description of your first input parameter
:returns: Whatever your script returns when called
:raises Exception if any issues are encountered
"""

# Put all your imports here, one per line.
# However multiple imports from the same lib are allowed on a line.
# Imports from Python standard libraries
import sys
import logging
import os

# Imports from the cheese factory
import camelot

# Imports from the local filesystem

# Put your constants here. These should be named in CAPS.

# Put your global variables here.

# Put your class definitions here. These should use the CapWords convention.

# Put your function definitions here. These should be lowercase, separated by underscores.


def scrape_tables_from_pdfs():
    script_dir = os.path.dirname(__file__)  # <-- absolute dir the script is in
    rel_path = "financial_pdfs/First Citizens Bank 2019 Consolidated.pdf"
    abs_file_path = os.path.join(script_dir, rel_path)
    tables = camelot.read_pdf(abs_file_path, flavor='stream', pages='3,4')
    pass


def main():
    """Docstring description for each function"""
    try:
        # All main code here
        scrape_tables_from_pdfs()
    except Exception:
        logging.exception("Error in script "+os.path.basename(__file__))
        sys.exit(1)
    else:
        logging.info(os.path.basename(__file__)+" executed successfully.")
        sys.exit(0)


# If this script is being run from the command-line, then run the main() function
if __name__ == "__main__":
    main()
