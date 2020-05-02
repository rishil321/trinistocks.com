#!/usr/bin/env python3
# -*- coding: utf-8 -*-
 
"""
Description of this module/script goes here
:param -f OR --first_parameter: The description of your first input parameter
:param -s OR --second_parameter: The description of your second input parameter 
:returns: Whatever your script returns when called
:raises Exception if any issues are encountered
"""
 
# Put all your imports here, one per line. However multiple imports from the same lib are allowed on a line.
import requests # external lib; need to pip install
import sys
 
# Put your constants here. These should be named in CAPS.

# Put your global variables here. 
 
# Put your class definitions here. These should use the CapWords convention.
 
# Put your function definitions here. These should be lowercase, separated by underscores.
 
def main():
    """Each function should have a docstring description as well"""
    # Of course, you can also use inline comments like these wherever you want
    api_response = requests.get('https://icanhazdadjoke.com/',headers={"Accept":"application/json"})
    print(api_response.text)
    sys.exit(0) # Use 0 for normal exits, 1 for general errors and 2 for syntax errors (eg. bad input parameters)
 
if __name__ == "__main__":
	main()