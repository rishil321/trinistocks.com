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
import sys
import logging
import git
import os
from pathlib import Path
import glob
 
# Put your constants here. These should be named in CAPS.

# Put your global variables here. 
 
# Put your class definitions here. These should use the CapWords convention.
 
# Put your function definitions here. These should be lowercase, separated by underscores.
def repo_pull_commit_push(repo_remote_url,repo_local_dir,repo_subdir_name,repo_subdir_files_extension):
    """Pull the repo at repo_url to get the latest data, add all files from the local repo,
    commit the changes, then push back to the remote"""
    # check to see if a repo has been init already
    try: 
        repo = git.Repo(repo_local_dir)
        logging.info("Git repo has already been created.")
    except (git.exc.InvalidGitRepositoryError,git.exc.NoSuchPathError):
        logging.info("No git repo has been initialized for this module. Cloning from github.com now.")
        git.Repo.clone_from(repo_remote_url,repo_local_dir)
        logging.info("Repo cloned successfully.")
        repo = git.Repo(repo_local_dir)
        # now we have a valid repo created 
    # pull the latest data from the repo
    origin = repo.remotes.origin
    origin.pull()
    logging.info("Repo from remote URL: "+repo_remote_url+" pulled successfully.")
    # create the subdirectory if it does not exist
    subdir_path = os.path.join(repo_local_dir,repo_subdir_name)
    Path(subdir_path).mkdir(parents=False, exist_ok=True)
    # get all files with the correct extension in this subdir
    all_subdir_files = glob.glob(subdir_path+os.path.sep+"*"+repo_subdir_files_extension)
    # add all files in this subdir to the repo index
    repo.index.add(all_subdir_files)
    logging.info("Added all "+repo_subdir_files_extension+"files from "+repo_subdir_name+" to repo index.")
    # set the commit message
    repo.index.commit("Automatic commit by "+os.path.basename(__file__))
    # git push 
    origin.push()
    logging.info("All indexed files pushed to remote repo at "+repo_remote_url+" successfully.")
    return subdir_path
    
def repo_pull(repo_remote_url,repo_local_dir):
    """Pull the remote repo at repo_url to get the latest data to the local repo"""
    # check to see if a repo has been init already
    try: 
        repo = git.Repo(repo_local_dir)
        logging.info("Git repo has already been created.")
    except (git.exc.InvalidGitRepositoryError,git.exc.NoSuchPathError):
        logging.info("No git repo has been initialized for this module. Cloning from github.com now.")
        git.Repo.clone_from(repo_remote_url,repo_local_dir)
        logging.info("Repo cloned successfully.")
        repo = git.Repo(repo_local_dir)
        # now we have a valid repo created 
    # pull the latest data from the repo
    origin = repo.remotes.origin
    origin.pull()
    logging.info("Repo from remote URL: "+repo_remote_url+" pulled successfully.")

def main():
	"""Each function should have a docstring description as well"""
	# Of course, you can also use inline comments like these wherever you want
	sys.exit(0) # Use 0 for normal exits, 1 for general errors and 2 for syntax errors (eg. bad input parameters)
 
if __name__ == "__main__":
	main()