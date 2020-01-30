# This script provides general methods to scrape for filenames in a certain directory.

#  imports
import argparse
import os
import glob
from typing import Optional, Match

import yaml


import re



# Define parser
parser = argparse.ArgumentParser(description="Scrape a directory for specified files")
# Define root directory
parser.add_argument(
    "--root",
    type=str,
    help="Path to root directory (default: CURRENT)",
)

# Define  yaml settings blueprint to be ammended with pathname
parser.add_argument(
    "--blueprint",
    type=str,
    help="Path to settings blueprint (default: CURRENT/blueprint.yml)",
)

    # Define types of files to look for, default only *.nc
parser.add_argument(
    "--fileextensions",
    nargs="+"
    , type=str,
    help="Extensions of which files should be searched, provided without dot, e.g. as nc, txt, csv (default:nc)"
)
# (parts) of filenames to look for

parser.add_argument(
    "--searchterms",
    nargs="+"
    ,type=str,
    help="Terms for which every file containing at least one of these should be searched"
)

# (parts) of filenames to look for

parser.add_argument(
    "--outputfile",
    nargs="+"
    ,type=str,
    help="Path and name to output file  (default: CURRENT/output.nc)"
)

args = parser.parse_args()

# default root directory
if not args.root:
    args.root = os.getcwd()

if not os.path.exists(args.root):
    exit("Model binary '{}' not found".format(args.model))


if not os.path.exists(args.root):
    exit("root directory '{}' not found".format(args.root))

# default setings blueprint
if not args.blueprint:
    args.blueprint = os.path.join(os.getcwd(),"blueprint.yml")

# default file extension to look for
if not args.fileextensions:
    args.fileextensions =  ("nc")

# default - empty- search term to look for
if not args.searchterms:
    args.searchterms =  (["pr","prsn","tas"])

# default setings output file
    if not args.outputfile:
        args.outputfile = os.path.join(os.getcwd(),"/output.yml")


# use glob to identify files

# loop over all search terms and file extensions, TBD extension for multiple variants:


numberOfSearchTerms  =3
numberOfFileExtensions =1

filenames = {}

for i_searchterm in range(len(args.searchterms)):
    filenames[args.searchterms[i_searchterm]]=(glob.glob(args.root+"/**/"+"*"+args.searchterms[i_searchterm]+"*.nc",recursive=True))

# load default settings file
with open(args.blueprint, 'r') as stream:
    try:
        settings = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)
searchresults = {}

#sort file paths
for i_searchterm in range(len(args.searchterms)):
    searchterm = args.searchterms[i_searchterm]
    results_for_searchterm=[]
    for i_filenames in range (len(filenames[searchterm])):
        searchresult = re.search(('.*'+searchterm+'(\_+)'), filenames[searchterm][i_filenames])
        if searchresult:
            results_for_searchterm.append(searchresult.string)
    searchresults[searchterm]=(results_for_searchterm)
# TODO implement auto grouping of paths


# save file paths
for i_searchterm in range(len(args.searchterms)):
    settings["input"][args.searchterms[i_searchterm]]= searchresults[args.searchterms[i_searchterm]]
# modify years TODO: implement auto recognition of years
    settings ["years"]["from"]=2095
    settings["years"]["to"] = 2099
# modify output file
    settings["output"]["file"] = args.outputfile
# save new settings file
with open("settings.yml", "w") as output:
        yaml.dump(settings, output)

