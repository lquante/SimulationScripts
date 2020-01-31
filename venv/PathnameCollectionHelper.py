# This script provides general methods to scrape for filenames in a certain directory.

#  imports
import argparse
import os
import glob
from array import array
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
    help="Path and name to output file  (default: CURRENT/output_IDENTIFIER.nc)"
)

args = parser.parse_args()

# default root directory
if not args.root:
    args.root = os.getcwd()

if not os.path.exists(args.root):
    exit("root directory '{}' not found".format(args.root))

# default settings blueprint
if not args.blueprint:
    args.blueprint = os.path.join(os.getcwd(),"blueprint.yml")

# default file extension to look for
if not args.fileextensions:
    args.fileextensions =  ("nc")

# default -  search terms to look for TODO: generalize search term default via blueprint.yml
if not args.searchterms:
    args.searchterms =  (["pr","prsn","tas"])

# default setings output file
    if not args.outputfile:
        args.outputfile = os.getcwd()



# loop over all search terms and file extensions, TBD extension for multiple variants
filenames = {}
for i_searchterm in args.searchterms:
    filenames[i_searchterm]=(glob.glob(args.root+"/**/"+"*"+i_searchterm+"*.nc",recursive=True))
# load default settings file
with open(args.blueprint, 'r') as stream:
    try:
        settings = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)
# define sets to store results
directories = set()
timeperiods = set()
searchresults = {} #TODO implement completness check for results, i.e. relax assumption that always all search variables can be found
#find relevant directories for each search term
for i_searchterm in args.searchterms:
    results_for_searchterm=[]
    searchpattern = ('(?<=/).*' + i_searchterm + '(\_+)+.*\d{8}-\d{8}\.nc')
    # get directory path to group files accordingly, TODO: relax assumption, that all data to be used together is stored in the same directory
    for i_filenames in filenames[i_searchterm]:

            directory = re.search('.*/(?=' + searchpattern +')',
                                      i_filenames)
            if directory:
                directory_string = directory.group()
                directories.add(directory_string)
        #store all filepaths in each directory
            for i_directories in directories:
                filepath= re.search(searchpattern, i_filenames)
                if filepath:
                    filepath_string = filepath.string
                    time_period = re.search('(\d{4})(\d{4})(-)(\d{4})(\d{4})(.nc)$',filepath_string)
                    start_year = int(time_period.group(1))
                    final_year = int(time_period.group(4))
                    timespan = (start_year,final_year)
                    timeperiods.add(timespan)
                    # TODO: relax assumption, that all data ends with YYYYMMDD - YYYYMMDD.nc encoded date and only whole years are relevant for the data, with the same time period for all data
                    searchresults[directory_string,str(timespan),i_searchterm] = {"filepath":filepath_string}
timeperiods_list = list(timeperiods)

# create setting files for all directories, timespans and searchterms, TODO: check for simplification, redundancy reduction
directory_iterator = 0
pathcollection = []
for i_directories in directories:
    for i_timespan in timeperiods_list:
        index = "_dir"+str(directory_iterator) + "_" + str(i_timespan[0]) + str(i_timespan[1])
        for i_searchterm in args.searchterms:
            if (i_directories,str(i_timespan),i_searchterm) in searchresults.keys():
                settings["input"][i_searchterm]= searchresults[i_directories,str(i_timespan),i_searchterm]["filepath"]
            # modify years
                settings ["years"]={"years":{"from":i_timespan[0],"to":i_timespan[1]}}

        # modify output file
        print(args.outputfile)
        settings["output"]["file"] = os.path.join(args.outputfile,"output"+index+".nc")
        # save new settings file
        name_settings = "settings"+index+".yml"
        with open(name_settings, "w") as output:
            yaml.dump(settings, output)

        # collect paths to settings
        pathcollection.append(os.path.join(os.getcwd(),name_settings))

    directory_iterator += 1
# create *.yml file of settingsfiles:
with open("collected_settings", "w") as output:
    yaml.dump(pathcollection, output)
