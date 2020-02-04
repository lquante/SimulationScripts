# This script provides general methods to scrape for filenames in a certain directory.
#  imports
import argparse
import os
from pathlib import Path
import re
from ruamel.yaml import ruamel

# Define parser
parser = argparse.ArgumentParser(description="Scrape a directory for specified files")
# Define root directory
parser.add_argument(
    "--root",
    type=str,
    help="Path to root directory (default: CURRENT)",
)

# Define  yaml settings blueprint to be amended with pathname
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
    , type=str,
    help="Terms for which every file containing at least one of these should be searched"
)

# (parts) of filenames to look for

parser.add_argument(
    "--outputfile",
    nargs="+"
    , type=str,
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
    args.blueprint = os.path.join(os.getcwd(), "blueprint.yml")

# default file extension to look for
if not args.fileextensions:
    args.fileextensions = "nc"

# default -  search terms to look for TODO: generalize search term default via blueprint.yml
if not args.searchterms:
    args.searchterms = (["pr", "prsn", "tas"])

    # default settings output file
    if not args.outputfile:
        args.outputfile = os.getcwd()
# loop over all search terms and file extensions, TBD extension for multiple variants
filenames = {}
filecache = []
for i_searchterm in args.searchterms:
    for filename in Path(args.root).rglob((i_searchterm + "_" + "*.nc")):
        filecache.append(str(filename))
    filenames[i_searchterm] = filecache
# load default settings file
yaml = ruamel.yaml.YAML()
with open(args.blueprint, 'r') as stream:
    try:
        settings = yaml.load(stream)
    except yaml.YAMLError as exc:
        print(exc)
# define sets to store results
timeperiods = set()
searchresults = {}  # TODO implement completeness check for results, i.e. relax assumption that always all search variables can be found
# find relevant directories for each search term
for i_searchterm in args.searchterms:
    searchpattern = ('(?<=/).*' + i_searchterm + '(\_+)+.*\d{8}-\d{8}\.nc')
    # TODO: relax assumption, that all data is unique with respect to timespan, NB: since directories are searched consecutivly, some (sufficient?!) ordering occurs by directories
    for i_filenames in filenames[i_searchterm]:
        filepath = re.search(searchpattern, i_filenames)
        if filepath:
            filepath_string = filepath.string
            time_period = re.search('(\d{4})(\d{4})(-)(\d{4})(\d{4})(.nc)$', filepath_string)
            #regular expression to get model identifier from filename, TODO: check for generalization, ATM using sspXXX as end of model identification, where XXX is a three digit number
            model = re.search('(.*/)(\w*_\w*_)(.*_ssp\d{3})(_\w*_\w*_)(\d{4}\d{4}-\d{4}\d{4}.nc)$', filepath_string)
            if (model):
                model_string = model.group(3)
            else:
                model_string = "model_not_identified"
            start_year = int(time_period.group(1))
            final_year = int(time_period.group(4))
            timespan = (start_year, final_year)
            timeperiods.add(timespan)
            # TODO: relax assumption, that all data ends with YYYYMMDD - YYYYMMDD.nc encoded date and only whole years are relevant for the data, with the same time period for all data
            searchresults[start_year, final_year, i_searchterm] = {"file": filepath_string, "model": model_string}
timeperiods_list = list(timeperiods)
# create setting files for all timespans and searchterms, TODO: check for simplification, redundancy reduction
timespan_iterator = 0
pathcollection = []
for i_timespan in timeperiods_list:
    timeindex = str(i_timespan[0]) + str(i_timespan[1])
    start_year = i_timespan[0]
    final_year = i_timespan[1]
    for i_searchterm in args.searchterms:
        # check if key exists
        if (start_year, final_year, i_searchterm) in searchresults.keys():
            # get model name for first searchterm
            settings["input"]["model"] = searchresults[start_year, final_year,i_searchterm]["model"]
            # check if model is identical to previous searchterms
            i_model = searchresults[start_year, final_year, i_searchterm]["model"]
            if (settings["input"]["model"] == i_model):
                # write filenames for searchterm
                settings["input"][i_searchterm] = searchresults[start_year, final_year, i_searchterm]["file"]
                # modify years
                settings["years"]["from"] = start_year
                settings["years"]["to"] = final_year
            else:
                print(i_model + "does not match" + settings["input"]["model"] + " - no settings written!")
        # modify output file
        outputfilename = str("output_" + i_model +"_"+ timeindex + ".nc")
        settings["output"]["file"] = os.path.join(args.outputfile[0], outputfilename)
    # save new settings file
    name_settings = "settings_" + i_model + "_" + timeindex + ".yml"
    yaml = ruamel.yaml.YAML()
    yaml.default_flow_style = None
    with open(name_settings, "w") as output:
        yaml.dump(settings, output)
    # collect paths to settings
    pathcollection.append(os.path.join(os.getcwd(), name_settings))
    timespan_iterator += 1
# create *.yml file of settingsfiles:
yaml = ruamel.yaml.YAML()
yaml.default_flow_style = None
with open("list_of_settings.yml", "w") as output:
    yaml.dump(pathcollection, output)
