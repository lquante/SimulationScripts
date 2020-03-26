# This script provides some assistance to scrape for filenames in a certain directory and generate a *.yml settings file
#  imports
import argparse
import os
import re
from pathlib import Path

from ruamel.yaml import ruamel

# Define parser
parser = argparse.ArgumentParser(description="Scrape a directory for specified files")
# Define root directory
parser.add_argument(
    "--root",
    type=str,
    help="Path to root directory, which shall be scraped including subdirectories (default: CURRENT)",
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

#  scenarios to look for in filenames

parser.add_argument(
    "--scenarios",
    nargs="+"
    , type=str,
    help="Scenarios for which every file containing at least one of these should be searched"
)

# path to store settingsfiles

parser.add_argument(
    "--settingsdir"
    , type=str,
    help="Path to create settings directory (default: CURRENT)"
)

parser.add_argument(
    "--outputdir"
    , type=str,
    help="Path and name to create output directory (default: settingsdir)"
)

# variants for execution
parser.add_argument("--isimip", action="store_true", help="follow ISIMIP naming conventions to identify model")

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
    args.fileextensions = ["nc"]

# default -  search terms to look for TODO: generalize scenario default via blueprint.yml, make scenario search robust (ATM non-existing scenario results might be filled up with data from previous scenarios
if not args.scenarios:
    args.scenarios = (["ssp126", "ssp585"])

# default -  search terms to look for TODO: generalize search term default via blueprint.yml
if not args.searchterms:
    args.searchterms = (["pr", "prsn", "tas"])

# default settings for path to put settingsfiles
if not args.settingsdir:
    args.settingsdir = os.getcwd()

# default settings output file
if not args.outputdir:
    args.outputdir = args.settingsdir

settingsdir = os.path.join(args.settingsdir, "settings")
outputdir = os.path.join(args.outputdir, "output")
# loop over all search terms and file extensions, TBD extension for multiple variants
filenames = {}
filecache = []
for i_scenario in args.scenarios:
    for i_searchterm in args.searchterms:
        for filename in Path(args.root).rglob("*.nc"):
            if filename.rglob(i_searchterm + "*.nc"):
                if Path(args.root).rglob((i_scenario + "_" + "*.nc")):
                    filecache.append(str(filename))
            filenames[i_scenario, i_searchterm] = filecache
# load settings file
yaml = ruamel.yaml.YAML()
with open(args.blueprint, 'r') as stream:
    try:
        settings = yaml.load(stream)
    except yaml.YAMLError as exc:
        print(exc)
# define someobjects to store results
timeperiods = set()
models = set()
searchresults = {}  # TODO implement completeness check for results, i.e. relax assumption that always all search variables can be found
# find files for each scenario
for i_scenario in args.scenarios:
    # find files for each search term
    scenariopattern = ('(?<=/).*' + i_scenario + '.*' + args.fileextensions[0])
    # NB: reasonable assumption, that all data is unique with respect to scenariopattern
    for i_searchterm in args.searchterms:
        searchpattern = ('(?<=/).*' + i_searchterm + '(_+)' + '.*' + args.fileextensions[0])
        for i_filenames in filenames[i_scenario, i_searchterm]:
            filepath_scenario = re.search(scenariopattern, i_filenames)
            if filepath_scenario:
                scenario_string = filepath_scenario.string
                filepath_searchterm = re.search(searchpattern, scenario_string)
                if filepath_searchterm:
                    searchterm_string = filepath_searchterm.string
                    # regular expression to get model identifier from filename under ISIMIP3b conventions

                    if (args.isimip == True):
                        time_period = re.search('(\d{4})(_)(\d{4})(.nc)$', searchterm_string)
                        if (time_period):
                            start_year = int(time_period.group(1))
                            final_year = int(time_period.group(3))
                        model = re.search('(.*/)(.*)(_r)(.*_)(\d{4}.\d{4}.nc)$',
                                          searchterm_string)
                        if (model):
                            model_string = model.group(2)
                        else:
                            model_string = "model_not_identified"

                        # regular expression to get model identifier from filename under CMIP6 conventions
                    else:
                        time_period = re.search('(\d{4})(\d{4})(.)(\d{4})(\d{4})(.nc)$', searchterm_string)
                        if (time_period):
                            start_year = int(time_period.group(1))
                            final_year = int(time_period.group(4))
                        model = re.search(
                            '(.*/)(\w*_\w*_)(.*' + i_scenario + ')(_\w*_\w*_)(\d{4}\d{4}-\d{4}\d{4}.nc)$',
                            searchterm_string)
                        if (model):
                            model_string = model.group(3)
                        else:
                            model_string = "model_not_identified"

                    models.add(model_string)
                    timespan = (start_year, final_year)
                    timeperiods.add(timespan)
                    searchresults[i_scenario, start_year, final_year, i_searchterm, model_string] = {
                        "file": searchterm_string}
timeperiods_list = list(timeperiods)
models_list = list(models)
# create setting files for all timespans and searchterms, TODO: check for simplification, redundancy reduction
timespan_iterator = 0
settingspathcollection = []
outputpathcollection = []
inputfilecollection = []
# create directory to put settingsfiles

os.chdir(args.settingsdir)
if (os.path.exists(settingsdir) == False):
    os.mkdir(settingsdir)

for i_model in models_list:
    for i_scenario in args.scenarios:
        for i_timespan in timeperiods_list:
            timeindex = str(i_timespan[0]) + str(i_timespan[1])
            start_year = i_timespan[0]
            final_year = i_timespan[1]
            for i_searchterm in args.searchterms:
                # check if key exists
                if (i_scenario, start_year, final_year, i_searchterm, i_model) in searchresults.keys():
                    settings["input"]["model"] = i_model
                    # write filenames for searchterm
                    filename = searchresults[i_scenario, start_year, final_year, i_searchterm, i_model][
                        "file"]
                    settings["input"][i_searchterm] = filename
                    inputfilecollection.append(filename)
                    # modify years
                    settings["years"]["from"] = start_year
                    settings["years"]["to"] = final_year
            # modify output file
            outputfilename = str("output_" + i_model + "_" + timeindex + ".nc")
            outputfilepath = os.path.join(args.outputdir, outputfilename)
            # collect paths to outputfiles
            outputpathcollection.append(outputfilepath)
            settings["output"]["file"] = outputfilepath
            # save new settings file
            name_settings = "settings_" + i_scenario + "_" + i_model + "_" + timeindex + ".yml"
            yaml = ruamel.yaml.YAML()
            yaml.default_flow_style = None
            os.chdir(settingsdir)
            with open(name_settings, "w") as output:
                yaml.dump(settings, output)
            # collect paths to settings
            settingspathcollection.append(os.path.join(os.getcwd(), name_settings))
            timespan_iterator += 1

# create *.yml file of inputfiles:

os.chdir(settingsdir)
yaml = ruamel.yaml.YAML()
yaml.default_flow_style = None
with open("inputfiles.yml", "w") as output:
    yaml.dump(inputfilecollection, output)

# create *.yml file of settingsfiles:

os.chdir(os.path.join(args.settingsdir, "settings"))
yaml = ruamel.yaml.YAML()
yaml.default_flow_style = None
with open("list_of_settings.yml", "w") as output:
    yaml.dump(settingspathcollection, output)

# create *.yml file of outputfiles:
os.chdir(args.outputdir)
if (os.path.exists(outputdir) == False):
    os.mkdir(os.path.join(args.outputdir, "output"))
os.chdir(outputdir)
yaml = ruamel.yaml.YAML()
yaml.default_flow_style = None
with open("list_of_outputfiles.yml", "w") as output:
    yaml.dump(outputpathcollection, output)
