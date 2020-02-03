#!/usr/bin/env python
# -*- coding: utf-8 -*-
#  imports
import argparse
import os
import shutil
import sys
import imp
import yaml
import pyaml
import glob
from shutil import copyfile

# defining parser & properties of ensemble run
from pip._vendor.distlib.compat import raw_input

parser = argparse.ArgumentParser(description="Schedule an ensemble of model runs")
parser.add_argument(
    "--model",
    type=str,
    help="Path to model binary (default: CURRENT/model)",
)
parser.add_argument("--cpus", type=int, default=16, help="Number of cpus (default: 16)")
parser.add_argument(
    "--maxtime",
    type=str,
    default="1-00:00:00",
    help="Max runtime (default: 1-00:00:00)",
)
parser.add_argument(
    "--list_of_settings", type=str, default="CURRENT/list_of_settings.yml", help="File containing paths to individual settings files"
)

# variants for execution
parser.add_argument("--local", action="store_true", help="run locally, not on cluster")
parser.add_argument("--dry", action="store_true", help="dry run (do not run model)")
parser.add_argument("--verbose", action="store_true", help="be verbose")

args = parser.parse_args()
# default moel location
if not args.model:
    args.model = os.path.join(os.getcwd(), "model")

if not os.path.exists(args.model):
    exit("Model binary '{}' not found".format(args.model))

# default location of settings collection
if not args.list_of_settings:
    args.list_of_settings = os.path.join(os.getcwd(), "list_of_settings")
if not os.path.exists(args.list_of_settings):
    exit("List of settings '{}' not found".format(args.list_of_settings))

# determine number of runs for which settings are provided

list_of_settings = args.list_of_settings
numberOfRuns = len(list_of_settings)

# prepare run
def schedule_run():
    global run_id
    global settings_yml
    global run_cnt
    run_label = f"run{run_id}"
    if os.path.exists(run_label):
        run_id += 1
        return
    os.mkdir(run_label)
    # load and move settingsfile
    run_settings_paths = list_of_settings[run_id]
    shutil.move(run_settings_paths, f"{run_label}/settings.yml")

    if args.dry:
        return
    if args.local:
        os.system(
            "cd {} && {} {} >output.txt 2>errors.txt && cd ..".format(
                run_label, args.model, args.settings
            )
        )
    else:
        cmd = (
            "start-model"
            + " --model {}".format(args.model)
            + " --cpus {}".format(args.cpus)
            + " --jobname '{}/{}'".format(os.path.basename(os.getcwd()), run_label)
            + " --logdir {}".format(run_label)
            + " --maxtime {}".format(args.maxtime)
            + " --queue {}".format(args.queue)
            + " --workdir {}".format(run_label)
            + " {}/settings.yml".format(run_label)
        )

        if args.verbose:
            print(cmd)

        os.system(cmd)
        run_cnt += 1



if numberOfRuns > 1:
    print("Number of runs to be scheduled: %s" % numberOfRuns)
    sys.stdout.write("Run? y/N : ")
    if sys.version_info >= (3, 0):
        if input() != "y":
            exit("Aborted")
    else:
        if raw_input() != "y":
            exit("Aborted")
run_id = 0
schedule_run()
while run_id<numberOfRuns():
    schedule_run()
