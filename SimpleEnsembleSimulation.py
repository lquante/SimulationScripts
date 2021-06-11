#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#  imports
import argparse
import datetime
import os
import shutil
import subprocess
import sys

from pip._vendor.distlib.compat import raw_input
from ruamel.yaml import ruamel


# Author: Sven Willner <sven.willner@pik-potsdam.de>


def schedule_acclimate(
        acclimate="./acclimate",
        account="acclimat",
        autorelease=True,
        checkpointdir=None,
        cpus=16,
        jobname="acclimate",
        logdir=None,
        mintime="4:00:00",
        notify=True,
        partition="priority",
        prelimitseconds=60 * 60,
        qos="priority",
        resumeid=None,
        settings=None,
        workdir=".",
):
    if resumeid is None and settings is None:
        raise RuntimeError("Specify resume id or settings")

    if logdir is None:
        logdir = workdir
    if checkpointdir is None:
        checkpointdir = logdir

    acclimate = os.path.abspath(acclimate)
    print(acclimate)
    checkpointdir = os.path.abspath(checkpointdir)
    logdir = os.path.abspath(logdir)
    workdir = os.path.abspath(workdir)

    if not os.path.exists(acclimate):
        raise RuntimeError(f"Acclimate not found: {acclimate}")

    if subprocess.call([acclimate, "--info"], stdout=subprocess.DEVNULL) != 0:
        raise RuntimeError(f"Could not run acclimate: {acclimate}")

    os.makedirs(checkpointdir, exist_ok=True)
    os.makedirs(logdir, exist_ok=True)
    os.makedirs(workdir, exist_ok=True)

    other_options = ""
    if notify:
        other_options += "#SBATCH --mail-type=END,FAIL,TIME_LIMIT\n"

    if resumeid is None:
        output = "%j"
    else:
        output = resumeid

    batch = f"""#!/usr/bin/env bash
#SBATCH --account={account}
#SBATCH --acctg-freq=energy=0
#SBATCH --constraint=haswell
#SBATCH --cpus-per-task={cpus}
#SBATCH --error="{logdir}/{output}.txt"
#SBATCH --exclusive
#SBATCH --export=ALL,OMP_PROC_BIND=FALSE,OMP_NUM_THREADS={cpus}
#SBATCH --job-name="{jobname}"
#SBATCH --nice=0
#SBATCH --nodes=1
#SBATCH --open-mode=append
#SBATCH --output="{logdir}/{output}.txt"
#SBATCH --partition={partition}
#SBATCH --profile=none
#SBATCH --qos={qos}
#SBATCH --requeue
#SBATCH --signal=SIGTERM@{prelimitseconds}
#SBATCH --time-min={mintime}
#SBATCH --workdir="{workdir}"
{other_options}
msg () {{
    echo "$1 $SLURM_JOB_NAME $SLURM_JOB_ID #$RESTART_COUNT @ $(date +%FT%T)"
}}
fail () {{
    msg "FAILED($1)"
    exit $1
}}
if [ -z "$ORIGINAL_JOB_ID" ]
then
    export ORIGINAL_JOB_ID=$SLURM_JOB_ID
fi
export DMTCP_CHECKPOINT_DIR="{checkpointdir}/$ORIGINAL_JOB_ID"

if [ ! -e "$DMTCP_CHECKPOINT_DIR" ]
then
    echo "Checkpoint directory missing"
    fail 1
fi
if RESTART_FILE=$(ls "$DMTCP_CHECKPOINT_DIR"/*.dmtcp 2>/dev/null)
then
    msg "CONTINUING" 
    cp -f "$RESTART_FILE" "$DMTCP_CHECKPOINT_DIR/backup"
    try=0
    while true
    do
        srun --disable-status dmtcp_restart "$RESTART_FILE"
        retval=$?
        try=$((try+1))
        if [ $retval -eq 1 ]
        then
            if [ $try -eq 5 ]
            then
                break
            fi
            echo "Waiting for 2min..."
            sleep 120
        else
            break
        fi
    done
else
    msg "STARTING"
    srun dmtcp_launch "{acclimate}" "$DMTCP_CHECKPOINT_DIR/settings.yml"
    retval=$?
fi
if [ $retval -eq 0 ]
then
    rm -rf "$DMTCP_CHECKPOINT_DIR"
    msg "DONE"
else
    if [ $retval -eq 7 ]
    then
        msg "REQUEUING"
        export RESTART_COUNT=$((RESTART_COUNT+1))
        scontrol requeue "$SLURM_JOB_ID"
    else
        fail $retval
    fi
fi
msg "LAST LINE"
    """
    print(batch)
    env = os.environ.copy()
    if resumeid is None:
        env["ORIGINAL_JOB_ID"] = ""
    else:
        env["ORIGINAL_JOB_ID"] = str(resumeid)
        env["RESTART_COUNT"] = "0"
    p = subprocess.Popen(
        ["sbatch", "--hold", "--parsable"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        bufsize=4096,
        env=env,
    )
    p.stdin.write(bytes(batch, "utf8"))
    p.stdin.close()
    jobid = int(p.stdout.read().decode("utf8"))
    schedule_time = datetime.datetime.now().replace(microsecond=0).isoformat()
    if p.wait() != 0:
        return

    try:
        if resumeid is None:
            output = str(jobid)
        with open(f"{logdir}/{output}.txt", "a") as f:
            f.write(f"SCHEDULED {jobname} {jobid} #0 @ {schedule_time}\n")

        if settings is not None:
            finalcheckpointdir = os.path.join(checkpointdir, str(jobid))
            os.makedirs(finalcheckpointdir, exist_ok=False)
            with open(os.path.join(finalcheckpointdir, "settings.yml"), "w") as f:
                f.write(settings)

        subprocess.check_call(["scontrol", "release", str(jobid)])
    except e:
        subprocess.check_call(["scontrol", "cancel", str(jobid)])
        raise e

    return jobid


# defining parser & properties of ensemble run

parser = argparse.ArgumentParser(description="Schedule an ensemble of model runs")
parser.add_argument(
    "--model",
    type=str,
    help="Path to model binary (default: CURRENT/model)",
)
parser.add_argument("--cpus", type=int, default=16, help="Number of cpus (default: 16)")
parser.add_argument("--memory", type=int, default=4000, help="RAM per job in MB (default: 4000)")
parser.add_argument(
    "--time",
    type=str,
    required=True,
    help="Max runtime, please estimate as accurate as possible for efficient queuing",
)

parser.add_argument("--queue", type=str, default="short", help="queue to be used on the cluster")

parser.add_argument("--qos", type=str, default="standard", help="qos to be used on the cluster")

parser.add_argument("--partition", type=str, default="standard", help="partition to be used on the cluster")

parser.add_argument(
    "--settings", type=str, help="File containing paths to individual settings files"
)

parser.add_argument(
    "--dependency", type=int, help="JOB ID that needs to finish before Job starts"
)

# variants for execution
parser.add_argument("--local", action="store_true", help="run locally, not on cluster")
parser.add_argument("--dry", action="store_true", help="dry run (do not run model)")
parser.add_argument("--python", action="store_true", help="run model with python")
parser.add_argument("--acclimate", action="store_true", help="run acclimate with restart option")
parser.add_argument("--verbose", action="store_true", help="be verbose")
# initialize argument parser
args = parser.parse_args()
# default model location
if not args.model:
    args.model = os.path.join(os.getcwd(), "model")
if not os.path.exists(args.model):
    exit("Model binary '{}' not found".format(args.model))

# default location of settings collection
if not args.settings:
    args.settings = os.path.join(os.getcwd(), "/list_of_settings.yml")
if not os.path.exists(args.settings):
    exit("List of settings '{}' not found".format(args.settings))

# open list of settings
yaml = ruamel.yaml.YAML()
with open(args.settings, 'r') as stream:
    list_of_settings = yaml.load(stream)
# determine number of runs for which settings are provided
numberOfRuns = len(list_of_settings)


# prepare run
def schedule_run():
    global settings_yml
    global run_cnt
    # load path of settingsfile
    run_settings_file = list_of_settings[run_cnt]
    run_settings_paths = os.path.dirname(run_settings_file)
    # create run label

    identifier = str(run_settings_file)
    run_label = os.path.join(run_settings_paths, identifier + "_run_" + str(run_cnt))
    # check if directory for run already exisits
    if os.path.exists(run_label):
        run_cnt += 1
        return
    # create directory for run
    os.mkdir(run_label)
    # copy settings file

    path_settings = os.path.join(run_label + "/settings.yml")
    shutil.copy(run_settings_file, path_settings)
    if args.dry:
        return
    if args.local:
        # shell script needs to be in same directory as this script
        if (args.python):
            cmd = ("./local-model"
               + " --python 1"
               + " --model {}".format(args.model)
               + " --logdir {}".format(run_label)
               + " --workdir {}".format(run_label)
               + " {}".format(path_settings))
        else:
            cmd = ("./local-model"
                   + " --model {}".format(args.model)
                   + " --logdir {}".format(run_label)
                   + " --workdir {}".format(run_label)
                   + " {}".format(path_settings))
        if args.verbose:
            print(cmd)
            print(os.getcwd())
        os.system(cmd)
        run_cnt += 1

    else:
        # shell script needs to be in same directory as this script

        # adjust default time for longer queues
        if (args.acclimate):
            with open(path_settings) as f:
                jobid = schedule_acclimate(
                    acclimate=args.model,
                    checkpointdir=None,
                    cpus=16,
                    jobname="acclimate",
                    logdir=run_label,
                    mintime=args.time,
                    partition=args.partition,
                    qos=args.qos,
                    settings=f.read(),
                    workdir=run_label,
                )
            print(f"Job ID: {jobid}")
            print(args.model)
        else:

            if (args.python):
                cmd = ("/p/tmp/quante/SimulationScripts/./start-model"
                       + " --model {}".format(args.model)
                       + " --python 1"
                       + " --cpus {}".format(args.cpus)
                       + " --memory {}".format(args.memory)
                       + " --jobname '{}'".format(run_label)
                       + " --logdir {}".format(run_label)
                       + " --time {}".format(args.time)
                       + " --queue {}".format(args.queue)
                       + " --qos {}".format(args.qos)
                       + " --partition {}".format(args.partition)
                       + " --workdir {}".format(run_label)
                       + " {}".format(path_settings)
                       )
            else:
                cmd = ("/p/tmp/quante/SimulationScripts/./start-model"
                       + " --model {}".format(args.model)
                       + " --cpus {}".format(args.cpus)
                       + " --memory {}".format(args.memory)
                       + " --jobname '{}'".format(run_label)
                       + " --logdir {}".format(run_label)
                       + " --time {}".format(args.time)
                       + " --queue {}".format(args.queue)
                       + " --qos {}".format(args.qos)
                       + " --partition {}".format(args.partition)
                       + " --workdir {}".format(run_label)
                       + " {}".format(path_settings)
                       )
            if args.verbose:
                print(cmd)
            os.system(cmd)
            run_cnt += 1


# execute runs
if numberOfRuns >= 1:
    print("Number of runs to be scheduled: %s" % numberOfRuns)
    sys.stdout.write("Run? y/N : ")
    if sys.version_info >= (3, 0):
        if input() != "y":
            exit("Aborted")
    else:
        if raw_input() != "y":
            exit("Aborted")
run_cnt = 0
schedule_run()
while run_cnt < numberOfRuns:
    schedule_run()
