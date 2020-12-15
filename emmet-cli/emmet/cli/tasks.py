import os
import sys
import json
import math
import shlex
import click
import shutil
import logging
import subprocess
import multiprocessing

from fnmatch import fnmatch
from collections import defaultdict, deque
from hpsspy import HpssOSError
from hpsspy.os.path import isfile
from emmet.cli.utils import VaspDirsGenerator, EmmetCliError, ReturnCodes
from emmet.cli.utils import ensure_indexes, get_subdir, parse_vasp_dirs
from emmet.cli.utils import chunks, iterator_slice
from emmet.cli.decorators import sbatch
from emmet.cli.utils import organize_path, compress_launchers

import datetime
from typing import List, Dict
from pathlib import Path
from maggma.stores.advanced_stores import MongograntStore

logger = logging.getLogger("emmet")
GARDEN = "/home/m/matcomp/garden"
PREFIXES = ["res_", "aflow_", "block_"]
FILE_FILTERS = [
    "INCAR*",
    "CONTCAR*",
    "KPOINTS*",
    "POSCAR*",
    "POTCAR*",
    "vasprun.xml*",
    "OUTCAR*",
]
FILE_FILTERS_DEFAULT = [
    f"{d}{os.sep}{f}" if d else f
    for f in FILE_FILTERS
    for d in ["", "relax1", "relax2"]
]
STORE_VOLUMETRIC_DATA = []

TMP_STORAGE = f"{os.environ.get('SCRATCH', '/global/cscratch1/sd/mwu1011')}/projects/tmp_storage"
LOG_DIR = f"{os.environ.get('SCRATCH', '/global/cscratch1/sd/mwu1011')}/projects/logs"


@click.group()
@click.option(
    "-d",
    "--directory",
    required=True,
    help="Directory to use for HPSS or parsing.",
)
@click.option(
    "-m", "--nmax", show_default=True, default=10, help="Maximum number of directories."
)
@click.option(
    "-p",
    "--pattern",
    show_default=True,
    default="block_*",
    help="Pattern for sub-paths to include.",
)
@click.option("--reorg", is_flag=True, help="Reorganize directory in block/launchers.")
def tasks(directory, nmax, pattern, reorg):
    """Backup, restore, and parse VASP calculations."""
    pass


@tasks.command()
@sbatch
def prep():
    """Prepare directory for HPSS backup"""
    ctx = click.get_current_context()
    directory = ctx.parent.params["directory"]
    gen = VaspDirsGenerator()
    list(x for x in gen)
    logger.info(f"Prepared {gen.value} VASP calculation(s) in {directory}.")
    return ReturnCodes.SUCCESS if gen.value else ReturnCodes.ERROR


def run_command(args, filelist):
    nargs, nfiles, nshow = len(args), len(filelist), 1
    full_args = args + filelist
    args_short = (
        full_args[: nargs + nshow] + [f"({nfiles - 1} more ...)"]
        if nfiles > nshow
        else full_args
    )
    logger.info(" ".join(args_short))
    popen = subprocess.Popen(
        full_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )
    for stdout_line in iter(popen.stdout.readline, ""):
        yield stdout_line
    popen.stdout.close()
    return_code = popen.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, full_args)


def recursive_chown(path, group):
    for dirpath, dirnames, filenames in os.walk(path):
        shutil.chown(dirpath, group=group)
        for filename in filenames:
            shutil.chown(os.path.join(dirpath, filename), group=group)


def check_pattern(nested_allowed=False):
    ctx = click.get_current_context()
    pattern = ctx.parent.params["pattern"]
    if not nested_allowed and os.sep in pattern:
        raise EmmetCliError(f"Nested pattern ({pattern}) not allowed!")
    elif not any(pattern.startswith(p) for p in PREFIXES):
        raise EmmetCliError(
            f"Pattern ({pattern}) only allowed to start with one of {PREFIXES}!"
        )


def load_block_launchers():
    prefix = (
        "block_"  # TODO old prefixes (e.g. res/aflow) might not be needed for backup
    )
    block_launchers = defaultdict(list)
    gen = VaspDirsGenerator()
    for idx, vasp_dir in enumerate(gen):
        if idx and not idx % 500:
            logger.info(f"{idx} launchers found ...")
        launch_dir = prefix + vasp_dir.split(prefix, 1)[-1]
        block, launcher = launch_dir.split(os.sep, 1)
        block_launchers[block].append(launcher)
    logger.info(f"Loaded {len(block_launchers)} block(s) with {gen.value} launchers.")
    return block_launchers


def extract_filename(line):
    ls = line.strip().split()
    return ls[-1] if len(ls) == 7 else None


@tasks.command()
@sbatch
@click.option("--clean", is_flag=True, help="Remove original launchers.")
@click.option("--check", is_flag=True, help="Check backup consistency.")
def backup(clean, check):  # noqa: C901
    """Backup directory to HPSS"""
    ctx = click.get_current_context()
    run = ctx.parent.parent.params["run"]
    ctx.parent.params["nmax"] = sys.maxsize  # disable maximum launchers for backup
    logger.warning("--nmax ignored for HPSS backup!")
    directory = ctx.parent.params["directory"]
    if not check and clean:
        logger.error("Not running --clean without --check enabled.")
        return ReturnCodes.ERROR

    check_pattern()

    logger.info("Discover launch directories ...")
    block_launchers = load_block_launchers()

    counter, nremove_total = 0, 0
    os.chdir(directory)
    for block, launchers in block_launchers.items():
        logger.info(f"{block} with {len(launchers)} launcher(s)")
        try:
            isfile(f"{GARDEN}/{block}.tar")
        except HpssOSError:  # block not in HPSS
            if run:
                filelist = [os.path.join(block, l) for l in launchers]
                args = shlex.split(f"htar -M 5000000 -Phcvf {GARDEN}/{block}.tar")
                try:
                    for line in run_command(args, filelist):
                        logger.info(line.strip())
                except subprocess.CalledProcessError as e:
                    logger.error(str(e))
                    return ReturnCodes.ERROR
                counter += 1
        else:
            logger.warning(f"Skip {block} - already in HPSS")

        # Check backup here to allow running it separately
        if check:
            logger.info(f"Verify {block}.tar ...")
            args = shlex.split(
                f"htar -Kv -Hrelpaths -Hverify=all -f {GARDEN}/{block}.tar"
            )
            files_remove = []
            try:
                for line in run_command(args, []):
                    line = line.strip()
                    if line.startswith("HTAR: V "):
                        ls = line.split(", ")
                        if len(ls) == 3:
                            nfiles = len(files_remove)
                            if nfiles and not nfiles % 1000:
                                logger.info(f"{nfiles} files ...")
                            files_remove.append(ls[0].split()[-1])
                    else:
                        logger.info(line)
            except subprocess.CalledProcessError as e:
                logger.error(str(e))
                return ReturnCodes.ERROR

            if clean:
                nremove = len(files_remove)
                nremove_total += nremove
                if run:
                    with click.progressbar(files_remove, label="Removing files") as bar:
                        for fn in bar:
                            os.remove(fn)
                    logger.info(f"Removed {nremove} files from disk for {block}.")
                else:
                    logger.info(f"Would remove {nremove} files from disk for {block}.")

    logger.info(f"{counter}/{len(block_launchers)} blocks newly backed up to HPSS.")
    if clean:
        if run:
            logger.info(f"Verified and removed a total of {nremove_total} files.")
        else:
            logger.info(f"Would verify and remove a total of {nremove_total} files.")
    return ReturnCodes.SUCCESS


@tasks.command()
@sbatch
@click.option(
    "-o",
    "--outputfile",
    required=True,
    type=click.Path(),
    help="file to save the content to. Path should be full path."
)
@click.option(
    "--configfile",
    required=True,
    default=(Path().expanduser() / ".mongogrant.json"),
    type=click.Path(),
    help="mongo db connections. Path should be full path."
)
@click.option(
    "-n",
    "--num",
    required=False,
    default=1000,
    type=click.IntRange(min=0, max=1000),
    help="maximum number of materials to query"
)
def find_unuploaded_launcher_paths(outputfile, configfile, num):
    outputfile: Path = Path(outputfile)
    configfile: Path = Path(configfile)
    if outputfile.exists():
        logger.info(f"Will be over writing {outputfile}")
    if configfile.exists() is False:
        raise FileNotFoundError(f"Config file {configfile} is not found")

    gdrive_mongo_store = MongograntStore(mongogrant_spec="rw:knowhere.lbl.gov/mp_core_mwu",
                                         collection_name="gdrive",
                                         mgclient_config_path=configfile.as_posix())
    material_mongo_store = MongograntStore(mongogrant_spec="ro:mongodb04.nersc.gov/mp_emmet_prod",
                                           collection_name="materials_2020_09_08",
                                           mgclient_config_path=configfile.as_posix())
    tasks_mongo_store = MongograntStore(mongogrant_spec="ro:mongodb04.nersc.gov/mp_emmet_prod",
                                        collection_name="tasks",
                                        mgclient_config_path=configfile.as_posix())
    gdrive_mongo_store.connect()
    material_mongo_store.connect()
    tasks_mongo_store.connect()

    logger.info("Stores connected")
    return ReturnCodes.SUCCESS


@tasks.command()
@sbatch
@click.option(
    "-l",
    "--inputfile",
    required=True,
    type=click.Path(exists=True),
    help="Text file with list of launchers to restore (relative to `directory`).",
)
@click.option(
    "-f",
    "--file-filter",
    multiple=True,
    show_default=True,
    default=FILE_FILTERS_DEFAULT,
    help="Set the file filter(s) to match files against in each launcher.",
)
def restore(inputfile, file_filter):  # noqa: C901
    """Restore launchers from HPSS"""
    ctx = click.get_current_context()
    run = ctx.parent.parent.params["run"]
    nmax = ctx.parent.params["nmax"]
    pattern = ctx.parent.params["pattern"]
    directory = ctx.parent.params["directory"]
    if not os.path.exists(directory):
        os.makedirs(directory)

    check_pattern(nested_allowed=True)
    shutil.chown(directory, group="matgen")
    block_launchers = defaultdict(list)
    nlaunchers = 0
    with open(inputfile, "r") as infile:
        os.chdir(directory)
        with click.progressbar(infile, label="Load blocks") as bar:
            for line in bar:
                if fnmatch(line, pattern):
                    if nlaunchers == nmax:
                        break
                    if os.sep in line:
                        block, launcher = line.split(os.sep, 1)
                    else:
                        block, launcher = line.strip(), ""
                    for ff in file_filter:
                        block_launchers[block].append(
                            os.path.join(launcher.strip(), ff)
                        )
                    nlaunchers += 1

    nblocks = len(block_launchers)
    nfiles = sum(len(v) for v in block_launchers.values())
    logger.info(
        f"Restore {nblocks} block(s) with {nlaunchers} launchers"
        f" and {nfiles} file filters to {directory} ..."
    )

    nfiles_restore_total, max_args = 0, 14000
    for block, files in block_launchers.items():
        # get full list of matching files in archive and check against existing files
        args = shlex.split(f"htar -tf {GARDEN}/{block}.tar")
        filelist = [os.path.join(block, f) for f in files]
        filelist_chunks = [
            filelist[i: i + max_args] for i in range(0, len(filelist), max_args)
        ]
        filelist_restore, cnt = [], 0
        try:
            for chunk in filelist_chunks:
                for line in run_command(args, chunk):
                    fn = extract_filename(line)
                    if fn:
                        cnt += 1
                        if os.path.exists(fn):
                            logger.debug(f"Skip {fn} - already exists on disk.")
                        else:
                            filelist_restore.append(fn)
        except subprocess.CalledProcessError as e:
            logger.error(str(e))
            return ReturnCodes.ERROR

        # restore what's missing
        if filelist_restore:
            nfiles_restore = len(filelist_restore)
            nfiles_restore_total += nfiles_restore
            if run:
                logger.info(
                    f"Restore {nfiles_restore}/{cnt} files for {block} to {directory} ..."
                )
                args = shlex.split(f"htar -xvf {GARDEN}/{block}.tar")
                filelist_restore_chunks = [
                    filelist_restore[i: i + max_args]
                    for i in range(0, len(filelist_restore), max_args)
                ]
                try:
                    for chunk in filelist_restore_chunks:
                        for line in run_command(args, chunk):
                            logger.info(line.strip())
                except subprocess.CalledProcessError as e:
                    logger.error(str(e))
                    return ReturnCodes.ERROR
            else:
                logger.info(
                    f"Would restore {nfiles_restore}/{cnt} files for {block} to {directory}."
                )
        else:
            logger.warning(f"Nothing to restore for {block}!")

        if run:
            logger.info(f"Set group of {block} to matgen recursively ...")
            recursive_chown(block, "matgen")

    if run:
        logger.info(f"Restored {nfiles_restore_total} files to {directory}.")
    else:
        logger.info(f"Would restore {nfiles_restore_total} files to {directory}.")
    return ReturnCodes.SUCCESS


@tasks.command()
@sbatch
@click.option(
    "-l",
    "--input-dir",
    required=True,
    type=click.Path(exists=False),
    help="Directory of blocks to upload to GDrive, relative to ('directory') ex: compressed",
)
@click.option(
    "--logging-dir",
    default=LOG_DIR,
    required=False,
    type=click.Path(exists=True),
    help="Directory of logs, relative to ('directory') ex: logs",

)
@click.option(
    "-o",
    "--output-dir",
    required=False,
    default=TMP_STORAGE,
    show_default=True,
    type=click.Path(exists=False),
    help="Directory to move the data to after upload is done, relative to ('directory'). ex: temp_storage"
         "Not moving if it is not supplied",
)
def upload(input_dir, logging_dir, output_dir):
    ctx = click.get_current_context()
    run = ctx.parent.parent.params["run"]
    nmax = ctx.parent.params["nmax"]
    pattern = ctx.parent.params["pattern"]
    directory = ctx.parent.params["directory"]
    full_input_dir: Path = (Path(directory) / input_dir)
    full_output_dir: Path = (Path(directory) / output_dir)
    full_logging_dir: Path = (Path(directory) / logging_dir)
    if full_input_dir.exists() is False:
        raise FileNotFoundError(f"input_dir {full_input_dir.as_posix()} not found")
    block_count = 0
    launcher_count = 0
    for root, dirs, files in os.walk(full_input_dir.as_posix()):
        for name in files:
            launcher_count += 1
        for name in dirs:
            block_count += 1

    base_msg = f"upload [{block_count}] blocks with [{launcher_count}] launchers"

    cmds = ["rclone",
            "--log-level", "INFO",
            "-c", "--auto-confirm",
            "copy",
            full_input_dir.as_posix(),
            "GDriveUpload:"]
    if run:
        if full_output_dir.exists() is False:
            full_output_dir.mkdir(exist_ok=True, parents=True)
        run_outputs = run_command(args=cmds, filelist=[])
        for run_output in run_outputs:
            logger.info(run_output)

        logger.info(msg=base_msg)
    else:
        cmds.extend(["-n", "--dry-run"])
        run_outputs = run_command(args=cmds, filelist=[])
        for run_output in run_outputs:
            logger.info(run_output)
        logger.info(msg="would have " + base_msg)

    return ReturnCodes.SUCCESS


@tasks.command()
@sbatch
@click.option(
    "-l",
    "--input-dir",
    required=True,
    type=click.Path(),
    help="Directory of blocks to compress, relative to ('directory') ex: raw`",
)
@click.option(
    "-o",
    "--output-dir",
    required=True,
    type=click.Path(exists=False),
    help="Directory of blocks to output the compressed blocks, relative to ('directory') ex: compressed",
)
@click.option(
    "--nproc",
    type=int,
    default=1,
    show_default=True,
    help="Number of processes for parallel parsing.",
)
def compress(input_dir, output_dir, nproc):
    ctx = click.get_current_context()
    run = ctx.parent.parent.params["run"]
    directory = ctx.parent.params["directory"]
    root_dir: Path = (Path(directory) / input_dir)
    if root_dir.exists() is False:
        raise FileNotFoundError(f"input_dir {root_dir.as_posix()} not found")
    paths: List[str] = []
    for root, dirs, files in os.walk(root_dir.as_posix()):
        for name in dirs:
            if "launcher" in name:
                dir_name = os.path.join(root, name)
                start = dir_name.find("block_")
                dir_name = dir_name[start:]
                paths.append(dir_name)
    paths_organized: Dict[str, List[str]] = organize_path(paths)
    msg = f"compressed [{len(paths_organized)}] blocks with [{len(paths)}] launchers"
    if run:
        pool = multiprocessing.Pool(processes=nproc)
        pool.starmap(func=compress_launchers, iterable=[(Path(input_dir), Path(output_dir),
                                                         block_name, launcher_paths)
                                                        for block_name, launcher_paths in paths_organized.items()])
        # for block_name, launcher_paths in paths_organized.items():
        #     compress_launchers(input_dir=Path(input_dir), output_dir=Path(output_dir),
        #                        block_name=block_name, launcher_paths=launcher_paths)
        logger.info(msg=msg)
    else:
        logger.info(msg="would have " + msg)
    return ReturnCodes.SUCCESS


@tasks.command()
@sbatch
@click.option(
    "--task-ids",
    type=click.Path(exists=True),
    help="JSON file mapping launcher name to task ID.",
)
@click.option(
    "--snl-metas",
    type=click.Path(exists=True),
    help="JSON file mapping launcher name to SNL metadata.",
)
@click.option(
    "--nproc",
    type=int,
    default=1,
    show_default=True,
    help="Number of processes for parallel parsing.",
)
@click.option(
    "-s",
    "--store-volumetric-data",
    multiple=True,
    default=STORE_VOLUMETRIC_DATA,
    help="Store any of CHGCAR, LOCPOT, AECCAR0, AECCAR1, AECCAR2, ELFCAR.",
)
def parse(task_ids, snl_metas, nproc, store_volumetric_data):  # noqa: C901
    """Parse VASP launchers into tasks"""
    ctx = click.get_current_context()
    if "CLIENT" not in ctx.obj:
        raise EmmetCliError("Use --spec to set target DB for tasks!")

    run = ctx.parent.parent.params["run"]
    nmax = ctx.parent.params["nmax"]
    directory = ctx.parent.params["directory"].rstrip(os.sep)
    tag = os.path.basename(directory)
    target = ctx.obj["CLIENT"]
    snl_collection = target.db.snls_user
    logger.info(
        f"Connected to {target.collection.full_name} with {target.collection.count()} tasks."
    )
    ensure_indexes(
        ["task_id", "tags", "dir_name", "retired_task_id"], [target.collection]
    )

    chunk_size = math.ceil(nmax / nproc)
    if nproc > 1 and nmax <= chunk_size:
        nproc = 1
        logger.warning(
            f"nmax = {nmax} but chunk size = {chunk_size} -> sequential parsing."
        )

    pool = multiprocessing.Pool(processes=nproc)
    gen = VaspDirsGenerator()
    iterator = iterator_slice(gen, chunk_size)  # process in chunks
    queue = deque()
    count = 0

    sep_tid = None
    if task_ids:
        with open(task_ids, "r") as f:
            task_ids = json.load(f)
    else:
        # reserve list of task_ids to avoid collisions during multiprocessing
        # insert empty doc with max ID + 1 into target collection for parallel SLURM jobs
        # NOTE use regex first to reduce size of distinct below 16MB
        q = {"task_id": {"$regex": r"^mp-\d{7,}$"}}
        all_task_ids = [
            t["task_id"] for t in target.collection.find(q, {"_id": 0, "task_id": 1})
        ]
        if not all_task_ids:
            all_task_ids = target.collection.distinct("task_id")

        next_tid = max(int(tid.split("-")[-1]) for tid in all_task_ids) + 1
        lst = [f"mp-{next_tid + n}" for n in range(nmax)]
        task_ids = chunks(lst, chunk_size)

        if run:
            sep_tid = f"mp-{next_tid + nmax}"
            target.collection.insert({"task_id": sep_tid})
            logger.info(f"Inserted separator task with task_id {sep_tid}.")
            logger.info(f"Reserved {len(lst)} task ID(s).")
        else:
            logger.info(f"Would reserve {len(lst)} task ID(s).")

    sep_snlid = None
    if snl_metas:
        with open(snl_metas, "r") as f:
            snl_metas = json.load(f)

        # reserve list of snl_ids to avoid collisions during multiprocessing
        # insert empty doc with max ID + 1 into target collection for parallel SLURM jobs
        all_snl_ids = snl_collection.distinct("snl_id")
        prefixes = set()
        next_snlid = -1

        for snlid in all_snl_ids:
            prefix, index = snlid.split("-", 1)
            index = int(index)
            prefixes.add(prefix)
            if index > next_snlid:
                next_snlid = index

        next_snlid += 1
        prefix = prefixes.pop()  # NOTE use the first prefix found
        nsnls = len(snl_metas)

        for n, launcher in enumerate(snl_metas):
            snl_id = f"{prefix}-{next_snlid + n}"
            snl_metas[launcher]["snl_id"] = snl_id

        if run:
            sep_snlid = f"{prefix}-{next_snlid + nsnls}"
            snl_collection.insert({"snl_id": sep_snlid})
            logger.info(f"Inserted separator SNL with snl_id {sep_snlid}.")
            logger.info(f"Reserved {nsnls} SNL ID(s).")
        else:
            logger.info(f"Would reserve {nsnls} SNL ID(s).")

    while iterator or queue:
        try:
            args = [next(iterator), tag, task_ids, snl_metas]
            queue.append(pool.apply_async(parse_vasp_dirs, args))
        except (StopIteration, TypeError):
            iterator = None

        while queue and (len(queue) >= pool._processes or not iterator):
            process = queue.pop()
            process.wait(1)
            if not process.ready():
                queue.append(process)
            else:
                count += process.get()

    pool.close()
    if run:
        logger.info(
            f"Successfully parsed and inserted {count}/{gen.value} tasks in {directory}."
        )
        if sep_tid:
            target.collection.remove({"task_id": sep_tid})
            logger.info(f"Removed separator task {sep_tid}.")
        if sep_snlid:
            snl_collection.remove({"snl_id": sep_snlid})
            logger.info(f"Removed separator SNL {sep_snlid}.")
    else:
        logger.info(f"Would parse and insert {count}/{gen.value} tasks in {directory}.")
    return ReturnCodes.SUCCESS if count and gen.value else ReturnCodes.WARNING
