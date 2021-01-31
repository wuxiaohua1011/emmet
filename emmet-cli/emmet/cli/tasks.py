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
from emmet.cli.utils import ensure_indexes, get_subdir, parse_vasp_dirs, find_all_launcher_paths
from emmet.cli.utils import chunks, iterator_slice
from emmet.cli.decorators import sbatch
from emmet.cli.utils import compress_launchers, log_to_mongodb, move_dir, GDriveLog, nomad_upload_data, nomad_find_not_uploaded, find_unuploaded_launcher_paths

import datetime
from typing import List, Dict
from pathlib import Path
from maggma.stores.advanced_stores import MongograntStore
import glob

logger = logging.getLogger("emmet")
GARDEN = "/home/m/matcomp/garden"
PREFIX = "block_"
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
    default=f"{PREFIX}*",
    help="Pattern for sub-paths to include.",
)
def tasks(directory, nmax, pattern):
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
    elif not pattern.startswith(PREFIX):
        raise EmmetCliError(f"Pattern ({pattern}) only allowed to start with {PREFIX}!")


def load_block_launchers():
    block_launchers = defaultdict(list)
    gen = VaspDirsGenerator()
    for idx, vasp_dir in enumerate(gen):
        if idx and not idx % 500:
            logger.info(f"{idx} launchers found ...")
        launch_dir = PREFIX + vasp_dir.split(PREFIX, 1)[-1]
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
def backup(clean, check):
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
def restore(inputfile, file_filter):
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
                    block, launcher = line.split(os.sep, 1)
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

    nfiles_restore_total, max_args = 0, 15000
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
def upload(input_dir):
    ctx = click.get_current_context()
    run = ctx.parent.parent.params["run"]
    nmax = ctx.parent.params["nmax"]
    pattern = ctx.parent.params["pattern"]
    directory = ctx.parent.params["directory"]
    full_input_dir: Path = (Path(directory) / input_dir)
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
        run_outputs = run_command(args=cmds, filelist=[])
        for run_output in run_outputs:
            logger.info(run_output.strip())

        logger.info(msg=base_msg.strip())
    else:
        cmds.extend(["-n", "--dry-run"])
        run_outputs = run_command(args=cmds, filelist=[])
        for run_output in run_outputs:
            logger.info(run_output)
        logger.info(msg=("would have " + base_msg).strip())

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
    full_input_dir: Path = (Path(directory) / input_dir)
    full_output_dir: Path = (Path(directory) / output_dir)
    if full_input_dir.exists() is False:
        raise FileNotFoundError(f"input_dir {full_input_dir.as_posix()} not found")

    paths: List[str] = find_all_launcher_paths(full_input_dir)

    path_organized_by_blocks: Dict[str, List[str]] = dict()
    for path in paths:
        block_name = path.split("/")[0]
        if block_name in path_organized_by_blocks:
            path_organized_by_blocks[block_name].append(path)
        else:
            path_organized_by_blocks[block_name] = [path]

    msg = f"compressed [{len(path_organized_by_blocks)}] blocks"
    if run:
        if not full_output_dir.exists():
            full_output_dir.mkdir(parents=True, exist_ok=True)

        pool = multiprocessing.Pool(processes=nproc)
        pool.starmap(func=compress_launchers, iterable=[(Path(full_input_dir), Path(full_output_dir),
                                                         sorted(launcher_paths, key=len, reverse=True))
                                                        for launcher_paths in path_organized_by_blocks.values()])
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
def parse(task_ids, nproc, store_volumetric_data):
    """Parse VASP launchers into tasks"""
    ctx = click.get_current_context()
    if "CLIENT" not in ctx.obj:
        raise EmmetCliError("Use --spec to set target DB for tasks!")

    run = ctx.parent.parent.params["run"]
    nmax = ctx.parent.params["nmax"]
    directory = ctx.parent.params["directory"].rstrip(os.sep)
    tag = os.path.basename(directory)
    target = ctx.obj["CLIENT"]
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
        all_task_ids = target.collection.distinct(
            "task_id", {"task_id": {"$regex": r"^mp-\d{7,}$"}}
        )
        if not all_task_ids:
            all_task_ids = target.collection.distinct("task_id")

        next_tid = max(int(tid.split("-")[-1]) for tid in all_task_ids) + 1
        lst = [f"mp-{next_tid + n}" for n in range(nmax)]
        if run:
            sep_tid = f"mp-{next_tid + nmax}"
            target.collection.insert({"task_id": sep_tid})
            logger.info(f"Inserted separator task with task_id {sep_tid}.")
        task_ids = chunks(lst, chunk_size)
        logger.info(f"Reserved {len(lst)} task ID(s).")

    while iterator or queue:
        try:
            args = [next(iterator), tag, task_ids]
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
    else:
        logger.info(f"Would parse and insert {count}/{gen.value} tasks in {directory}.")
    return ReturnCodes.SUCCESS if count and gen.value else ReturnCodes.WARNING


@tasks.command()
@sbatch
@click.option(
    "--mongo-configfile",
    required=False,
    default=Path("~/.mongogrant.json").expanduser().as_posix(),
    type=click.Path(),
    help="mongo db connections. Path should be full path."
)
@click.option(
    "-n",
    "--num-materials",
    required=False,
    default=1000,
    type=click.IntRange(min=0, max=1000),
    help="maximum number of materials to query"
)
def upload_latest(mongo_configfile, num_materials):
    ctx = click.get_current_context()
    run = ctx.parent.parent.params["run"]
    directory = ctx.parent.params["directory"]
    full_root_dir: Path = Path(directory)
    full_mongo_config_path: Path = Path(mongo_configfile).expanduser()
    full_emmet_input_file_path: Path = full_root_dir / "emmet_input_file.txt"

    if run:
        try:
            base_cmds = ["emmet", "--run", "--yes", "--issue", "87", "tasks", "-d", full_root_dir.as_posix()]

            # find all un-uploaded launchers
            task_records: List[GDriveLog] = find_unuploaded_launcher_paths(outputfile=full_emmet_input_file_path.as_posix(),
                                                                           configfile=full_mongo_config_path.as_posix(),
                                                                           num=num_materials)
            # restore
            restore_dir = (full_root_dir / "restore")
            if restore_dir.exists() is False:
                restore_dir.mkdir(parents=True, exist_ok=True)
            restore_cmds = base_cmds[:-1] + [restore_dir.as_posix()] + ["restore", "--inputfile", full_emmet_input_file_path.as_posix()]
            run_and_log_info(args=restore_cmds)
            logger.info(f"Restoring using command [{' '.join(restore_cmds)}]")
            # logger.info("DBUGGING, NOT EXECUTING")

            # move restored content to directory/raw
            run_and_log_info(args=["rclone", "moveto", restore_dir.as_posix(), (full_root_dir / 'raw').as_posix()])


            # run compressed cmd
            compress_cmds = base_cmds + ["compress", "-l", "raw", "-o", "compressed", "--nproc", "4"]
            logger.info(f"Compressing using command [{' '.join(compress_cmds)}]".strip())
            run_and_log_info(args=compress_cmds)

            # run upload cmd
            # upload_cmds = base_cmds + ["upload", "--input-dir", "compressed"]
            # logger.info(f"Uploading using command [{' '.join(upload_cmds)}]")
            # run_and_log_info(args=upload_cmds)
            #
            # # log to mongodb
            # log_to_mongodb(mongo_configfile=mongo_configfile, task_records=task_records,
            #                raw_dir=full_root_dir / 'raw', compress_dir=full_root_dir / "compressed")
            #
            # # move uploaded & compressed content to tmp long term storage
            # mv_cmds = ["rclone", "move",
            #            f"{(full_root_dir / 'compressed').as_posix()}",
            #            f"{(full_root_dir / 'tmp_storage').as_posix()}",
            #            "--delete-empty-src-dirs"]
            # run_and_log_info(args=mv_cmds)
            #
            # # run clean up command
            # # DANGEROUS!!
            # remove_raw = ["rclone", "purge", f"{(full_root_dir/'raw').as_posix()}"]
            # run_and_log_info(args=remove_raw)
            #
            # remove_restore = ["rclone", "purge", f"{restore_dir.as_posix()}"]
            # run_and_log_info(args=remove_restore)
        except Exception as e:
            logger.error(f"Something bad happened: {e}")

    else:
        logger.info("Run flag not supplied...")
    return ReturnCodes.SUCCESS

@tasks.command()
@sbatch
@click.option(
    "--nomad-configfile",
    required=True,
    type=click.Path(),
    help="nomad user name and password json file path. Path should be full path"
)
@click.option(
    "-n",
    "--num",
    required=False,
    default=1000,
    type=click.IntRange(min=0, max=1000),
    help="maximum number of materials to upload"
)
@click.option(
    "--mongo-configfile",
    required=False,
    default=Path("~/.mongogrant.json").expanduser().as_posix(),
    type=click.Path(),
    help="mongo db connections. Path should be full path."
)
def upload_to_nomad(nomad_configfile, num, mongo_configfile):
    configfile: Path = Path(mongo_configfile)
    full_nomad_config_path: Path = Path(nomad_configfile).expanduser()
    num: int = num
    ctx = click.get_current_context()
    run = ctx.parent.parent.params["run"]
    directory = ctx.parent.params["directory"]
    full_root_dir: Path = Path(directory)

    configfile: Path = Path(configfile)
    if configfile.exists() is False:
        raise FileNotFoundError(f"Config file [{configfile}] is not found")

    # connect to mongo necessary mongo stores
    gdrive_mongo_store = MongograntStore(mongogrant_spec="rw:knowhere.lbl.gov/mp_core_mwu",
                                         collection_name="gdrive",
                                         mgclient_config_path=configfile.as_posix())

    if run:
        gdrive_mongo_store.connect()
        if not full_nomad_config_path.exists():
            raise FileNotFoundError(f"Nomad Config file not found in {full_nomad_config_path}")
        cred: dict = json.load(full_nomad_config_path.open('r'))
        username: str = cred["username"]
        password: str = cred["password"]
        # find the earliest n tasks that has not been uploaded
        task_ids_not_uploaded: List[str] = nomad_find_not_uploaded(username=username, password=password, num=num,
                                                                   gdrive_mongo_store=gdrive_mongo_store)
        print(task_ids_not_uploaded)
        # upload those n tasks
        status: List[bool] = nomad_upload_data(task_ids=task_ids_not_uploaded,
                                               username=username, password=password,
                                               gdrive_mongo_store=gdrive_mongo_store, root_dir=full_root_dir / "tmp_storage")

        gdrive_mongo_store.close()
    else:
        logger.info("Not running. Please supply the run flag. ")

    return ReturnCodes.SUCCESS


def run_and_log_info(args, filelist=None):
    if filelist is None:
        filelist = []
    run_outputs = run_command(args=args, filelist=filelist)
    for run_output in run_outputs:
        logger.info(run_output.strip())



