import os
import stat
import mgzip
import click
import shutil
import logging
import itertools
import multiprocessing

from enum import Enum
from glob import glob
from fnmatch import fnmatch
from datetime import datetime
from collections import defaultdict
from pymatgen import Structure
from pymatgen.util.provenance import StructureNL
from atomate.vasp.database import VaspCalcDb
from fireworks.fw_config import FW_BLOCK_FORMAT
from mongogrant.client import Client
from atomate.vasp.drones import VaspDrone
from pymongo.errors import DocumentTooLarge
from dotty_dict import dotty

from emmet.core.utils import group_structures
from emmet.cli import SETTINGS
import hashlib
from _hashlib import HASH as Hash
from typing import Union
from pathlib import Path
from typing import List, Dict
import tarfile
import subprocess, shlex
from pydantic import BaseModel, Field
from typing import List, Dict, Set, Any
from maggma.stores.advanced_stores import MongograntStore
from maggma.core.store import Sort

logger = logging.getLogger("emmet")
perms = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP


class EmmetCliError(Exception):
    pass


class ReturnCodes(Enum):
    """codes to print command exit message in github issue comments"""

    SUCCESS = "COMPLETED"
    ERROR = "encountered ERROR"
    WARNING = "exited with WARNING"
    SUBMITTED = "submitted to SLURM"


def structures_match(s1, s2):
    return bool(len(list(group_structures([s1, s2]))) == 1)


def ensure_indexes(indexes, colls):
    created = defaultdict(list)
    for index in indexes:
        for coll in colls:
            keys = [k.rsplit("_", 1)[0] for k in coll.index_information().keys()]
            if index not in keys:
                coll.ensure_index(index)
                created[coll.full_name].append(index)

    if created:
        indexes = ", ".join(created[coll.full_name])
        logger.debug(f"Created the following index(es) on {coll.full_name}:\n{indexes}")


def calcdb_from_mgrant(spec_or_dbfile):
    if os.path.exists(spec_or_dbfile):
        return VaspCalcDb.from_db_file(spec_or_dbfile)

    client = Client()
    role = "rw"  # NOTE need write access to source to ensure indexes
    host, dbname_or_alias = spec_or_dbfile.split("/", 1)
    auth = client.get_auth(host, dbname_or_alias, role)
    if auth is None:
        raise Exception("No valid auth credentials available!")
    return VaspCalcDb(
        auth["host"],
        27017,
        auth["db"],
        "tasks",
        auth["username"],
        auth["password"],
        authSource=auth["db"],
    )


def get_meta_from_structure(struct):
    d = {"formula_pretty": struct.composition.reduced_formula}
    d["nelements"] = len(set(struct.composition.elements))
    d["nsites"] = len(struct)
    d["is_ordered"] = struct.is_ordered
    d["is_valid"] = struct.is_valid()
    return d


def aggregate_by_formula(coll, q, key=None):
    query = {"$and": [q, SETTINGS.exclude]}
    query.update(SETTINGS.base_query)
    nested = False
    if key is None:
        for k in SETTINGS.aggregation_keys:
            q = {k: {"$exists": 1}}
            q.update(SETTINGS.base_query)
            doc = coll.find_one(q)
            if doc:
                key = k
                nested = int("snl" in doc)
                break
        else:
            raise ValueError(
                f"could not find one of the aggregation keys {SETTINGS.aggregation_keys} in {coll.full_name}!"
            )

    push = {k.split(".")[-1]: f"${k}" for k in structure_keys[nested]}
    return coll.aggregate(
        [
            {"$match": query},
            {"$sort": {"nelements": 1, "nsites": 1}},
            {"$group": {"_id": f"${key}", "structures": {"$push": push}}},
        ],
        allowDiskUse=True,
        batchSize=1,
    )


def load_structure(dct):
    s = Structure.from_dict(dct)
    s.remove_oxidation_states()
    return s.get_primitive_structure()


# a utility function to get us a slice of an iterator, as an iterator
# when working with iterators maximum lazyness is preferred
def iterator_slice(iterator, length):
    iterator = iter(iterator)
    while True:
        res = tuple(itertools.islice(iterator, length))
        if not res:
            break
        yield res


def chunks(lst, n):
    return [lst[i: i + n] for i in range(0, len(lst), n)]


def get_subdir(dn):
    return dn.rstrip(os.sep).rsplit(os.sep, 1)[-1]


def get_timestamp_dir(prefix="launcher"):
    time_now = datetime.utcnow().strftime(FW_BLOCK_FORMAT)
    return "_".join([prefix, time_now])


def is_vasp_dir(list_of_files):
    for f in list_of_files:
        if f.startswith("INCAR"):
            return True


def make_block(base_path):
    ctx = click.get_current_context()
    run = ctx.parent.parent.params["run"]
    block = get_timestamp_dir(prefix="block")
    block_dir = os.path.join(base_path, block)
    if run:
        os.mkdir(block_dir)
    return block_dir


def get_symlinked_path(root, base_path_index):
    """organize directory in block_*/launcher_* via symbolic links"""
    ctx = click.get_current_context()
    run = ctx.parent.parent.params["run"]
    root_split = root.split(os.sep)
    base_path = os.sep.join(root_split[:base_path_index])

    if root_split[base_path_index].startswith("block_"):
        block_dir = os.sep.join(root_split[: base_path_index + 1])
    else:
        all_blocks = glob(os.path.join(base_path, "block_*/"))
        for block_dir in all_blocks:
            p = os.path.join(block_dir, "launcher_*/")
            if len(glob(p)) < 300:
                break
        else:
            # didn't find a block with < 300 launchers
            block_dir = make_block(base_path)

    if root_split[-1].startswith("launcher_"):
        launch_dir = os.path.join(block_dir, root_split[-1])
        if not os.path.exists(launch_dir):
            if run:
                os.rename(root, launch_dir)
            logger.debug(f"{root} -> {launch_dir}")
    else:
        launch = get_timestamp_dir(prefix="launcher")
        launch_dir = os.path.join(block_dir, launch)
        if run:
            os.rename(root, launch_dir)
            os.symlink(launch_dir, root)
        logger.debug(f"{root} -> {launch_dir}")

    return launch_dir


def create_orig_inputs(vaspdir):
    ctx = click.get_current_context()
    run = ctx.parent.parent.params["run"]
    for inp in ["INCAR", "KPOINTS", "POTCAR", "POSCAR"]:
        input_path = os.path.join(vaspdir, inp)
        if not glob(input_path + ".orig*"):
            matches = glob(input_path + "*")
            if matches:
                input_path = matches[0]
                orig_path = input_path.replace(inp, inp + ".orig")
                if run:
                    shutil.copyfile(input_path, orig_path)
                logger.debug(f"{input_path} -> {orig_path}")


# https://stackoverflow.com/a/34073559
class VaspDirsGenerator:
    def __init__(self):
        self.gen = get_vasp_dirs()

    def __iter__(self):
        self.value = yield from self.gen


def get_vasp_dirs():
    ctx = click.get_current_context()
    run = ctx.parent.parent.params["run"]
    nmax = ctx.parent.params["nmax"]
    pattern = ctx.parent.params["pattern"]
    reorg = ctx.parent.params["reorg"]

    base_path = ctx.parent.params["directory"].rstrip(os.sep)
    base_path_index = len(base_path.split(os.sep))
    if pattern:
        pattern_split = pattern.split(os.sep)
        pattern_split_len = len(pattern_split)

    counter = 0
    for root, dirs, files in os.walk(base_path, topdown=True):
        if counter == nmax:
            break

        level = len(root.split(os.sep)) - base_path_index
        if pattern and dirs and pattern_split_len > level:
            p = pattern_split[level]
            dirs[:] = [d for d in dirs if fnmatch(d, p)]

        for d in dirs:
            dn = os.path.join(root, d)
            st = os.stat(dn)
            if not bool(st.st_mode & perms):
                raise EmmetCliError(f"Insufficient permissions {st.st_mode} for {dn}.")

        if is_vasp_dir(files):
            gzipped = False
            for f in files:
                fn = os.path.join(root, f)
                if os.path.islink(fn):
                    if run:
                        os.unlink(fn)
                        logger.warning(f"Unlinked {fn}.")
                    else:
                        logger.warning(f"Would unlink {fn}.")
                    continue

                st = os.stat(fn)
                if not bool(st.st_mode & perms):
                    raise EmmetCliError(
                        f"Insufficient permissions {st.st_mode} for {fn}."
                    )

                if run and not f.endswith(".gz"):
                    fn_gz = fn + ".gz"
                    if os.path.exists(fn_gz):
                        os.remove(fn_gz)  # remove left-over gz (cancelled job)

                    with open(fn, "rb") as fo, mgzip.open(fn_gz, "wb", thread=0) as fw:
                        fw.write(fo.read())

                    os.remove(fn)  # remove original
                    shutil.chown(fn_gz, group="matgen")
                    gzipped = True

            # NOTE skip symlink'ing on MP calculations from the early days
            vasp_dir = get_symlinked_path(root, base_path_index) if reorg else root
            create_orig_inputs(vasp_dir)
            dirs[:] = []  # don't descend further (i.e. ignore relax1/2)
            logger.log(logging.INFO if gzipped else logging.DEBUG, vasp_dir)
            yield vasp_dir
            counter += 1

    return counter


def reconstruct_command(sbatch=False):
    ctx = click.get_current_context()
    command = []
    for level, (cmd, params) in enumerate(
            zip(
                ctx.command_path.split(),
                [ctx.grand_parent.params, ctx.parent.params, ctx.params],
            )
    ):
        command.append(cmd)
        if level:
            command.append("\\\n")
        for k, v in params.items():
            k = k.replace("_", "-")
            if v:
                if isinstance(v, bool):
                    if (sbatch and k != "sbatch" and k != "bb") or not sbatch:
                        command.append(f"--{k}")
                elif isinstance(v, str):
                    command.append(f'--{k}="{v}"')
                elif isinstance(v, tuple) or isinstance(v, list):
                    for x in v:
                        command.append(f'--{k}="{x}"')
                        command.append("\\\n")
                else:
                    command.append(f"--{k}={v}")
                if level:
                    command.append("\\\n")

    return " ".join(command).strip().strip("\\")


def parse_vasp_dirs(vaspdirs, tag, task_ids, snl_metas):  # noqa: C901
    process = multiprocessing.current_process()
    name = process.name
    chunk_idx = int(name.rsplit("-")[1]) - 1
    logger.info(f"{name} starting.")
    tags = [tag, SETTINGS.year_tags[-1]]
    ctx = click.get_current_context()
    spec_or_dbfile = ctx.parent.parent.params["spec_or_dbfile"]
    target = calcdb_from_mgrant(spec_or_dbfile)
    snl_collection = target.db.snls_user
    sbxn = list(filter(None, target.collection.distinct("sbxn")))
    logger.info(f"Using sandboxes {sbxn}.")
    no_dupe_check = ctx.parent.parent.params["no_dupe_check"]
    run = ctx.parent.parent.params["run"]
    projection = {"tags": 1, "task_id": 1}
    count = 0
    drone = VaspDrone(
        additional_fields={"tags": tags},
        store_volumetric_data=ctx.params["store_volumetric_data"],
    )

    for vaspdir in vaspdirs:
        logger.info(f"{name} VaspDir: {vaspdir}")
        launcher = get_subdir(vaspdir)
        query = {"dir_name": {"$regex": launcher}}
        docs = list(
            target.collection.find(query, projection).sort([("_id", -1)]).limit(1)
        )

        if docs:
            if no_dupe_check:
                logger.warning(f"FORCING re-parse of {launcher}!")
            else:
                if run:
                    shutil.rmtree(vaspdir)
                    logger.warning(f"{name} {launcher} already parsed -> removed.")
                else:
                    logger.warning(f"{name} {launcher} already parsed -> would remove.")
                continue

        try:
            task_doc = drone.assimilate(vaspdir)
        except Exception as ex:
            logger.error(f"Failed to assimilate {vaspdir}: {ex}")
            continue

        task_doc["sbxn"] = sbxn
        manual_taskid = isinstance(task_ids, dict)
        snl_metas_avail = isinstance(snl_metas, dict)
        task_id = task_ids[launcher] if manual_taskid else task_ids[chunk_idx][count]
        task_doc["task_id"] = task_id
        logger.info(f"Using {task_id} for {launcher}.")

        if docs:
            # make sure that task gets the same tags as the previously parsed task
            # (run through set to implicitly remove duplicate tags)
            if docs[0]["tags"]:
                existing_tags = list(set(docs[0]["tags"]))
                task_doc["tags"] += existing_tags
                logger.info(f"Adding existing tags {existing_tags} to {tags}.")

        snl_dct = None
        if snl_metas_avail:
            snl_meta = snl_metas.get(launcher)
            if snl_meta:
                references = snl_meta.get("references")
                authors = snl_meta.get(
                    "authors", ["Materials Project <feedback@materialsproject.org>"]
                )
                kwargs = {"projects": [tag]}
                if references:
                    kwargs["references"] = references

                struct = Structure.from_dict(task_doc["input"]["structure"])
                snl = StructureNL(struct, authors, **kwargs)
                snl_dct = snl.as_dict()
                snl_dct.update(get_meta_from_structure(struct))
                snl_id = snl_meta["snl_id"]
                snl_dct["snl_id"] = snl_id
                logger.info(f"Created SNL object for {snl_id}.")

        if run:
            if task_doc["state"] == "successful":
                if docs and no_dupe_check:
                    target.collection.remove({"task_id": task_id})
                    logger.warning(f"Removed previously parsed task {task_id}!")

                try:
                    target.insert_task(task_doc, use_gridfs=True)
                except DocumentTooLarge:
                    output = dotty(task_doc["calcs_reversed"][0]["output"])
                    pop_keys = [
                        "normalmode_eigenvecs",
                        "force_constants",
                        "outcar.onsite_density_matrices",
                    ]

                    for k in pop_keys:
                        if k not in output:
                            continue

                        logger.warning(f"{name} Remove {k} and retry ...")
                        output.pop(k)
                        try:
                            target.insert_task(task_doc, use_gridfs=True)
                            break
                        except DocumentTooLarge:
                            continue
                    else:
                        logger.warning(f"{name} failed to reduce document size")
                        continue

                if target.collection.count(query):
                    if snl_dct:
                        result = snl_collection.insert_one(snl_dct)
                        logger.info(
                            f"SNL {result.inserted_id} inserted into {snl_collection.full_name}."
                        )

                    shutil.rmtree(vaspdir)
                    logger.info(f"{name} Successfully parsed and removed {launcher}.")
                    count += 1
        else:
            count += 1

    return count


def make_tar_file(output_dir: Path, output_file_name: str, source_dir: Path):
    if not output_file_name.endswith(".tar.gz"):
        output_file_name = output_file_name + ".tar.gz"
    if output_dir.exists() is False:
        output_dir.mkdir(parents=True, exist_ok=True)
    output_tar_file = output_dir / output_file_name

    if output_tar_file.exists() is False:
        with tarfile.open(output_tar_file.as_posix(), "w:gz") as tar:
            tar.add(source_dir.as_posix(), arcname=os.path.basename(source_dir.as_posix()))


def compress_launchers(input_dir: Path, output_dir: Path, launcher_paths: List[str]):
    """

    create directories & zip

    :param input_dir:
    :param output_dir:
    :param block_name:
    :param launcher_paths:
    :return:
    """

    for launcher_path in launcher_paths:
        out_dir = Path(output_dir) / Path(launcher_path).parent
        output_file_name = launcher_path.split("/")[-1]
        if (out_dir / output_file_name).exists():
            continue
        else:
            logger.info(f"Compressing {launcher_path}".strip())
            make_tar_file(output_dir=out_dir,
                          output_file_name=output_file_name,
                          source_dir=Path(input_dir) / launcher_path)


def find_un_uploaded_materials_task_id(gdrive_mongo_store: MongograntStore,
                                       material_mongo_store: MongograntStore,
                                       max_num: int = 1000) -> List[str]:
    """
    Given mongo stores, find the next max_num mp_ids that are not yet uploaded.

    :param gdrive_mongo_store: gdrive mongo store
    :param material_mongo_store: materials mongo store
    :param max_num: int, maximum number of materials to return
    :return:
        list of materials that are not uploaded
    """

    # fetch max_num materials from materials mongo store
    materials: List[str] = find_materials_task_id_helper(material_mongo_store=material_mongo_store,
                                                         max_num=max_num, exclude_list=[])
    result: Set[str] = set(materials)
    # remove any of them that are already in the gdrive store
    gdrive_mp_ids = set(
        [entry["task_id"] for entry in gdrive_mongo_store.query(criteria={"task_id": {"$in": list(result)}},
                                                                properties={"task_id": 1})])
    result =
    retry = 0  # if there are really no more materials to add, just exit
    while len(result) < max_num and retry < 5:
        # fetch again from materials mongo store if there are more space
        materials: List[str] = find_materials_task_id_helper(material_mongo_store=material_mongo_store,
                                                             max_num=max_num, exclude_list=list(result))

        # remove any of them that are not in gdrive store
        result = set(materials)
        print(result)
        # remove any of them that are already in the gdrive store
        gdrive_mp_ids = set(
            [entry["task_id"] for entry in gdrive_mongo_store.query(criteria={"task_id": {"$in": list(result)}},
                                                                    properties={"task_id": 1})])
        result = result - gdrive_mp_ids
        retry += 1
    return list(result)


def find_materials_task_id_helper(material_mongo_store, max_num, exclude_list=None) -> List[str]:
    if exclude_list is None:
        exclude_list = []
    result: List[str] = []
    materials = material_mongo_store.query(criteria=
        {"$and": [{"deprecated": False},
                  {"task_id": {"$nin": exclude_list}}]},
        properties={"task_id": 1, "blessed_tasks": 1,
                    "last_updated": 1},
        sort={"last_updated": Sort.Descending},
        limit=max_num)
    for material in materials:
        if "blessed_tasks" in material:
            blessed_tasks: dict = material["blessed_tasks"]
            result.extend(list(blessed_tasks.values()))
    return result


class GDriveLog(BaseModel):
    path: str = Field(..., title="Path for the file",
                      description="Should reflect both local disk space AND google drive path")
    last_updated: datetime = Field(default=datetime.now())
    task_id: str = Field(default="", title="Material ID in which this launcher belongs to")
    file_size: int = Field(default=0, description="file size of the tar.gz")
    md5hash: str = Field(default="", description="md5 hash of the content of the files inside this gzip")
    files: List[Dict[str, Any]] = Field(default=[], description="meta data of the content of the gzip")


class File(BaseModel):
    file_name: str = Field(default="")
    size: int = Field(default=0)
    md5hash: str = Field(default="")


def move_dir(src: str, dst: str, pattern: str):
    for file_path in glob(f'{src}/{pattern}'):
        logger.info(f"Moving [{file_path}] to [{dst}]")
        shutil.move(src=file_path, dst=f"{dst}")


def md5_update_from_file(filename: Union[str, Path], hash: Hash) -> Hash:
    assert Path(filename).is_file()
    with open(str(filename), "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash.update(chunk)
    return hash


def md5_file(filename: Union[str, Path]) -> str:
    return str(md5_update_from_file(filename, hashlib.md5()).hexdigest())


def md5_update_from_dir(directory: Union[str, Path], hash: Hash) -> Hash:
    assert Path(directory).is_dir()
    for path in sorted(Path(directory).iterdir(), key=lambda p: str(p).lower()):
        hash.update(path.name.encode())
        if path.is_file():
            hash = md5_update_from_file(path, hash)
        elif path.is_dir():
            hash = md5_update_from_dir(path, hash)
    return hash


def md5_dir(directory: Union[str, Path]) -> str:
    """
    TODO ask patrick how does he want me to expose it?
    :param directory: directory to compute md5 hash on
    :return:
        the hash in string
    """
    return str(md5_update_from_dir(directory, hashlib.md5()).hexdigest())


def fill_record_data(record: GDriveLog, raw_dir: Path, compress_dir: Path):
    compress_file_dir = (compress_dir / record.path).as_posix() + ".tar.gz"
    record.file_size = os.path.getsize(compress_file_dir)
    record.md5hash = md5_dir(raw_dir / record.path)
    list_of_files = getListOfFiles(dirName= (raw_dir / record.path).as_posix())
    record.files.extend([_make_file_dict(file_path=Path(file), start_at=record.path) for file in list_of_files])


def getListOfFiles(dirName):
    """
        For the given path, get the List of all files in the directory tree
    """
    listOfFile = os.listdir(dirName)
    allFiles = list()
    for entry in listOfFile:
        fullPath = os.path.join(dirName, entry)
        if os.path.isdir(fullPath):
            allFiles = allFiles + getListOfFiles(fullPath)
        else:
            allFiles.append(fullPath)
    return allFiles


def _make_file_dict(file_path: Path, start_at: str) -> dict:
    start_index = file_path.as_posix().find(start_at) + len(start_at) + 1  # there is a slash after that
    path = file_path.as_posix()[start_index:]
    return {"path": path,
            "size": os.path.getsize(file_path.as_posix()),
            "md5hash": md5_file(file_path)}


def find_all_launcher_paths(input_dir: Path) -> List[str]:
    paths: List[str] = []
    for root, dirs, files in os.walk(input_dir.as_posix()):
        for name in dirs:
            if "launcher" in name:
                sub_paths = find_all_launcher_paths_helper(Path(root) / name)
                paths.extend(sub_paths)
    return paths


def find_all_launcher_paths_helper(input_dir: Path) -> List[str]:
    dir_name = input_dir.as_posix()
    start = dir_name.find("block_")
    dir_name = dir_name[start:]

    paths: List[str] = [dir_name]  # since itself is a launcher path
    for root, dirs, files in os.walk(input_dir.as_posix()):
        for name in dirs:
            if "launcher" in name:
                sub_paths = find_all_launcher_paths_helper(Path(root) / name)
                paths.extend(sub_paths)
    return paths
