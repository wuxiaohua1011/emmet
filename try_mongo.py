from maggma.stores.advanced_stores import MongograntStore
from maggma.stores.advanced_stores import Sort
from typing import List
from pathlib import Path


def nomad_find_not_uploaded(gdrive_mongo_store: MongograntStore, num: int) -> List[str]:
    """
    1. find a list of tasks that are not uploaded to nomad, sort ascending based on date created. limit by num

    if num < 0, return 32 GB worth of materials

    :param gdrive_mongo_store:
    :param num:
    :return:
        materials = material_mongo_store.query(
        criteria={"$and": [{"deprecated": False}, {"task_id": {"$nin": exclude_list}}]},
        properties={"task_id": 1, "blessed_tasks": 1, "last_updated": 1},
        sort={"last_updated": Sort.Descending},
        limit=max_num)
    """
    if num >= 0:
        raw = gdrive_mongo_store.query(
            criteria={"$and": [{"nomad_updated": None}, {"error": None}]},
            properties={"task_id": 1, "file_size": 1},
            sort={"last_updated": Sort.Ascending},
            limit=num
        )
    else:
        raw = gdrive_mongo_store.query(
            criteria={"$and": [{"nomad_updated": None}, {"error": None}]},
            properties={"task_id": 1, "file_size": 1},
            sort={"last_updated": Sort.Ascending}
        )
    max_nomad_upload_size = 32 * 1e9  # 32 gb
    size = 0
    result: List[str] = []
    for r in raw:
        task_id = r["task_id"]
        file_size = r["file_size"]
        if size + file_size < max_nomad_upload_size:
            result.append(task_id)
            size += file_size
        else:
            break
    print(f"total size = {size}")
    return result


def clear_uploaded_fields(mongo_store: MongograntStore):
    cursor = mongo_store.query(criteria=
        {"nomad_updated": {"$ne": None}}
    )
    new_objs = []
    for obj in cursor:
        obj["nomad_updated"] = None
        obj["nomad_upload_id"] = None
        new_objs.append(obj)
    mongo_store.update(docs=new_objs, key="task_id")
    print("updated")


if __name__ == "__main__":
    gdrive_mongo_store = MongograntStore(mongogrant_spec="rw:knowhere.lbl.gov/mp_core_mwu",
                                         collection_name="gdrive")
    gdrive_mongo_store.connect()
    nomad_upload_query = {
        "nomad_upload_id": {"$ne": None}
    }
    count = gdrive_mongo_store.count(criteria=nomad_upload_query)
    clear_uploaded_fields(gdrive_mongo_store)