from maggma.stores.advanced_stores import MongograntStore
from pathlib import Path
if __name__ == "__main__":
    gdrive_mongo_store = MongograntStore(mongogrant_spec="rw:knowhere.lbl.gov/mp_core_mwu",
                                         collection_name="gdrive")
    gdrive_mongo_store.connect()
    nomad_upload_query = {
        "nomad_upload_id": {"$ne": None}
    }
    count = gdrive_mongo_store.count(criteria=nomad_upload_query)
    print(f"Setting {count} to not nomad uploaded")
