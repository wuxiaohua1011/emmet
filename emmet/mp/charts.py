from maggma.builders import Builder


class MPChartBuilder(Builder):
    """
    Builds a variety of summary charts for the Materials Project website.

    Based off a Jupyter notebook by Joey Montoya, modified by Donny Winston.
    """

    @staticmethod
    def get_material_dates(store):
        docs = store.query(criteria={"sbxn": "core"}, properties=["task_id": 1, "snl_final.about.created_at": 1])
        all_dates = {}
        for doc in docs:
            if isinstance(doc['snl_final']['about']['created_at'], datetime):
                all_dates[doc["task_id"]] = doc['snl_final']['about']['created_at']
            elif isinstance(doc['snl_final']['about']['created_at'], six.string_types):
                dt_string = doc['snl_final']['about']['created_at']
                if ' ' in dt_string and '.' in dt_string:
                    dt = datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S.%f")
                elif ' ' in dt_string:
                    dt = datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S")
                else:
                    dt = datetime.strptime(dt_string, "%Y-%m-%dT%H:%M:%S.%f")
                all_dates[doc["task_id"]] = dt
            else:
                dt_string = doc['snl_final']['about']['created_at']['string']
                dt = datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S.%f")
                all_dates[doc["task_id"]] = dt
        return all_dates

    @staticmethod
    def get_molecule_dates(store):
        cur = db.molecules.find({}, {"snl_final.about.created_at.string": 1})
        data = [c['snl_final']['about']['created_at']['string'] for c in cur]
        data = [datetime.strptime(d, "%Y-%m-%d %H:%M:%S.%f") for d in data]
        return data

    @staticmethod
    def get_tensor_dates(store):
        pass

    def get_xas_dates(self):
        pass

    def get_bandstructure_dates(self):
        pass

    def get_magnetic_orderings_dates(self):