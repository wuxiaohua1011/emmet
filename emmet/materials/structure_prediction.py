from itertools import groupby

from maggma.builder import Builder
from pymatgen import Specie
from pymatgen.alchemy.materials import TransformedStructure
from pymatgen.analysis.structure_matcher import StructureMatcher, \
    ElementComparator
from pymatgen.analysis.structure_prediction.substitution_probability import \
    SubstitutionPredictor
from pymatgen.symmetry.analyzer import SpacegroupAnalyzer
from pymatgen.transformations.standard_transformations import \
    SubstitutionTransformation
from tqdm import tqdm


class SPBuilder(Builder):
    """Predict new structures based on transformations of source structures.

    Given a submitted set of species (element + oxidation state), and a
    likelihood threshold referenced to data-mined substitution probabilities,
    use a store of initial structures (e.g. experimentally known compounds) to
    produce a list of candidate structures, each with an above-threshold score
    and with a record of transformation (provenance) from its initial
    structure.
    """
    def __init__(self, jobs_in, structs_known,
                 jobs_out, structs_pred, **kwargs):
        self.jobs_in = jobs_in
        self.structs_known = structs_known
        self.jobs_out = jobs_out
        self.structs_pred = structs_pred
        super().__init__(sources=[jobs_in, structs_known],
                         targets=[jobs_out, structs_pred], **kwargs)

    def get_items(self):
        # TODO option to retry/rerun jobs, e.g. using a state field.
        has_output = self.jobs_out.distinct(self.jobs_out.key)
        jobs_todo = self.jobs_in.query(
            criteria={self.jobs_in.key: {"$nin": has_output}})
        for job in jobs_todo:
            structures = []
            species = []
            for el, oxi_states in job['element_oxidation_states'].items():
                for oxi in oxi_states:
                    species.append(Specie(el, int(oxi)))
            t = float(job['threshold'])
            predictions = SubstitutionPredictor(threshold=t).list_prediction(
                    species)
            for p in tqdm(predictions, "predictions", len(predictions)):
                subs = p['substitutions']
                if len(set(subs.values())) < len(species):
                    continue
                sub_t = SubstitutionTransformation(subs)
                target = map(str, subs.keys())
                for ts in self.fetch_structs_known(target):
                    ts.append_transformation(sub_t)
                    if ts.final_structure.charge == 0:
                        structures.append({'ts': ts,
                                           'rf': ts.composition.reduced_formula,
                                           'probability': p['probability']})
            structures.sort(key=lambda s: s['rf'])
            for k, g in groupby(structures, key=lambda s: s['rf']):
                yield {"structures": list(g), "job": job}

    def fetch_structs_known(self, species):
        return [TransformedStructure.from_dict(e)
                for e in self.structs_known.query(criteria={
                    "_materialsproject.catspecies": "".join(sorted(species))})]

    def process_item(self, item):
        # remove duplicates, keeping highest probability
        sm = StructureMatcher(comparator=ElementComparator())
        structs = item['structures']
        structs.sort(key=lambda s: s['probability'], reverse=True)
        filtered_structs = []
        rf = structs[0]['rf']
        for s in tqdm(structs, "structures: {}".format(rf), len(structs)):
            if not any(sm.fit(s['ts'].final_structure, s2['ts'].final_structure)
                       for s2 in filtered_structs):
                filtered_structs.append(s)
        results = []
        for i, s in enumerate(filtered_structs):
            entry = s['ts'].as_dict()
            entry['pretty_formula'] = s['rf']
            entry['probability'] = s['probability']
            entry['nsites'] = len(s['ts'].final_structure)
            sga = SpacegroupAnalyzer(s['ts'].final_structure)
            entry['space_group'] = {'symbol': sga.get_space_group_symbol()}
            entry['sp_id'] = item['job']['structure_predictor_id']
            entry['xtal_id'] = i
            results.append(entry)
        return {
            'job_out': {'sp_id': item['job']['structure_predictor_id']},
            'structs': results
        }

    def update_targets(self, items):
        for item in items:
            self.structs_pred.update(item["structs"], key=["sp_id", "xtal_id"])
            item["job_out"].update({"state": "success"})
            self.jobs_out.update([item["job_out"]])
