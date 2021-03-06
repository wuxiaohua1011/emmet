from maggma.builders.map_builder import MapBuilder
from maggma.core import Store
from pymatgen.core import Structure
from pymatgen.core import __version__ as pymatgen_version

from emmet.core.oxidation_states import OxidationStateDoc


class OxidationStatesBuilder(MapBuilder):
    def __init__(
        self,
        materials: Store,
        oxidation_states: Store,
        **kwargs,
    ):
        """
        Creates Oxidation State documents from materials

        Args:
            materials: Store of materials docs
            oxidation_states: Store to update with oxidation state document
            query : query on materials to limit search
        """
        self.materials = materials
        self.oxidation_states = oxidation_states
        self.kwargs = kwargs

        # Enforce that we key on material_id
        self.materials.key = "material_id"
        self.oxidation_states.key = "material_id"
        super().__init__(
            source=materials,
            target=oxidation_states,
            projection=["structure"],
            **kwargs,
        )

    def unary_function(self, item):
        structure = Structure.from_dict(item["structure"])
        oxi_doc = OxidationStateDoc.from_structure(structure)
        doc = oxi_doc.dict()

        doc.update(
            {
                "pymatgen_version": pymatgen_version,
                "successful": True,
            }
        )

        return doc
