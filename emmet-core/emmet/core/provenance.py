""" Core definition of a Provenance Document """
import warnings
from datetime import datetime
from typing import ClassVar, Dict, List, Optional

from pybtex.database import BibliographyData, parse_string
from pydantic import BaseModel, EmailStr, Field, validator

from emmet.core.material_property import PropertyDoc
from emmet.core.mpid import MPID
from emmet.core.utils import ValueEnum


class Database(ValueEnum):
    """
    Database identifiers for provenance IDs
    """

    ICSD = "icsd"
    Pauling_Files = "pf"
    COD = "cod"


class Author(BaseModel):
    """
    Author information
    """

    name: str = Field(None)
    email: EmailStr = Field(None)


class History(BaseModel):
    """
    History of the material provenance
    """

    name: str
    url: str
    description: Optional[Dict] = Field(
        None, description="Dictionary of exra data for this history node"
    )


class ProvenanceDoc(PropertyDoc):
    """
    A provenance property block
    """

    property_name: ClassVar[str] = "provenance"

    created_at: datetime = Field(
        ...,
        description="creation date for the first structure corresponding to this material",
    )

    references: List[str] = Field(
        [], description="Bibtex reference strings for this material"
    )

    authors: List[Author] = Field([], description="List of authors for this material")

    remarks: List[str] = Field(
        [], description="List of remarks for the provenance of this material"
    )

    tags: List[str] = Field([])

    theoretical: bool = Field(
        True, description="If this material has any experimental provenance or not"
    )

    database_IDs: Dict[Database, List[str]] = Field(
        dict(), description="Database IDs corresponding to this material"
    )

    history: List[History] = Field(
        [],
        description="List of history nodes specifying the transformations or orignation"
        " of this material for the entry closest matching the material input",
    )

    @validator("authors")
    def remove_duplicate_authors(cls, authors):
        authors_dict = {entry.name.lower(): entry for entry in authors}
        return list(authors_dict.items())

    @classmethod
    def from_SNLs(
        cls,
        material_id: MPID,
        snls: List[Dict],
    ) -> "ProvenanceDoc":
        """
        Converts legacy Pymatgen SNLs into a single provenance document
        """

        # Choose earliest created_at
        created_at = sorted(
            [
                snl.get("about", {}).get("created_at", {}).get("string", datetime.max)
                for snl in snls
            ]
        )[0]

        # Choose earliest history
        history = sorted(
            snls,
            key=lambda snl: snl.get("about", {})
            .get("created_at", {})
            .get("string", datetime.max),
        )[0]["about"]["history"]

        # Aggregate all references into one dict to remove duplicates
        refs = {}
        for snl in snls:
            try:
                entries = parse_string(snl["about"]["references"], bib_format="bibtex")
                refs.update(entries.entries)
            except Exception:
                warnings.warn(f"Failed parsing bibtex: {snl['about']['references']}")

        bib_data = BibliographyData(entries=refs)
        references = [ref.to_string("bibtex") for ref in bib_data.entries]

        # TODO: Maybe we should combine this robocrystallographer?
        # TODO: Refine these tags / remarks
        remarks = list(
            set([remark for snl in snls for remark in snl["about"]["remarks"]])
        )
        tags = [r for r in remarks if len(r) < 140]

        # Aggregate all authors - Converting a single dictionary first
        # performs duplicate checking
        authors_dict = {
            entry["name"].lower(): entry["email"]
            for snl in snls
            for entry in snl["about"]["authors"]
        }
        authors = [
            {"name": name.title(), "email": email}
            for name, email in authors_dict.items()
        ]

        # Check if this entry is experimental
        if any(
            snl.get("about", {}).get("history", [{}])[0].get("experimental", False)
            for snl in snls
        ):
            experimental = True

        # Aggregate all the database IDs
        snl_ids = [snl.get("snl_id", "") for snl in snls]
        db_ids = {
            Database(db_id): [snl_id for snl_id in snl_ids if db_id in snl_id]
            for db_id in map(str, Database)
        }

        # remove Nones and empty lists
        db_ids = {k: list(filter(None, v)) for k, v in db_ids.items()}
        db_ids = {k: v for k, v in db_ids.items() if len(v) > 0}

        # Get experimental bool
        experimental = any(
            snl.get("about", {}).get("history", [{}])[0].get("experimental", False)
            for snl in snls
        )

        snl_fields = {
            "created_at": created_at,
            "references": references,
            "authors": authors,
            "remarks": remarks,
            "tags": tags,
            "database_IDs": db_ids,
            "theoretical": not experimental,
            "history": history,
        }

        return ProvenanceDoc(material_id=material_id, **snl_fields)
