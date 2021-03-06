from pathlib import Path

import pytest
from maggma.stores import JSONStore, MemoryStore
from monty.serialization import dumpfn, loadfn

from emmet.builders.materials.search import SearchBuilder
from emmet.builders.vasp.materials import MaterialsBuilder


@pytest.fixture(scope="session")
def tasks_store(test_dir):
    return JSONStore(test_dir / "test_si_tasks.json.gz")


@pytest.fixture(scope="session")
def materials(tasks_store):
    materials_store = MemoryStore(key="material_id")
    builder = MaterialsBuilder(tasks=tasks_store, materials=materials_store)
    builder.run()
    return materials_store


@pytest.fixture
def electronic_structure():
    return MemoryStore(key="material_id")


@pytest.fixture
def thermo():
    return MemoryStore(key="material_id")


@pytest.fixture
def grain_boundaries():
    return MemoryStore()


@pytest.fixture
def magnetism():
    return MemoryStore()


@pytest.fixture
def elasticity():
    return MemoryStore()


@pytest.fixture
def dielectric():
    return MemoryStore()


@pytest.fixture
def phonon():
    return MemoryStore()


@pytest.fixture
def insertion_electrodes():
    return MemoryStore()


@pytest.fixture
def substrates():
    return MemoryStore()


@pytest.fixture
def surfaces():
    return MemoryStore()


@pytest.fixture
def eos():
    return MemoryStore()


@pytest.fixture
def xas():
    return MemoryStore()


@pytest.fixture
def search():
    return MemoryStore(key="material_id")


def test_search_builder(
    materials,
    thermo,
    xas,
    grain_boundaries,
    electronic_structure,
    magnetism,
    elasticity,
    dielectric,
    phonon,
    insertion_electrodes,
    substrates,
    surfaces,
    eos,
    search,
):

    builder = SearchBuilder(
        materials=materials,
        electronic_structure=electronic_structure,
        thermo=thermo,
        magnetism=magnetism,
        dielectric=dielectric,
        phonon=phonon,
        insertion_electrodes=insertion_electrodes,
        elasticity=elasticity,
        substrates=substrates,
        surfaces=surfaces,
        xas=xas,
        grain_boundaries=grain_boundaries,
        eos=eos,
        search=search,
    )

    builder.run()
    assert search.count() == 1


def test_serialization(tmpdir):
    builder = SearchBuilder(
        MemoryStore(),
        MemoryStore(),
        MemoryStore(),
        MemoryStore(),
        MemoryStore(),
        MemoryStore(),
        MemoryStore(),
        MemoryStore(),
        MemoryStore(),
        MemoryStore(),
        MemoryStore(),
        MemoryStore(),
        MemoryStore(),
        MemoryStore(),
    )

    dumpfn(builder.as_dict(), Path(tmpdir) / "test.json")
    loadfn(Path(tmpdir) / "test.json")
