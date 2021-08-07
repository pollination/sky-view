from pollination.sky_view.entry import SkyViewEntryPoint
from queenbee.recipe.dag import DAG


def test_sky_view():
    recipe = SkyViewEntryPoint().queenbee
    assert recipe.name == 'sky-view-entry-point'
    assert isinstance(recipe, DAG)
