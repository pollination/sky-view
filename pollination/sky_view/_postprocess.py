from pollination_dsl.dag import Inputs, GroupedDAG, task, Outputs
from dataclasses import dataclass
from pollination.honeybee_radiance.grid import MergeFolderData
from pollination.path.copy import CopyFile


@dataclass
class SkyViewPostprocess(GroupedDAG):
    """Post-process for sky view."""

    # inputs
    input_folder = Inputs.folder(
        description='Folder with initial results before redistributing the '
        'results to the original grids.'
    )

    grids_info = Inputs.file(
        description='Grids information from the original model.'
    )

    @task(template=CopyFile)
    def copy_grid_info(self, src=grids_info):
        return [
            {
                'from': CopyFile()._outputs.dst,
                'to': 'results/sky_view/grids_info.json'
            }
        ]

    @task(
        template=MergeFolderData
    )
    def restructure_results(self, input_folder=input_folder, extension='res'):
        return [
            {
                'from': MergeFolderData()._outputs.output_folder,
                'to': 'results/sky_view'
            }
        ]

    results = Outputs.folder(
        source='results', description='results folder.'
    )
