from pollination_dsl.dag import Inputs, GroupedDAG, task, Outputs
from dataclasses import dataclass
from pollination.honeybee_radiance.grid import MergeFolderData
from pollination.path.copy import CopyFile
from pollination.honeybee_radiance.post_process import SkyViewVisMetadata
from pollination.honeybee_display.translate import ModelToVis


@dataclass
class SkyViewPostprocess(GroupedDAG):
    """Post-process for sky view."""

    # inputs
    model = Inputs.file(
        description='A Honeybee Model in either JSON or Pkl format. This can also '
        'be a zipped honeybee-radiance folder.',
        extensions=['json', 'hbjson', 'pkl', 'hbpkl', 'zip']
    )

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

    @task(
        template=SkyViewVisMetadata,
        needs=[restructure_results]
    )
    def create_vis_metadata(self):
        return [
            {
                'from': SkyViewVisMetadata()._outputs.cfg_file,
                'to': 'results/sky_view/vis_metadata.json'
            }
        ]

    @task(
        template=ModelToVis,
        needs=[create_vis_metadata, copy_grid_info]
    )
    def create_vsf(
        self, model=model, grid_data='results', output_format='vsf'
    ):
        return [
            {
                'from': ModelToVis()._outputs.output_file,
                'to': 'visualization.vsf'
            }
        ]

    visualization = Outputs.file(
        source='visualization.vsf',
        description='Result visualization in VisualizationSet format.'
    )

    results = Outputs.folder(
        source='results', description='results folder.'
    )
