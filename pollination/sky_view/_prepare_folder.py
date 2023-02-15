from pollination_dsl.dag import Inputs, GroupedDAG, task, Outputs
from dataclasses import dataclass
from pollination.honeybee_radiance.sky import GenSkyWithCertainIllum
from pollination.honeybee_radiance.octree import CreateOctreeWithSky
from pollination.honeybee_radiance.translate import CreateRadianceFolderGrid
from pollination.honeybee_radiance.grid import SplitGridFolder, MergeFolderData
from pollination.honeybee_radiance.raytrace import RayTracingSkyView

# input/output alias
from pollination.alias.inputs.model import hbjson_model_grid_input
from pollination.alias.inputs.radiancepar import rad_par_sky_view_input
from pollination.alias.inputs.bool_options import cloudy_uniform_input
from pollination.alias.inputs.grid import grid_filter_input, \
    min_sensor_count_input, cpu_count
from pollination.alias.outputs.daylight import sky_view_results


@dataclass
class SkyViewPrepareFolder(GroupedDAG):
    """Prepare folder for sky view."""

    # inputs
    model = Inputs.file(
        description='A Honeybee model in HBJSON file format.',
        extensions=['json', 'hbjson', 'pkl', 'hbpkl', 'zip'],
        alias=hbjson_model_grid_input
    )

    cpu_count = Inputs.int(
        default=50,
        description='The maximum number of CPUs for parallel execution. This will be '
        'used to determine the number of sensors run by each worker.',
        spec={'type': 'integer', 'minimum': 1},
        alias=cpu_count
    )

    min_sensor_count = Inputs.int(
        description='The minimum number of sensors in each sensor grid after '
        'redistributing the sensors based on cpu_count. This value takes '
        'precedence over the cpu_count and can be used to ensure that '
        'the parallelization does not result in generating unnecessarily small '
        'sensor grids. The default value is set to 1, which means that the '
        'cpu_count is always respected.', default=500,
        spec={'type': 'integer', 'minimum': 1},
        alias=min_sensor_count_input
    )

    cloudy_sky = Inputs.str(
        description='A switch to indicate whether the sky is overcast clouds instead '
        'of uniform.', default='uniform',
        spec={'type': 'string', 'enum': ['cloudy', 'uniform']},
        alias=cloudy_uniform_input
    )

    grid_filter = Inputs.str(
        description='Text for a grid identifier or a pattern to filter the sensor grids '
        'of the model that are simulated. For instance, first_floor_* will simulate '
        'only the sensor grids that have an identifier that starts with '
        'first_floor_. By default, all grids in the model will be simulated.',
        default='*',
        alias=grid_filter_input
    )

    radiance_parameters = Inputs.str(
        description='The radiance parameters for ray tracing. Note that the -ab '
        'parameter is always equal to 1 regardless of input here and the -I parameter '
        'is fixed based on the metric', default='-aa 0.1 -ad 2048 -ar 64',
        alias=rad_par_sky_view_input
    )

    @task(template=GenSkyWithCertainIllum)
    def generate_sky(self, uniform=cloudy_sky, ground_reflectance=0):
        return [
            {
                'from': GenSkyWithCertainIllum()._outputs.sky,
                'to': 'resources/input.sky'
            }
        ]

    @task(template=CreateRadianceFolderGrid, annotations={'main_task': True})
    def create_rad_folder(
        self, input_model=model, grid_filter=grid_filter
            ):
        """Translate the input model to a radiance folder."""
        return [
            {
                'from': CreateRadianceFolderGrid()._outputs.model_folder,
                'to': 'model'
            },
            {
                'from': CreateRadianceFolderGrid()._outputs.bsdf_folder,
                'to': 'model/bsdf'
            },
            {
                'from': CreateRadianceFolderGrid()._outputs.model_sensor_grids_file,
                'to': 'resources/grids_info.json'
            }
        ]

    @task(
        template=CreateOctreeWithSky, needs=[generate_sky, create_rad_folder]
    )
    def create_octree(
        self, model=create_rad_folder._outputs.model_folder,
        sky=generate_sky._outputs.sky
    ):
        """Create octree from radiance folder and sky."""
        return [
            {
                'from': CreateOctreeWithSky()._outputs.scene_file,
                'to': 'resources/scene.oct'
            }
        ]

    @task(
        template=SplitGridFolder, needs=[create_rad_folder],
        sub_paths={'input_folder': 'grid'}
    )
    def split_grid_folder(
        self, input_folder=create_rad_folder._outputs.model_folder,
        cpu_count=cpu_count, cpus_per_grid=1, min_sensor_count=min_sensor_count
    ):
        """Split sensor grid folder based on the number of CPUs"""
        return [
            {
                'from': SplitGridFolder()._outputs.output_folder,
                'to': 'resources/grid'
            },
            {
                'from': SplitGridFolder()._outputs.dist_info,
                'to': 'initial_results/_redist_info.json'
            }
        ]

    model_folder = Outputs.folder(
        source='model', description='input model folder folder.'
    )

    resources = Outputs.folder(
        source='resources', description='resources folder.'
    )

    initial_results = Outputs.folder(
        source='initial_results', description='initial results folder.'
    )

    sensor_grids = Outputs.list(source='resources/grid/_info.json')
