from pollination_dsl.dag import Inputs, DAG, task, Outputs
from dataclasses import dataclass
from pollination.honeybee_radiance.sky import GenSkyWithCertainIllum
from pollination.honeybee_radiance.octree import CreateOctreeWithSky
from pollination.honeybee_radiance.translate import CreateRadianceFolderGrid

# input/output alias
from pollination.alias.inputs.model import hbjson_model_grid_input
from pollination.alias.inputs.radiancepar import rad_par_sky_view_input
from pollination.alias.inputs.bool_options import cloudy_uniform_input
from pollination.alias.inputs.grid import sensor_count_input, grid_filter_input
from pollination.alias.outputs.daylight import sky_view_results

from ._raytracing import SkyViewRayTracing


@dataclass
class SkyViewEntryPoint(DAG):
    """Sky view entry point."""

    # inputs
    sensor_count = Inputs.int(
        default=200,
        description='The maximum number of grid points per parallel execution',
        spec={'type': 'integer', 'minimum': 1},
        alias=sensor_count_input
    )

    cloudy_sky = Inputs.str(
        description='A switch to indicate whether the sky is overcast clouds instead '
        'of uniform.', default='uniform',
        spec={'type': 'string', 'enum': ['cloudy', 'uniform']},
        alias=cloudy_uniform_input
    )

    grid_filter = Inputs.str(
        description='Text for a grid identifer or a pattern to filter the sensor grids '
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

    model = Inputs.file(
        description='A Honeybee model in HBJSON file format.',
        extensions=['json', 'hbjson'],
        alias=hbjson_model_grid_input
    )

    @task(template=GenSkyWithCertainIllum)
    def generate_sky(self, uniform=cloudy_sky, ground_reflectance=0):
        return [
            {
                'from': GenSkyWithCertainIllum()._outputs.sky,
                'to': 'resources/input.sky'
            }
        ]

    @task(template=CreateRadianceFolderGrid)
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
                'to': 'results/grids_info.json'
            },
            {
                'from': CreateRadianceFolderGrid()._outputs.sensor_grids,
                'description': 'Sensor grids information.'
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
        template=SkyViewRayTracing,
        needs=[create_rad_folder, create_octree],
        loop=create_rad_folder._outputs.sensor_grids,
        sub_folder='initial_results/{{item.name}}',  # create a subfolder for each grid
        sub_paths={'sensor_grid': 'grid/{{item.full_id}}.pts'}  # sensor_grid sub_path
    )
    def sky_view_ray_tracing(
        self,
        sensor_count=sensor_count,
        radiance_parameters=radiance_parameters,
        octree_file=create_octree._outputs.scene_file,
        grid_name='{{item.full_id}}',
        sensor_grid=create_rad_folder._outputs.model_folder,
        bsdfs=create_rad_folder._outputs.bsdf_folder
    ):
        # this task doesn't return a file for each loop.
        # instead we access the results folder directly as an output
        pass

    results = Outputs.folder(
        source='results', description='Folder with raw result files (.res) that contain '
        'sky view (or exposure)) values for each sensor.', alias=sky_view_results
    )
