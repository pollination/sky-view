from pollination_dsl.dag import Inputs, DAG, task, Outputs
from dataclasses import dataclass
from pollination.honeybee_radiance.raytrace import RayTracingSkyView

# input/output alias
from pollination.alias.inputs.model import hbjson_model_grid_input
from pollination.alias.inputs.radiancepar import rad_par_sky_view_input
from pollination.alias.inputs.bool_options import cloudy_uniform_input
from pollination.alias.inputs.grid import grid_filter_input, \
    min_sensor_count_input, cpu_count
from pollination.alias.outputs.daylight import sky_view_results

from ._prepare_folder import SkyViewPrepareFolder
from ._postprocess import SkyViewPostprocess


@dataclass
class SkyViewEntryPoint(DAG):
    """Sky view entry point."""

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

    @task(template=SkyViewPrepareFolder)
    def prepare_folder_sky_view(
        self, model=model, cpu_count=cpu_count,
        min_sensor_count=min_sensor_count, cloudy_sky=cloudy_sky,
        grid_filter=grid_filter
    ):
        return [
            {
                'from': SkyViewPrepareFolder()._outputs.model_folder,
                'to': 'model'
            },
            {
                'from': SkyViewPrepareFolder()._outputs.resources,
                'to': 'resources'
            },
            {
                'from': SkyViewPrepareFolder()._outputs.initial_results,
                'to': 'initial_results'
            },
            {
                'from': SkyViewPrepareFolder()._outputs.sensor_grids
            }
        ]

    @task(
        template=RayTracingSkyView,
        needs=[prepare_folder_sky_view],
        loop=prepare_folder_sky_view._outputs.sensor_grids,
        sub_folder='initial_results/{{item.full_id}}',  # subfolder for each grid
        sub_paths={
            'scene_file': 'scene.oct',
            'grid': 'grid/{{item.full_id}}.pts',
            'bsdf_folder': 'bsdf'
            }
    )
    def sky_view_ray_tracing(
        self,
        radiance_parameters=radiance_parameters,
        scene_file=prepare_folder_sky_view._outputs.resources,
        grid=prepare_folder_sky_view._outputs.resources,
        bsdf_folder=prepare_folder_sky_view._outputs.model_folder
    ):
        return [
            {
                'from': RayTracingSkyView()._outputs.result,
                'to': '../{{item.name}}.res'
            }
        ]

    @task(
        template=SkyViewPostprocess,
        needs=[prepare_folder_sky_view, sky_view_ray_tracing],
        sub_paths={
            'grids_info': 'grids_info.json'
            }
    )
    def postprocess_sky_view(
        self, input_folder=prepare_folder_sky_view._outputs.initial_results,
        grids_info=prepare_folder_sky_view._outputs.resources,
    ):
        return [
            {
                'from': SkyViewPostprocess()._outputs.results,
                'to': 'results'
            }
        ]

    results = Outputs.folder(
        source='results/sky_view', description='Folder with raw result files (.res) '
        'that contain sky view (or exposure)) values for each sensor.',
        alias=sky_view_results
    )
