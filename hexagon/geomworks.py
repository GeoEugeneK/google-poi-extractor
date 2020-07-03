import math

from qgis.core import *

from hexagon.definition import SearchTaskDefinition
import config


class Densifier(object):

    transformer_to_wgs: QgsCoordinateTransform
    transformer_to_metric: QgsCoordinateTransform

    DENSIFY_ANGLES: list = [30, 90, 150, 210, 270, 330]  # degrees

    def __init__(self):

        assert config.METRIC_CRS_EPSG > 0 and isinstance(config.METRIC_CRS_EPSG, int), \
            f"invalid value {config.METRIC_CRS_EPSG} for metric coordinate system EPSG code"

        WGS_84 = QgsCoordinateReferenceSystem.fromEpsgId(4326)
        METRIC_CRS = QgsCoordinateReferenceSystem.fromEpsgId(config.METRIC_CRS_EPSG)

        self.transformer_to_wgs = QgsCoordinateTransform(
            METRIC_CRS,
            WGS_84,
            QgsProject.instance()
        )

        self.transformer_to_metric = QgsCoordinateTransform(
            WGS_84,
            METRIC_CRS,
            QgsProject.instance()
        )

    def densify(self, task: SearchTaskDefinition):

        wgs_point = QgsPointXY(task.lon, task.lat)
        metric_point: QgsPointXY = self.transformer_to_metric.transform(wgs_point)

        # cut it as 3:2 ratio
        distance = task.radius * 0.6
        densified_metric = [metric_point.project(distance, a) for a in self.DENSIFY_ANGLES]
        densified_wgs = [self.transformer_to_wgs.transform(pt) for pt in densified_metric]

        radius = task.radius / 2

        # as well, include the center point with smaller radius
        task_for_center = SearchTaskDefinition(task.lon, task.lat, task.radius * 0.4, task.place_type)

        return [
            SearchTaskDefinition(
                lon=pt.x(),
                lat=pt.y(),
                radius=radius,
                place_type=task.place_type
            ) for pt in densified_wgs   # array of QgsPointXYs
        ] + [task_for_center]

DENSIFY_ANGLES_DEG = [30, 90, 150, 210, 270, 330]     # degrees
DENSIFY_ANGLES_RAD = [math.radians(x) for x in DENSIFY_ANGLES_DEG]


def densify_hexagon(hexagon: QgsGeometry):

    assert hexagon.wkbType() == QgsWkbTypes.Polygon, \
        f"expected single-geom polygon as input, received geometry with wkbType = {hexagon.wkbType()}"

    pole, depth = hexagon.poleOfInaccessibility(precision=0.5)

    # now we need to understand whether the hexagon is bottom-flat or side-flat

