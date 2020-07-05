import itertools
import math
from typing import List

from qgis.core import *

import config
from exceptions import SearchRecursionError
from tasks import TaskDefinition


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

    def densify(self, task: TaskDefinition) -> List[TaskDefinition]:

        # check if further recursion is possible
        radius = task.radius / 2
        if radius <= config.MIN_ALLOWED_RADIUS:
            raise SearchRecursionError

        wgs_point = QgsPointXY(task.lon, task.lat)
        metric_point: QgsPointXY = self.transformer_to_metric.transform(wgs_point)

        # cut it as 3:2 ratio
        distance = task.radius * 0.6
        densified_metric = [metric_point.project(distance, a) for a in self.DENSIFY_ANGLES]
        densified_wgs = [self.transformer_to_wgs.transform(pt) for pt in densified_metric]

        # as well, include the center point with smaller radius
        task_for_center = TaskDefinition(task.lon, task.lat, radius * 0.75, task.place_type)

        return [
            TaskDefinition(
                lon=pt.x(),
                lat=pt.y(),
                radius=radius,
                place_type=task.place_type
            ) for pt in densified_wgs   # array of QgsPointXYs
        ] + [task_for_center]


def __buffer_intersects(point: [QgsPoint, QgsPointXY], polygon: QgsGeometry, buffer_by: float) -> bool:

    assert buffer_by > 0, f"buffer distance must be greater than zero!"

    if isinstance(point, QgsPoint):
        point_as_geom = QgsGeometry.fromPointXY(QgsPointXY(point.x(), point.y()))
    elif isinstance(point, QgsPointXY):
        point_as_geom = QgsGeometry.fromPointXY(point)
    else:
        raise Exception(f"unexpected type {type(point)}")

    if point_as_geom.intersects(polygon):
        # first, check if point itself intersects the polygon
        return True
    else:
        # if not, check with buffer
        buff = point_as_geom.buffer(distance=buffer_by, segments=36)
        return buff.intersects(polygon) or buff.overlaps(polygon)   # return True if overlaps


def __assert_polygon(polygon: QgsGeometry):

    assert not polygon.isEmpty(), r"received empty geometry as input!"
    assert not polygon.isNull(), r"received null geometry as input!"
    assert polygon.isGeosValid(), r"received invalid polygon as input!"
    assert polygon.wkbType() == 3, f"expected singlepart polygon geometry, received wkbType {polygon.wkbType()}"
    assert polygon.area() > 0, r"received polygon with zero area!"


def make_grid(polygon: QgsGeometry, spacing: float, metric_epsg: int) -> List[QgsPoint]:

    """

    Helps create initial grid

    :param polygon:     Valid singlepart polygon as QgsGeometry in WGS 84 CRS.
    :param spacing:
    :return:
    """

    assert metric_epsg > 0, f"invalid EPSG code {metric_epsg}"

    MIN_DIMENSION = 10  # only for the warning message
    transformer = QgsCoordinateTransform(
        QgsCoordinateReferenceSystem.fromEpsgId(epsg=4326),
        QgsCoordinateReferenceSystem.fromEpsgId(epsg=metric_epsg),
        QgsProject.instance()
    )

    __assert_polygon(polygon)
    polygon.transform(transformer, QgsCoordinateTransform.ForwardTransform)   # inplace

    # set bounds
    bbox: QgsRectangle = polygon.boundingBox()
    xmin, xmax, ymin, ymax = bbox.xMinimum(), bbox.xMaximum(), bbox.yMinimum(), bbox.yMaximum()

    # calculate grid
    n_rows = math.ceil((xmax - xmin) // spacing)
    n_columns = math.ceil((xmax - xmin) / spacing)

    if n_rows == 0 or n_columns == 0:
        raise Exception(
            f"cannot create grid with provided parameters: \n"
            f"\t\tdelta X = {xmax - xmin:.1f}, delta Y = {xmax - xmin:.1f}, spacing = {spacing:.1f}, "
            f"shape {n_rows} x {n_columns}"
        )

    elif n_rows < MIN_DIMENSION or n_columns < MIN_DIMENSION:
        print(
            f"WARN: creating grid of potentially unwanted shape {n_rows} x {n_columns}.\n"
            f"\t\tdelta X = {xmax - xmin:.1f}, delta Y = {xmax - xmin:.1f}, spacing = {spacing:.1f}"
        )

    else:
        print(
            f"WARN: creating grid with rows x columns = {n_rows} x {n_columns}"
        )

    x_columns = [xmin + spacing * i for i in range(0, n_columns + 1)]   # make a little extra
    y_rows = [ymin + spacing * i for i in range(0, n_rows + 1)]         # make a little extra

    entire_grid = [QgsPoint(*xy) for xy in itertools.product(x_columns, y_rows)]
    points_within = [pt for pt in entire_grid if __buffer_intersects(pt, polygon, buffer_by=spacing*1.25)]

    print(
        f"INFO: total {len(points_within)} points selected out of "
        f"the original grid of {len(entire_grid)} points with spacing = {spacing:.1f} m")

    # project points back to WGS 84
    points_within_wgs84 = []
    for p in points_within:
        geom = QgsGeometry(p)
        geom.transform(transformer, QgsCoordinateTransform.ReverseTransform)
        points_within_wgs84.append(geom.asPoint())
        # p.transform(transformer, QgsCoordinateTransform.ReverseTransform)  # inplace

    del transformer

    return points_within_wgs84


def get_aoi_polygon(layer_uri: str):

    assert layer_uri, 'invalid layer uri'
    lyr = QgsVectorLayer(layer_uri, 'whatever name you want', 'ogr')

    assert lyr.isValid(), "layer is invalid!"
    assert lyr.featureCount() > 0, "no features found in layer"
    assert lyr.crs().authid().lower() == "epsg:4326", f'layer CRS must be WGS 84 (EPSG:4326), received {lyr.crs().authid()}'

    geoms = [ft.geometry() for ft in lyr.getFeatures()]

    assert len(geoms) > 0, f"no geometries can be extracted from layer {layer_uri}"

    aoi = geoms[0]
    __assert_polygon(aoi)       # what about multipolygon here ?

    return aoi
