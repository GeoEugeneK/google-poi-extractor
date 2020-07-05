import config
from qgis.core import *
from geometries.geomworks import get_aoi_polygon, make_grid
import datetime


def as_feature(geom: QgsGeometry):
    ft = QgsFeature()
    ft.setGeometry(geom)
    return ft


if __name__ == '__main__':

    now = datetime.datetime.now()
    OUT_GPKG = f"./test/grid-{now.strftime('%H-%M-%S')}.gpkg"

    spacing = config.INITIAL_RADIUS * 2 / (2 ** 0.5)
    print(f"Guessed spacing = {spacing:.1f} m")

    aoi_polygon = get_aoi_polygon(config.AOI_LAYER_URI)
    initial_points = make_grid(aoi_polygon, spacing=spacing, metric_epsg=config.METRIC_CRS_EPSG)

    # make out layer and writer
    lyr = QgsVectorLayer("Point", "grid_check", "memory")
    lyr.setCrs(QgsCoordinateReferenceSystem.fromEpsgId(4326))

    # populate
    lyr.startEditing()
    for pt in initial_points:
        added = lyr.addFeature(feature=as_feature(geom=QgsGeometry.fromPointXY(pt)))
        if not added:
            print("WARN: feature was not added")

    # # save options
    # options: QgsVectorFileWriter.SaveVectorOptions = QgsVectorFileWriter.SaveVectorOptions()
    # options.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer
    # options.EditionCapability = QgsVectorFileWriter.CanAddNewLayer

    # write
    lyr.commitChanges()
    print(f"Layer with {lyr.featureCount()} features is about to be written in {OUT_GPKG}...")
    QgsVectorFileWriter.writeAsVectorFormat(lyr, OUT_GPKG, 'utf-8', lyr.crs(), "GPKG")

    print('DONE')
