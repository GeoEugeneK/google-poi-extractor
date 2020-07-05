import datetime

from qgis.PyQt.QtCore import QVariant
from qgis.core import *

from geometries.geomworks import Densifier
from main import make_initial_tasks
from tasks import TaskDefinition


def as_feature(geom: QgsGeometry, r: float):
    ft = QgsFeature()
    ft.setGeometry(geom)
    ft.setAttributes([r])
    return ft


if __name__ == '__main__':

    now = datetime.datetime.now()
    OUT_GPKG = f"./test/densify-{now.strftime('%H-%M-%S')}.gpkg"

    # make tasks firt
    tasks = make_initial_tasks()
    print(f"{len(tasks)} initial tasks produced")

    # then densify
    densifier = Densifier()
    dense_tasks = []
    for t in tasks:
        new_tasks = densifier.densify(task=t)
        dense_tasks.extend(new_tasks
                           )
    print(f"{len(dense_tasks)} denser tasks")

    # make out layer and writer
    lyr = QgsVectorLayer("Point", "densify_check", "memory")
    lyr.setCrs(QgsCoordinateReferenceSystem.fromEpsgId(4326))

    # populate
    lyr.startEditing()
    lyr.addAttribute(QgsField("R", QVariant.Double))
    for t in dense_tasks:
        t: TaskDefinition
        pt = QgsPoint(t.lon, t.lat)
        added = lyr.addFeature(feature=as_feature(geom=QgsGeometry(pt), r=t.radius))
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
