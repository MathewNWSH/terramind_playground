from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union


@dataclass
class MyCube:
    s3_paths: list[Path]
    dates: list[datetime]
    geometries: list[BaseGeometry]

    def group_by_fully_covered(self, aoi: BaseGeometry) -> dict[str, Any]:
        ready_single = []
        ready_mosaic = []

        scenes_by_date = defaultdict(list)
        for path, geom, date in zip(
            self.s3_paths, self.geometries, self.dates, strict=False
        ):
            scenes_by_date[date].append({
                "s3_path": path,
                "geometry": geom,
                "date": date,
            })

        for date, scenes in scenes_by_date.items():
            for scene in scenes:
                if aoi.within(scene["geometry"]):
                    ready_single.append(scene)

            union_geom = unary_union([scene["geometry"] for scene in scenes])
            if aoi.within(union_geom):
                ready_mosaic.append({
                    "date": date,
                    "s3_paths": [scene["s3_path"] for scene in scenes],
                })

        return {"single": ready_single, "mosaic": ready_mosaic}
