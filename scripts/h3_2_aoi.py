from dataclasses import dataclass

import h3


@dataclass
class H3Handler:
    h3_index: str

    @property
    def to_latlng(self) -> tuple[float, float]:
        return h3.cell_to_latlng(self.h3_index)

    @property
    def to_bbox(self) -> tuple[float, float, float, float]:
        return h3.cell_to_boundary(self.h3_index)

    @property
    def to_geojson(self) -> dict:
        boundary = h3.cell_to_boundary(self.h3_index)
        coords = [[lng, lat] for lat, lng in boundary]
        coords.append(coords[0])  # zamknięcie poligonu

        return {
            "type": "Feature",
            "properties": {"h3_index": self.h3_index},
            "geometry": {"type": "Polygon", "coordinates": [coords]},
        }
