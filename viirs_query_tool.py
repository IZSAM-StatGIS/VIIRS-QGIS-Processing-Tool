# -*- coding: utf-8 -*-

"""
VIIRS query - QGIS Processing Script

Queries monthly VIIRS raster datasets hosted on Ellipsis Drive via REST API.

- Mandatory selection of input features (max 5)
- Monthly time range selection (year/month)
- Automatic CRS transform to EPSG:4326
- Automatic batching of timestampIds (max 50 per request)
- Output is a table (no geometry)
  - Points: pixel_value
  - Lines/Polygons: min/max/mean/median/deviation/sum
- Numeric outputs rounded to 2 decimals
"""

import json
import requests

from qgis.PyQt.QtCore import QCoreApplication, QDate, QVariant
from qgis.core import (
    QgsProcessingAlgorithm,
    QgsProcessingParameterEnum,
    QgsProcessingParameterVectorLayer,
    QgsProcessingParameterField,
    QgsProcessingParameterFeatureSink,
    QgsProcessing,
    QgsProcessingException,
    QgsCoordinateReferenceSystem,
    QgsCoordinateTransform,
    QgsProject,
    QgsWkbTypes,
    QgsGeometry,
    QgsFields,
    QgsField,
    QgsFeature,
    QgsFeatureSink,
)


class VIIRSQuery(QgsProcessingAlgorithm):
    P_DATASET = "DATASET"
    P_INPUT_LAYER = "INPUT_LAYER"
    P_ID_FIELD = "ID_FIELD"  # optional

    P_START_YEAR = "START_YEAR"
    P_START_MONTH = "START_MONTH"
    P_END_YEAR = "END_YEAR"
    P_END_MONTH = "END_MONTH"

    P_OUTPUT = "OUTPUT"

    MAX_SELECTED = 5
    FIRST_YEAR = 2018
    MAX_TIMESTAMPS_PER_REQUEST = 50

    DATASETS = {
        "LSTD": "5510ddc9-57fb-4014-b751-9da99fa56ae8",
        "LSTN": "7d5e1d57-7d0f-4a31-9a1c-a86d7245bd20",
        "NDVI": "4b276542-bdea-44c1-a206-36ed14531eee",
        "EVI":  "e933ceba-c12e-4e1f-8171-2c02706cea5f",
    }

    # ----------------------
    # Metadata
    # ----------------------
    def tr(self, s):
        return QCoreApplication.translate("VIIRSQuery", s)

    def createInstance(self):
        return VIIRSQuery()

    def name(self):
        return "viirs_query"

    def displayName(self):
        return self.tr("VIIRS query")

    def group(self):
        return self.tr("IZS Tools")

    def groupId(self):
        return "izstools"

    def shortHelpString(self):
        return self.tr(
            "Extract monthly VIIRS values (NDVI/EVI/LST) from Ellipsis Drive.\n\n"
            "How to use:\n"
            "1) Choose the VIIRS dataset.\n"
            "2) Select 1–5 features in the input layer (points/lines/polygons).\n"
            "3) Set the time range (year/month).\n\n"
            "Constraints:\n"
            "- Feature selection is mandatory; the tool will not process all layer features.\n"
            "- Max 5 selected features to limit the number of API requests.\n"
            "- API limitation: max 50 timestamps per request; batching is automatic.\n\n"
            "Output:\n"
            "- Points: pixel_value.\n"
            "- Lines/polygons: raster statistics.\n"
        )

    # ----------------------
    # Parameters
    # ----------------------
    def initAlgorithm(self, config=None):
        self.addParameter(
            QgsProcessingParameterEnum(
                self.P_DATASET,
                self.tr("VIIRS dataset"),
                options=list(self.DATASETS.keys()),
                defaultValue=0,
            )
        )

        self.addParameter(
            QgsProcessingParameterVectorLayer(
                self.P_INPUT_LAYER,
                self.tr("Input layer (select 1–5 features)"),
                types=[
                    QgsProcessing.TypeVectorPoint,
                    QgsProcessing.TypeVectorLine,
                    QgsProcessing.TypeVectorPolygon,
                ],
            )
        )

        self.addParameter(
            QgsProcessingParameterField(
                self.P_ID_FIELD,
                self.tr("Optional ID field (for joins)"),
                parentLayerParameterName=self.P_INPUT_LAYER,
                optional=True,
            )
        )

        current_year = QDate.currentDate().year()
        years = [str(y) for y in range(self.FIRST_YEAR, current_year + 1)]
        months = [f"{m:02d}" for m in range(1, 13)]

        self.addParameter(QgsProcessingParameterEnum(self.P_START_YEAR, self.tr("Start year"), years, defaultValue=0))
        self.addParameter(QgsProcessingParameterEnum(self.P_START_MONTH, self.tr("Start month"), months, defaultValue=0))
        self.addParameter(QgsProcessingParameterEnum(self.P_END_YEAR, self.tr("End year"), years, defaultValue=len(years) - 1))
        self.addParameter(
            QgsProcessingParameterEnum(
                self.P_END_MONTH,
                self.tr("End month"),
                months,
                defaultValue=QDate.currentDate().month() - 1,  # 0-based
            )
        )

        self.addParameter(
            QgsProcessingParameterFeatureSink(
                self.P_OUTPUT,
                self.tr("Output table"),
            )
        )

    # ----------------------
    # Helpers
    # ----------------------
    @staticmethod
    def _chunk_list(lst, size):
        for i in range(0, len(lst), size):
            yield lst[i:i + size]

    @staticmethod
    def _round2(value):
        if value is None:
            return None
        try:
            return round(float(value), 2)
        except Exception:
            return None

    @staticmethod
    def _yyyymm_from_iso(iso_str: str) -> int:
        if not iso_str or len(iso_str) < 7:
            return -1
        try:
            y = int(iso_str[0:4])
            m = int(iso_str[5:7])
            return y * 100 + m
        except Exception:
            return -1

    @staticmethod
    def _date_only(iso_str: str) -> str:
        if not iso_str:
            return ""
        s = str(iso_str)
        return s[:10] if len(s) >= 10 else s

    @staticmethod
    def _pixel_value_from_statistics(stats: dict):
        """For point inputs: prefer histogram[0].bin, fallback to mean."""
        if not stats:
            return None
        hist = stats.get("histogram")
        if isinstance(hist, list) and hist:
            first = hist[0] or {}
            if isinstance(first, dict) and first.get("bin") is not None:
                return first.get("bin")
        return stats.get("mean")

    # ----------------------
    # Main
    # ----------------------
    def processAlgorithm(self, parameters, context, feedback):
        vlayer = self.parameterAsVectorLayer(parameters, self.P_INPUT_LAYER, context)
        if vlayer is None:
            raise QgsProcessingException(self.tr("Invalid input layer."))

        selected = list(vlayer.getSelectedFeatures())
        if len(selected) == 0:
            raise QgsProcessingException(self.tr("Select at least 1 feature in the input layer before running the tool."))
        if len(selected) > self.MAX_SELECTED:
            raise QgsProcessingException(
                self.tr(f"Too many selected features ({len(selected)}). Max allowed: {self.MAX_SELECTED}.")
            )

        id_field = (self.parameterAsString(parameters, self.P_ID_FIELD, context) or "").strip()
        is_point_input = (vlayer.geometryType() == QgsWkbTypes.PointGeometry)

        dataset_index = self.parameterAsEnum(parameters, self.P_DATASET, context)
        dataset_name = list(self.DATASETS.keys())[dataset_index]
        dataset_id = self.DATASETS[dataset_name]

        # --- time range
        current_year = QDate.currentDate().year()
        years = [str(y) for y in range(self.FIRST_YEAR, current_year + 1)]
        months = [f"{m:02d}" for m in range(1, 13)]

        sy = int(years[self.parameterAsEnum(parameters, self.P_START_YEAR, context)])
        sm = int(months[self.parameterAsEnum(parameters, self.P_START_MONTH, context)])
        ey = int(years[self.parameterAsEnum(parameters, self.P_END_YEAR, context)])
        em = int(months[self.parameterAsEnum(parameters, self.P_END_MONTH, context)])

        start_key = sy * 100 + sm
        end_key = ey * 100 + em
        if start_key > end_key:
            raise QgsProcessingException(self.tr("Invalid time range: start is after end."))

        # --- timestamps list
        path_url = f"https://api.ellipsis-drive.com/v3/path/{dataset_id}"
        r = requests.get(path_url, timeout=60)
        if r.status_code != 200:
            raise QgsProcessingException(self.tr(f"Timestamp request failed (HTTP {r.status_code})."))

        data = r.json()
        timestamps = (data.get("raster") or {}).get("timestamps") or []
        if not isinstance(timestamps, list) or not timestamps:
            raise QgsProcessingException(self.tr("No timestamps found in API response."))

        ts_info = {}
        ts_ids = []
        for ts in timestamps:
            ts_id = ts.get("id")
            date_node = ts.get("date") or {}
            date_from_iso = str(date_node.get("from") or "")
            date_to_iso = str(date_node.get("to") or "")
            desc = str(ts.get("description") or "")

            key = self._yyyymm_from_iso(date_from_iso)
            if key == -1 or ts_id is None:
                continue

            if start_key <= key <= end_key:
                ts_id = str(ts_id)
                ts_ids.append(ts_id)
                ts_info[ts_id] = {
                    "date_from": self._date_only(date_from_iso),
                    "date_to": self._date_only(date_to_iso),
                    "description": desc,
                }

        if not ts_ids:
            raise QgsProcessingException(self.tr("No timestamps available in the selected time range."))

        # --- output schema
        fields = QgsFields()
        fields.append(QgsField("input_fid", QVariant.LongLong))
        fields.append(QgsField("input_id", QVariant.String))
        fields.append(QgsField("dataset", QVariant.String))
        fields.append(QgsField("timestampId", QVariant.String))
        fields.append(QgsField("date_from", QVariant.String))
        fields.append(QgsField("date_to", QVariant.String))
        fields.append(QgsField("description", QVariant.String))
        fields.append(QgsField("hasData", QVariant.Int))

        if is_point_input:
            fields.append(QgsField("pixel_value", QVariant.Double))
        else:
            fields.append(QgsField("min", QVariant.Double))
            fields.append(QgsField("max", QVariant.Double))
            fields.append(QgsField("mean", QVariant.Double))
            fields.append(QgsField("median", QVariant.Double))
            fields.append(QgsField("deviation", QVariant.Double))
            fields.append(QgsField("sum", QVariant.Double))

        sink, sink_id = self.parameterAsSink(
            parameters,
            self.P_OUTPUT,
            context,
            fields,
            QgsWkbTypes.NoGeometry,
            QgsCoordinateReferenceSystem(),
        )

        analyse_url = f"https://api.ellipsis-drive.com/v3/path/{dataset_id}/raster/timestamp/analyse"
        batches = list(self._chunk_list(ts_ids, self.MAX_TIMESTAMPS_PER_REQUEST))

        # CRS transform once
        need_transform = (vlayer.crs().authid() != "EPSG:4326")
        if need_transform:
            transform = QgsCoordinateTransform(
                vlayer.crs(),
                QgsCoordinateReferenceSystem("EPSG:4326"),
                QgsProject.instance(),
            )
        else:
            transform = None

        # --- process features
        for f in selected:
            input_fid = f.id()
            input_id_value = ""
            if id_field:
                raw = f.attribute(id_field)
                input_id_value = "" if raw is None else str(raw)

            geom = QgsGeometry(f.geometry())
            if geom is None or geom.isEmpty():
                continue

            # If geometry is curved, best-effort segmentize via abstract geometry (compat-safe)
            if QgsWkbTypes.isCurvedType(geom.wkbType()):
                try:
                    abs_g = geom.constGet()
                    if hasattr(abs_g, "segmentize"):
                        geom = QgsGeometry(abs_g.segmentize())
                except Exception:
                    pass

            if transform is not None:
                geom.transform(transform)

            geometry_param = geom.asJson()  # GeoJSON geometry string

            for batch in batches:
                params = {
                    "timestampIds": json.dumps(batch),
                    "geometry": geometry_param,
                    "returnType": "statistics",
                }

                rr = requests.get(analyse_url, params=params, timeout=180)
                if rr.status_code != 200:
                    raise QgsProcessingException(
                        self.tr(f"Analyse request failed (HTTP {rr.status_code}): {(rr.text or '')[:200]}")
                    )

                analysed = rr.json()
                if not isinstance(analysed, list):
                    raise QgsProcessingException(self.tr("Unexpected analyse response (expected a JSON list)."))

                for item in analysed:
                    ts_id = str((item.get("timestamp") or {}).get("id") or "")
                    has_data = 1 if item.get("hasData") else 0

                    info = ts_info.get(ts_id, {})
                    desc = info.get("description", "")
                    date_from = info.get("date_from", "")
                    date_to = info.get("date_to", "")

                    # band 1 only
                    stats = None
                    for res in item.get("result") or []:
                        if (res.get("band") or {}).get("number") == 1:
                            stats = res.get("statistics") or {}
                            break

                    base_attrs = [
                        input_fid,
                        input_id_value,
                        dataset_name,
                        ts_id,
                        date_from,
                        date_to,
                        desc,
                        has_data,
                    ]

                    out_f = QgsFeature(fields)

                    if is_point_input:
                        val = self._round2(self._pixel_value_from_statistics(stats)) if has_data else None
                        out_f.setAttributes(base_attrs + [val])
                    else:
                        out_f.setAttributes(base_attrs + [
                            self._round2(stats.get("min") if stats else None),
                            self._round2(stats.get("max") if stats else None),
                            self._round2(stats.get("mean") if stats else None),
                            self._round2(stats.get("median") if stats else None),
                            self._round2(stats.get("deviation") if stats else None),
                            self._round2(stats.get("sum") if stats else None),
                        ])

                    sink.addFeature(out_f, QgsFeatureSink.FastInsert)

        return {self.P_OUTPUT: sink_id}
