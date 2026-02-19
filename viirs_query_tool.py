# -*- coding: utf-8 -*-

import json
import requests

from qgis.PyQt.QtCore import QCoreApplication, QDate, QVariant
from qgis.core import (
    QgsProcessingAlgorithm,
    QgsProcessingParameterEnum,
    QgsProcessingParameterVectorLayer,
    QgsProcessingParameterField,
    QgsProcessingParameterString,
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
    P_ID_FIELD = "ID_FIELD"

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
            "Questo strumento estrae valori mensili VIIRS (es. NDVI/EVI/LST) dal dataset su Ellipsis Drive.\n\n"
            "Come usarlo:\n"
            "1) Scegli il dataset VIIRS.\n"
            "2) Seleziona nel layer di input 1–5 feature (punti/linee/poligoni).\n"
            "3) Imposta il periodo (mese/anno).\n\n"
            "Vincoli:\n"
            "- È obbligatorio selezionare le feature: il tool NON elabora tutte le feature del layer.\n"
            "- Massimo 5 feature selezionate per evitare troppe richieste.\n"
            "- Se il periodo include molti mesi, il tool effettua più richieste (massimo 50 mesi per richiesta).\n\n"
            "Output:\n"
            "- Per punti: un valore per ogni timestamp (pixel_value).\n"
            "- Per linee/poligoni: statistiche (min/max/mean/...).\n"
        )

    # --------------------------
    # Parameters
    # --------------------------

    def initAlgorithm(self, config=None):

        info_text = (
            "📌 VIIRS query\n"
            "Estrae valori mensili VIIRS (NDVI/EVI/LST) per le feature selezionate.\n\n"
            "✅ Cosa devi fare:\n"
            "• Seleziona 1–5 feature nel layer (punti/linee/poligoni)\n"
            "• Scegli dataset e periodo (mese/anno)\n\n"
            "⚠️ Vincoli:\n"
            f"• Massimo {self.MAX_SELECTED} feature selezionate\n"
            f"• Massimo {self.MAX_TIMESTAMPS_PER_REQUEST} timestamp per singola richiesta (il tool spezza automaticamente)\n"
        )

        self.addParameter(
            QgsProcessingParameterEnum(
                self.P_DATASET,
                "Seleziona dataset VIIRS",
                options=list(self.DATASETS.keys()),
                defaultValue=0,
            )
        )

        self.addParameter(
            QgsProcessingParameterVectorLayer(
                self.P_INPUT_LAYER,
                "Layer geometrie (seleziona 1–5 feature)",
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
                "Campo ID del layer (opzionale, per join)",
                parentLayerParameterName=self.P_INPUT_LAYER,
                optional=True,
            )
        )

        current_year = QDate.currentDate().year()
        years = [str(y) for y in range(self.FIRST_YEAR, current_year + 1)]
        months = [f"{m:02d}" for m in range(1, 13)]

        self.addParameter(QgsProcessingParameterEnum(self.P_START_YEAR, "Anno inizio", years, defaultValue=0))
        self.addParameter(QgsProcessingParameterEnum(self.P_START_MONTH, "Mese inizio", months, defaultValue=0))
        self.addParameter(QgsProcessingParameterEnum(self.P_END_YEAR, "Anno fine", years, defaultValue=len(years) - 1))
        self.addParameter(
            QgsProcessingParameterEnum(
                self.P_END_MONTH,
                "Mese fine",
                months,
                defaultValue=QDate.currentDate().month() - 1
            )
        )

        self.addParameter(
            QgsProcessingParameterFeatureSink(
                self.P_OUTPUT,
                "Output"
            )
        )

    # --------------------------
    # Helpers
    # --------------------------

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
    def _yyyymm_from_iso(iso_str):
        if not iso_str or len(iso_str) < 7:
            return -1
        try:
            return int(iso_str[0:4]) * 100 + int(iso_str[5:7])
        except Exception:
            return -1

    @staticmethod
    def _date_only(iso_str):
        if not iso_str:
            return ""
        s = str(iso_str)
        return s[:10] if len(s) >= 10 else s

    @staticmethod
    def _pixel_value_from_statistics(stats):
        if not stats:
            return None
        hist = stats.get("histogram")
        if isinstance(hist, list) and hist:
            bin_val = hist[0].get("bin")
            if bin_val is not None:
                return bin_val
        return stats.get("mean")

    # --------------------------
    # Main
    # --------------------------

    def processAlgorithm(self, parameters, context, feedback):

        vlayer = self.parameterAsVectorLayer(parameters, self.P_INPUT_LAYER, context)
        if vlayer is None:
            raise QgsProcessingException("Layer non valido")

        selected = list(vlayer.getSelectedFeatures())
        if len(selected) == 0:
            raise QgsProcessingException("Seleziona almeno 1 feature nel layer prima di eseguire il tool.")
        if len(selected) > self.MAX_SELECTED:
            raise QgsProcessingException(
                f"Hai selezionato {len(selected)} feature: oltre il limite massimo di {self.MAX_SELECTED}."
            )

        id_field = (self.parameterAsString(parameters, self.P_ID_FIELD, context) or "").strip()
        is_point_input = (vlayer.geometryType() == QgsWkbTypes.PointGeometry)

        dataset_index = self.parameterAsEnum(parameters, self.P_DATASET, context)
        dataset_name = list(self.DATASETS.keys())[dataset_index]
        dataset_id = self.DATASETS[dataset_name]

        # --- Time filtering
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
            raise QgsProcessingException("Periodo non valido: la data di inizio è successiva alla data di fine.")

        # --- Get timestamps
        url = f"https://api.ellipsis-drive.com/v3/path/{dataset_id}"
        r = requests.get(url, timeout=60)
        if r.status_code != 200:
            raise QgsProcessingException(f"Errore nel recupero dei timestamp (HTTP {r.status_code}).")

        data = r.json()
        timestamps = (data.get("raster") or {}).get("timestamps") or []

        ts_info = {}
        ts_ids = []

        for ts in timestamps:
            ts_id = ts.get("id")
            date_from = str((ts.get("date") or {}).get("from") or "")
            date_to = str((ts.get("date") or {}).get("to") or "")

            key = self._yyyymm_from_iso(date_from)
            if key == -1 or ts_id is None:
                continue

            if start_key <= key <= end_key:
                ts_id = str(ts_id)
                ts_ids.append(ts_id)
                ts_info[ts_id] = {
                    "date_from": self._date_only(date_from),
                    "date_to": self._date_only(date_to),
                    "description": ts.get("description") or ""
                }

        if not ts_ids:
            raise QgsProcessingException("Nessun timestamp disponibile nel periodo selezionato.")

        # --- Output schema
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
            QgsCoordinateReferenceSystem()
        )

        # --- Analyse with batching
        analyse_url = f"https://api.ellipsis-drive.com/v3/path/{dataset_id}/raster/timestamp/analyse"
        batches = list(self._chunk_list(ts_ids, self.MAX_TIMESTAMPS_PER_REQUEST))

        # CRS transform once
        need_transform = (vlayer.crs().authid() != "EPSG:4326")
        if need_transform:
            transform = QgsCoordinateTransform(
                vlayer.crs(),
                QgsCoordinateReferenceSystem("EPSG:4326"),
                QgsProject.instance()
            )
        else:
            transform = None

        for f in selected:

            input_fid = f.id()
            input_id_value = str(f.attribute(id_field)) if id_field else ""

            geom = QgsGeometry(f.geometry())
            if geom.isEmpty():
                continue

            if transform is not None:
                geom.transform(transform)

            geometry_param = geom.asJson()  # GeoJSON geometry

            for batch in batches:

                params = {
                    "timestampIds": json.dumps(batch),
                    "geometry": geometry_param,
                    "returnType": "statistics"
                }

                rr = requests.get(analyse_url, params=params, timeout=180)
                if rr.status_code != 200:
                    raise QgsProcessingException(
                        f"Errore analyse (HTTP {rr.status_code}): {(rr.text or '')[:200]}"
                    )

                analysed = rr.json()

                for item in analysed:

                    ts_id = str((item.get("timestamp") or {}).get("id") or "")
                    has_data = 1 if item.get("hasData") else 0
                    info = ts_info.get(ts_id, {})
                    stats = None

                    for res in item.get("result") or []:
                        if (res.get("band") or {}).get("number") == 1:
                            stats = res.get("statistics")
                            break

                    base_attrs = [
                        input_fid,
                        input_id_value,
                        dataset_name,
                        ts_id,
                        info.get("date_from", ""),
                        info.get("date_to", ""),
                        info.get("description", ""),
                        has_data
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
