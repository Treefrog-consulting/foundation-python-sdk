"""Registry mapping SDK reference-table properties to their server payloads.

Each entry is ``property_name -> (source, payload_key, model_class)``:

* ``source`` picks the payload the rows come from - the Deal, Common or Loss
  ``/api/ref`` response, or the separate Deal ``/api/clients`` response.
* ``payload_key`` is the JSON key inside that payload. Keys are the C# property
  names of the server view models run through Newtonsoft's camel-case strategy,
  so ``UILocations`` serialises as ``uiLocations``.
* ``model_class`` wraps each row. ``None`` means ``BaseModel``, which exposes
  ``id`` and ``name``; the full row is still reachable through ``as_polars()``.

Sources:
    Deal      Deal/Foundation.Deal/Models/ViewModels/Ref/RefViewModel.cs
    Common    Common/Foundation.Common.Data/Models/ViewModels/RefViewModel.cs
    Loss      Loss/Foundation.Loss.ViewModels/ReferenceViewModel.cs
    Identity  Identity/Foundation.Identity/Controllers/Api/{Roles,Users}Controller.cs
"""

from .base import BaseModel
from .broker import Broker
from .broker_office import BrokerOffice
from .category import Category
from .category_detail import CategoryDetail
from .client import Client
from .currency import Currency
from .custom_field import CustomField
from .custom_table_definition import CustomTableDefinition
from .event_set import EventSet
from .external_ref_type import ExternalRefType
from .layer_limit_attach_type import LayerLimitAttachType
from .layer_order_term_type import LayerOrderTermType
from .layer_premium_type import LayerPremiumType
from .layer_status import LayerStatus
from .lob import Lob
from .participant import Participant
from .participant_status import ParticipantStatus
from .peril import Peril
from .perspective import Perspective
from .region import Region
from .reinst_prem_base import ReinstPremBase
from .role import Role
from .unit_of_measure import UnitOfMeasure
from .user import User
from .variant import Variant
from .vendor import Vendor

DEAL = "deal"
COMMON = "common"
LOSS = "loss"
CLIENTS = "clients"
IDENTITY = "identity"

REFERENCE_TABLES: dict[str, tuple[str, str, type[BaseModel] | None]] = {
    # Deal module - /api/ref
    "brokers": (DEAL, "broker", Broker),
    "broker_offices": (DEAL, "brokerOffice", BrokerOffice),
    "categories": (DEAL, "category", Category),
    "category_details": (DEAL, "categoryDetail", CategoryDetail),
    "category_detail_maps": (DEAL, "categoryDetailMap", None),
    "currencies": (DEAL, "currency", Currency),
    "custom_fields": (DEAL, "customField", CustomField),
    "external_ref_types": (DEAL, "externalRefType", ExternalRefType),
    "layer_limit_attach_types": (DEAL, "layerLimitAttachType", LayerLimitAttachType),
    "loss_group_types": (DEAL, "lossGroupType", None),
    "layer_order_term_types": (DEAL, "layerOrderTermType", LayerOrderTermType),
    "layer_premium_types": (DEAL, "layerPremiumType", LayerPremiumType),
    "metrics": (DEAL, "metric", None),
    "participants": (DEAL, "participant", Participant),
    "program_groups": (DEAL, "programGroup", None),
    "participant_statuses": (DEAL, "participantStatus", ParticipantStatus),
    "tokens": (DEAL, "token", None),
    "token_module_maps": (DEAL, "tokenModuleMap", None),
    "token_types": (DEAL, "tokenType", None),
    "units_of_measure": (DEAL, "unitOfMeasure", UnitOfMeasure),
    "custom_field_lookups": (DEAL, "customFieldLookup", None),
    "reinst_prem_bases": (DEAL, "reinstPremBase", ReinstPremBase),
    "custom_table_definitions": (DEAL, "customTableDefinitions", CustomTableDefinition),
    "custom_table_column_definitions": (DEAL, "customTableColumnDefinitions", None),
    "custom_table_definitions_data_sort": (
        DEAL,
        "customTableDefinitionsDataSort",
        None,
    ),
    "pages": (DEAL, "pages", None),
    "components": (DEAL, "components", None),
    "ui_locations": (DEAL, "uiLocations", None),
    "ui_location_options": (DEAL, "uiLocationOptions", None),
    "layer_statuses": (DEAL, "layerStatus", LayerStatus),
    "ref_exceptions": (DEAL, "refExceptions", None),
    "reports": (DEAL, "report", None),
    # Singular: client.reference_runs is the filtered ReferenceRuns collection
    # built from the separate /api/referenceRuns call.
    "reference_run": (DEAL, "referenceRun", None),
    "functions": (DEAL, "functions", None),
    "layout_options": (DEAL, "layoutOptions", None),
    # Distinct from client.sys_config, the Loss module SysConfig alias cache.
    "sys_configs": (DEAL, "sysConfigs", None),
    # Deal module - /api/clients
    "clients": (CLIENTS, "clients", Client),
    # Common module - /api/ref
    "perils": (COMMON, "peril", Peril),
    "regions": (COMMON, "region", Region),
    "lobs": (COMMON, "lob", Lob),
    # Catastrophe models, not the foundation.models package.
    "models": (COMMON, "model", None),
    "model_views": (COMMON, "modelView", None),
    "model_view_details": (COMMON, "modelViewDetail", None),
    "accessible_objects": (COMMON, "accessibleObject", None),
    "ref_grid_displays": (COMMON, "refGridDisplay", None),
    "ref_grid_display_columns": (COMMON, "refGridDisplayColumn", None),
    # Loss module - /api/ref
    "event_sets": (LOSS, "eventSet", EventSet),
    "loss_types": (LOSS, "lossType", None),
    "perspectives": (LOSS, "perspective", Perspective),
    "variants": (LOSS, "variant", Variant),
    "vendors": (LOSS, "vendor", Vendor),
    "upload_statuses": (LOSS, "uploadStatus", None),
    "event_split_views": (LOSS, "eventSplitView", None),
    # Identity module - /api/Roles and /api/Users. Each arrives as a bare list
    # rather than a /api/ref envelope, so the cache wraps it in one.
    "roles": (IDENTITY, "roles", Role),
    "users": (IDENTITY, "users", User),
}
