################################################################################
#
# Liste des tags utiliés pour reconstituer les pistes cyclables
#
################################################################################

c(
  # 0. Corrections / ajouts 
  "bicycle", "segregated",
  
  # 1. Caractéristiques générales
  "highway", "construction", "junction", "tracktype", "service",
  "footway", "path", "steps", "living_street", "pedestrian",
  "residential", "unclassified",
  # "primary", "secondary", "tertiary",
  
  # 2. Aménagements cyclables
  "cycleway", "cycleway.left", "cycleway.right", "cycleway.both",
  "cycleway.width", "cycleway.est_width",
  "cycleway.left.width", "cycleway.right.width", "cycleway.both.width",
  "cycleway.left.est_width", "cycleway.right.est_width", "cycleway.both.est_width",
  "cycleway.left.oneway", "cycleway.right.oneway",
  "cycleway.left.surface", "cycleway.right.surface", "cycleway.surface",
  "cycleway.left.smoothness", "cycleway.right.smoothness", "cycleway.smoothness",
  "cycleway.left.segregated", "cycleway.right.segregated",
  "cycleway.both.segregated", "cycleway.segregated",
  "ramp.bicycle", "oneway.bicycle",
  
  # 3. Aménagements piétons
  "sidewalk.bicycle", "sidewalk.left.segregated", "sidewalk.segregated",
  "sidewalk.left", "sidewalk.right",
  
  # 4. Circulation et accès
  "access", "motor_vehicle", "motorcar", "psv", "bus",
  "oneway", "lanes",
  
  # 5. Revêtement et qualité
  "surface", "surface.left", "surface.right",
  "smoothness", "smoothness.left", "smoothness.right",
  
  # 6. Signalisation et réglementation
  "traffic_sign", "designation", "maxspeed", "zone.maxspeed",
  "source.maxspeed", "cyclestreet",
  
  # 7. Métadonnées
  "ref", "description", "note", "fixme",
  "source", "source.geometry", "start_date",
  
  # Jamais trouvé :
  # "osm_timestamp", 
  
  # 8. Relations d’itinéraires
  "route"
  
  # Jamais trouvé :
  #, "route_icn_ref", "route_ncn_ref", "route_rcn_ref", "route_lcn_ref"
  
  # Ajouter ?
  # ,"icn", "ncn", "rcn", "icn_ref", "ncn_ref", "rcn_ref"
  )
