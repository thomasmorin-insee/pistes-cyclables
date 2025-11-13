SELECT
	CASE
		WHEN cyclestreet = 'yes' THEN 'VELO RUE'

		WHEN ramp_bicycle = 'yes' THEN 'RAMPE'

		WHEN highway IS NOT NULL AND highway != 'cycleway' AND cycleway = 'lane' AND lanes = '1' AND (oneway IS NULL OR oneway = 'no') THEN 'CHAUSSEE A VOIE CENTRALE BANALISEE'

		WHEN traffic_sign = 'FR:C115' OR ((bicycle = 'designated' OR designation = 'greenway') AND ((highway = 'path' AND (motor_vehicle IS NULL OR motor_vehicle = 'no' OR motorcar IS NULL OR motorcar = 'no')) OR highway = 'footway' OR (highway = 'track' AND tracktype = 'grade1'))) THEN 'VOIE VERTE'

		WHEN bicycle = 'yes' AND ((highway = 'path' AND (motor_vehicle IS NULL OR motor_vehicle = 'no' OR motorcar IS NULL OR motorcar = 'no')) OR highway = 'footway') THEN 'AMENAGEMENT MIXTE PIETON VELO HORS VOIE VERTE'

		WHEN cycleway = 'share_busway' OR cycleway_right = 'share_busway' OR cycleway_both = 'share_busway' OR (highway='service' AND access = 'no' AND (psv IN ('yes', 'designated') OR bus IN ('yes', 'designated')) AND bicycle IN ('yes', 'designated')) THEN 'COULOIR BUS+VELO'

		WHEN highway = 'cycleway' OR cycleway = 'track' OR cycleway_right = 'track' OR cycleway_both = 'track' THEN 'PISTE CYCLABLE'

		WHEN cycleway = 'lane' OR cycleway_right = 'lane' OR cycleway_both = 'lane' THEN 'BANDE CYCLABLE'

		WHEN cycleway IS NOT NULL OR cycleway_right IS NOT NULL OR cycleway_both IS NOT NULL OR highway = 'cycleway' THEN 'AUTRE'

		ELSE 'AUCUN'
	END AS ame_d,
	-- CASE
		-- WHEN maxspeed = '30' AND (zone_maxspeed = 'FR:30' OR source_maxspeed = 'FR:zone30') THEN 'ZONE 30'
		-- WHEN highway IN ('footway', 'pedestrian') THEN 'AIRE PIETONNE'
		-- WHEN highway = 'living_street' OR living_street = 'yes' THEN 'ZONE DE RENCONTRE'
		-- WHEN maxspeed ~ '^\d+$' AND (maxspeed)::int <= 50 THEN 'EN AGGLOMERATION'
		-- WHEN maxspeed ~ '^\d+$' AND (maxspeed)::int > 50 THEN 'HORS AGGLOMERATION'
		-- ELSE ''
	-- END AS regime_d,
	CASE
		WHEN
			(highway = 'cycleway' AND (oneway IS NULL OR oneway = 'no'))
			OR (cycleway_right IN ('lane', 'track') AND cycleway_right_oneway = 'no')
			OR (highway = 'footway' AND bicycle IN ('yes', 'designated') AND (oneway IS NULL OR oneway = 'no'))
			OR ramp_bicycle = 'yes'
			OR (((highway = 'path' AND (motor_vehicle IS NULL OR motor_vehicle = 'no' OR motorcar IS NULL OR motorcar = 'no')) OR highway='footway') AND bicycle IN ('yes', 'designated') AND (oneway IS NULL OR oneway = 'no'))
		THEN 'BIDIRECTIONNEL'
		WHEN
			cyclestreet = 'yes'
			OR ramp_bicycle = 'yes'
			OR (cycleway IS NOT NULL AND cycleway != 'no')
			OR (cycleway_both IS NOT NULL AND cycleway_both != 'no')
			OR (cycleway_right IS NOT NULL AND cycleway_right != 'no')
			OR bicycle IN ('yes', 'designated')
			OR highway = 'cycleway'
			OR sidewalk_bicycle = 'yes'
		THEN 'UNIDIRECTIONNEL'
		ELSE ''
	END AS sens_d,
	COALESCE(cycleway_right_width, cycleway_right_est_width, cycleway_both_width, cycleway_both_est_width, cycleway_width, cycleway_est_width, '') AS largeur_d,
	CASE
		WHEN
			highway IN ('footway', 'cycleway')
			OR sidewalk_bicycle = 'yes'
			OR ramp_bicycle = 'yes'
			OR (cycleway = 'track' OR cycleway_both = 'track' OR cycleway_right = 'track')
		THEN 'TROTTOIR'
		WHEN
			cyclestreet = 'yes'
			OR (cycleway IS NOT NULL AND cycleway != 'no')
			OR (cycleway_both IS NOT NULL AND cycleway_both != 'no')
			OR (cycleway_right IS NOT NULL AND cycleway_right != 'no')
			OR bicycle IN ('yes', 'designated')
		THEN 'CHAUSSEE'
		ELSE ''
	END AS local_d,
	CASE
		WHEN highway='construction' OR construction IS NOT NULL THEN 'EN TRAVAUX'
		ELSE 'EN SERVICE'
	END AS statut_d,
	CASE
		WHEN
			COALESCE(cycleway_right_surface, cycleway_surface, surface_right, surface) IN ('compacted', 'fine_gravel', 'grass_paver', 'chipseal', 'sett', 'cobblestone', 'unhewn_cobblestone')
			AND COALESCE(cycleway_right_smoothness, cycleway_smoothness, smoothness_right, smoothness) IN ('excellent', 'good')
			THEN 'RUGUEUX'
		WHEN
			COALESCE(cycleway_right_surface, cycleway_surface, surface_right, surface) IN ('asphalt', 'concrete', 'paved', 'concrete_lanes', 'concrete_plates', 'paving_stones', 'metal', 'wood')
			AND COALESCE(cycleway_right_smoothness, cycleway_smoothness, smoothness_right, smoothness) = 'bad'
			THEN 'RUGUEUX'
		WHEN
			COALESCE(cycleway_right_surface, cycleway_surface, surface_right, surface) IN ('compacted', 'fine_gravel', 'grass_paver', 'chipseal', 'sett', 'cobblestone', 'unhewn_cobblestone')
			AND COALESCE(cycleway_right_smoothness, cycleway_smoothness, smoothness_right, smoothness) = 'intermediate'
			THEN 'MEUBLE'
		WHEN
			COALESCE(cycleway_right_surface, cycleway_surface, surface_right, surface) IN ('unpaved', 'gravel', 'rock', 'pebblestone', 'ground', 'dirt', 'earth', 'grass', 'mud', 'sand', 'woodchips')
			AND COALESCE(cycleway_right_smoothness, cycleway_smoothness, smoothness_right, smoothness) IN ('excellent', 'good', 'intermediate')
			THEN 'MEUBLE'

		WHEN
			COALESCE(cycleway_right_smoothness, cycleway_smoothness, smoothness_right, smoothness) IN ('excellent', 'good')
			THEN 'LISSE'
		WHEN
			COALESCE(cycleway_right_smoothness, cycleway_smoothness, smoothness_right, smoothness) = 'intermediate'
			THEN 'RUGUEUX'
		WHEN
			COALESCE(cycleway_right_smoothness, cycleway_smoothness, smoothness_right, smoothness) IN ('bad', 'very_bad', 'horrible', 'very_horrible', 'impassable')
			THEN 'MEUBLE'

		WHEN
			COALESCE(cycleway_right_surface, cycleway_surface, surface_right, surface) IN ('asphalt', 'concrete', 'paved', 'concrete_lanes', 'concrete_plates', 'paving_stones', 'metal', 'wood')
			THEN 'LISSE'
		WHEN
			COALESCE(cycleway_right_surface, cycleway_surface, surface_right, surface) IN ('compacted', 'fine_gravel', 'grass_paver', 'chipseal', 'sett', 'cobblestone', 'unhewn_cobblestone')
			THEN 'RUGUEUX'
		WHEN
			COALESCE(cycleway_right_surface, cycleway_surface, surface_right, surface) IN ('unpaved', 'gravel', 'rock', 'pebblestone', 'ground', 'dirt', 'earth', 'grass', 'mud', 'sand', 'woodchips')
			THEN 'MEUBLE'

		WHEN
			COALESCE(cycleway_right_surface, cycleway_surface, surface_right, surface) IS NULL
			AND COALESCE(cycleway_right_smoothness, cycleway_smoothness, smoothness_right, smoothness) IN ('excellent', 'good')
			THEN 'LISSE'
		WHEN
			COALESCE(cycleway_right_surface, cycleway_surface, surface_right, surface) IS NULL
			AND COALESCE(cycleway_right_smoothness, cycleway_smoothness, smoothness_right, smoothness) = 'intermediate'
			THEN 'RUGUEUX'
		WHEN
			COALESCE(cycleway_right_surface, cycleway_surface, surface_right, surface) IS NULL
			AND COALESCE(cycleway_right_smoothness, cycleway_smoothness, smoothness_right, smoothness) IN ('bad', 'very_bad', 'horrible', 'very_horrible', 'impassable')
			THEN 'MEUBLE'

		WHEN
			COALESCE(cycleway_right_smoothness, cycleway_smoothness, smoothness_right, smoothness) IS NULL
			AND COALESCE(cycleway_right_surface, cycleway_surface, surface_right, surface) IN ('asphalt', 'concrete', 'paved', 'concrete_lanes', 'concrete_plates', 'paving_stones', 'metal', 'wood')
			THEN 'LISSE'
		ELSE ''
	END AS revet_d,
	-- <ADM8REF> AS code_com_g,
	CASE
		WHEN cyclestreet = 'yes' AND (oneway IS NULL OR oneway = 'no') THEN 'VELO RUE'

		WHEN highway IS NOT NULL AND highway != 'cycleway' AND cycleway = 'lane' AND lanes = '1' AND (oneway IS NULL OR oneway = 'no') THEN 'CHAUSSEE A VOIE CENTRALE BANALISEE'

		WHEN ((oneway IS NULL OR oneway = 'no') AND (junction IS NULL OR junction != 'roundabout') AND cycleway = 'share_busway') OR cycleway_left = 'share_busway' OR cycleway_both = 'share_busway' OR (highway='service' AND access = 'no' AND (psv IN ('yes', 'designated') OR bus IN ('yes', 'designated')) AND bicycle IN ('yes', 'designated')) AND (oneway IS NULL OR oneway = 'no') THEN 'COULOIR BUS+VELO'

		WHEN highway IS NOT NULL AND highway != 'cycleway' AND oneway = 'yes' AND cycleway_left = 'track' THEN 'DOUBLE SENS CYCLABLE PISTE'

		WHEN highway IS NOT NULL AND highway != 'cycleway' AND oneway = 'yes' AND (cycleway_left = 'opposite_lane' OR cycleway = 'opposite_lane') THEN 'DOUBLE SENS CYCLABLE BANDE'

		WHEN highway IS NOT NULL AND highway != 'cycleway' AND oneway = 'yes' AND oneway_bicycle = 'no' THEN 'DOUBLE SENS CYCLABLE NON MATERIALISE'

		WHEN COALESCE(cycleway_left_segregated, cycleway_both_segregated, sidewalk_left_segregated) = 'no' OR (highway NOT IN ('footway', 'cycleway', 'path') AND COALESCE(cycleway_segregated, sidewalk_segregated, segregated) = 'no') THEN 'AMENAGEMENT MIXTE PIETON VELO HORS VOIE VERTE'

		WHEN ((oneway IS NULL OR oneway = 'no') AND (junction IS NULL OR junction != 'roundabout') AND cycleway = 'track') OR cycleway_left = 'track' OR cycleway_both = 'track' THEN 'PISTE CYCLABLE'

		WHEN ((oneway IS NULL OR oneway = 'no') AND (junction IS NULL OR junction != 'roundabout') AND cycleway = 'lane') OR cycleway_left = 'lane' OR cycleway_both = 'lane' THEN 'BANDE CYCLABLE'

		WHEN ((oneway IS NULL OR oneway = 'no') AND (junction IS NULL OR junction != 'roundabout') AND cycleway IS NOT NULL) OR cycleway_left IS NOT NULL OR cycleway_both IS NOT NULL THEN 'AUTRE'

		ELSE 'AUCUN'
	END AS ame_g,
	-- CASE
		-- WHEN maxspeed = '30' AND (zone_maxspeed = 'FR:30' OR source_maxspeed = 'FR:zone30') THEN 'ZONE 30'
		-- WHEN highway = 'living_street' OR living_street = 'yes' THEN 'ZONE DE RENCONTRE'
		-- WHEN maxspeed ~ '^\d+$' AND (maxspeed)::int <= 50 THEN 'EN AGGLOMERATION'
		-- WHEN maxspeed ~ '^\d+$' AND (maxspeed)::int > 50 THEN 'HORS AGGLOMERATION'
		-- ELSE ''
	-- END AS regime_g,
	CASE
		WHEN
			cycleway_left_oneway = 'no'
		THEN 'BIDIRECTIONNEL'
		WHEN
			(cyclestreet = 'yes' AND (oneway IS NULL OR oneway = 'no'))
			OR (highway IS NOT NULL AND highway != 'cycleway' AND cycleway = 'lane' AND lanes = '1' AND (oneway IS NULL OR oneway = 'no'))
			OR (((oneway IS NULL OR oneway = 'no') AND (junction IS NULL OR junction != 'roundabout') AND cycleway = 'share_busway') OR cycleway_left = 'share_busway' OR cycleway_both = 'share_busway' OR (highway='service' AND access = 'no' AND (psv IN ('yes', 'designated') OR bus IN ('yes', 'designated')) AND bicycle IN ('yes', 'designated')) AND (oneway IS NULL OR oneway = 'no'))
			OR (highway IS NOT NULL AND highway != 'cycleway' AND oneway = 'yes' AND cycleway_left = 'track')
			OR (highway IS NOT NULL AND highway != 'cycleway' AND oneway = 'yes' AND (cycleway_left = 'opposite_lane' OR cycleway = 'opposite_lane'))
			OR oneway_bicycle = 'no'
			OR sidewalk_bicycle = 'yes'
			OR ((oneway IS NULL OR oneway = 'no') AND (junction IS NULL OR junction != 'roundabout') AND cycleway IS NOT NULL) OR cycleway_left IS NOT NULL OR cycleway_both IS NOT NULL
		THEN 'UNIDIRECTIONNEL'
		ELSE ''
	END AS sens_g,
	COALESCE(cycleway_left_width, cycleway_left_est_width, cycleway_both_width, cycleway_both_est_width, cycleway_width, cycleway_est_width, '') AS largeur_g,
	CASE
		WHEN
			(highway IS NOT NULL AND highway != 'cycleway' AND oneway = 'yes' AND cycleway_left = 'track')
			OR (COALESCE(cycleway_left_segregated, cycleway_both_segregated, sidewalk_left_segregated) = 'no' OR (highway NOT IN ('footway', 'cycleway', 'path') AND COALESCE(cycleway_segregated, sidewalk_segregated, segregated) = 'no'))
			OR (cycleway = 'track' OR cycleway_left = 'track' OR cycleway_both = 'track')
		THEN 'TROTTOIR'
		WHEN
			(cyclestreet = 'yes' AND (oneway IS NULL OR oneway = 'no'))
			OR (highway IS NOT NULL AND highway != 'cycleway' AND cycleway = 'lane' AND lanes = '1' AND (oneway IS NULL OR oneway = 'no'))
			OR (highway IS NOT NULL AND highway != 'cycleway' AND oneway = 'yes' AND (cycleway_left = 'opposite_lane' OR cycleway = 'opposite_lane'))
			OR (highway IS NOT NULL AND highway != 'cycleway' AND oneway = 'yes' AND oneway_bicycle = 'no')
			OR (cycleway = 'lane' OR cycleway_left = 'lane' OR cycleway_both = 'lane')
			OR (cycleway IS NOT NULL OR cycleway_left IS NOT NULL OR cycleway_both IS NOT NULL)
		THEN 'CHAUSSEE'
		ELSE ''
	END AS local_g,
	CASE
		WHEN highway='construction' OR construction IS NOT NULL THEN 'EN TRAVAUX'
		ELSE 'EN SERVICE'
	END AS statut_g,
	CASE
		WHEN
			COALESCE(cycleway_left_surface, cycleway_surface, surface_left, surface) IN ('compacted', 'fine_gravel', 'grass_paver', 'chipseal', 'sett', 'cobblestone', 'unhewn_cobblestone')
			AND COALESCE(cycleway_left_smoothness, cycleway_smoothness, smoothness_left, smoothness) IN ('excellent', 'good')
			THEN 'RUGUEUX'
		WHEN
			COALESCE(cycleway_left_surface, cycleway_surface, surface_left, surface) IN ('asphalt', 'concrete', 'paved', 'concrete_lanes', 'concrete_plates', 'paving_stones', 'metal', 'wood')
			AND COALESCE(cycleway_left_smoothness, cycleway_smoothness, smoothness_left, smoothness) = 'bad'
			THEN 'RUGUEUX'
		WHEN
			COALESCE(cycleway_left_surface, cycleway_surface, surface_left, surface) IN ('compacted', 'fine_gravel', 'grass_paver', 'chipseal', 'sett', 'cobblestone', 'unhewn_cobblestone')
			AND COALESCE(cycleway_left_smoothness, cycleway_smoothness, smoothness_left, smoothness) = 'intermediate'
			THEN 'MEUBLE'
		WHEN
			COALESCE(cycleway_left_surface, cycleway_surface, surface_left, surface) IN ('unpaved', 'gravel', 'rock', 'pebblestone', 'ground', 'dirt', 'earth', 'grass', 'mud', 'sand', 'woodchips')
			AND COALESCE(cycleway_left_smoothness, cycleway_smoothness, smoothness_left, smoothness) IN ('excellent', 'good', 'intermediate')
			THEN 'MEUBLE'

		WHEN
			COALESCE(cycleway_left_smoothness, cycleway_smoothness, smoothness_left, smoothness) IN ('excellent', 'good')
			THEN 'LISSE'
		WHEN
			COALESCE(cycleway_left_smoothness, cycleway_smoothness, smoothness_left, smoothness) = 'intermediate'
			THEN 'RUGUEUX'
		WHEN
			COALESCE(cycleway_left_smoothness, cycleway_smoothness, smoothness_left, smoothness) IN ('bad', 'very_bad', 'horrible', 'very_horrible', 'impassable')
			THEN 'MEUBLE'

		WHEN
			COALESCE(cycleway_left_surface, cycleway_surface, surface_left, surface) IN ('asphalt', 'concrete', 'paved', 'concrete_lanes', 'concrete_plates', 'paving_stones', 'metal', 'wood')
			THEN 'LISSE'
		WHEN
			COALESCE(cycleway_left_surface, cycleway_surface, surface_left, surface) IN ('compacted', 'fine_gravel', 'grass_paver', 'chipseal', 'sett', 'cobblestone', 'unhewn_cobblestone')
			THEN 'RUGUEUX'
		WHEN
			COALESCE(cycleway_left_surface, cycleway_surface, surface_left, surface) IN ('unpaved', 'gravel', 'rock', 'pebblestone', 'ground', 'dirt', 'earth', 'grass', 'mud', 'sand', 'woodchips')
			THEN 'MEUBLE'

		WHEN
			COALESCE(cycleway_left_surface, cycleway_surface, surface_left, surface) IS NULL
			AND COALESCE(cycleway_left_smoothness, cycleway_smoothness, smoothness_left, smoothness) IN ('excellent', 'good')
			THEN 'LISSE'
		WHEN
			COALESCE(cycleway_left_surface, cycleway_surface, surface_left, surface) IS NULL
			AND COALESCE(cycleway_left_smoothness, cycleway_smoothness, smoothness_left, smoothness) = 'intermediate'
			THEN 'RUGUEUX'
		WHEN
			COALESCE(cycleway_left_surface, cycleway_surface, surface_left, surface) IS NULL
			AND COALESCE(cycleway_left_smoothness, cycleway_smoothness, smoothness_left, smoothness) IN ('bad', 'very_bad', 'horrible', 'very_horrible', 'impassable')
			THEN 'MEUBLE'

		WHEN
			COALESCE(cycleway_left_smoothness, cycleway_smoothness, smoothness_left, smoothness) IS NULL
			AND COALESCE(cycleway_left_surface, cycleway_surface, surface_left, surface) IN ('asphalt', 'concrete', 'paved', 'concrete_lanes', 'concrete_plates', 'paving_stones', 'metal', 'wood')
		THEN 'LISSE'
		ELSE ''
	END AS revet_g,
	CASE
		WHEN smoothness IN ('excellent', 'good') THEN 'ROLLER'
		WHEN smoothness = 'intermediate' THEN 'VELO DE ROUTE'
		WHEN smoothness = 'bad' THEN 'VTC'
		WHEN smoothness IN ('very_bad', 'horrible', 'very_horrible') THEN 'VTT'
		ELSE ''
	END access_ame,
	-- substring(osm_timestamp, 1, 10) AS date_maj,
	-- COALESCE(maxspeed, '') AS trafic_vit,
	-- CASE
		-- WHEN lit = 'yes' THEN 'true'
		-- WHEN lit = 'no' THEN 'false'
		-- ELSE ''
	-- END AS lumiere,
	-- CASE
		-- WHEN start_date IS NOT NULL THEN substring(start_date, 1, 4)
		-- ELSE ''
	-- END AS d_service
	-- CASE
		-- WHEN 
			-- (cycleway IS NOT NULL AND cycleway != 'no')
			-- OR highway = 'cycleway'
			-- OR (cycleway_right IS NOT NULL AND cycleway_right != 'no')
			-- OR (cycleway_left IS NOT NULL AND cycleway_left != 'no')
			-- OR (cycleway_both IS NOT NULL AND cycleway_both != 'no')
			-- OR (highway IN ('path', 'footway') AND bicycle IN ('yes', 'designated'))
			-- OR (highway = 'service' AND bicycle IN ('yes', 'designated') AND access = 'no' AND (psv IN ('yes', 'designated') OR bus IN ('yes', 'designated')))
			-- OR (highway IN ('pedestrian', 'unclassified', 'residential', 'service', 'primary', 'secondary', 'tertiary') AND cyclestreet = 'yes')
			-- OR (highway = 'steps' AND ramp_bicycle = 'yes')
		-- THEN 'yes'
		-- ELSE 'false'
	-- END filtre_ac,
	CASE
		WHEN 
			(cycleway IS NOT NULL AND cycleway != 'no')
			OR highway = 'cycleway'
			OR (cycleway_right IS NOT NULL AND cycleway_right != 'no')
			OR (cycleway_left IS NOT NULL AND cycleway_left != 'no')
			OR (cycleway_both IS NOT NULL AND cycleway_both != 'no')
			OR (highway IN ('path', 'footway') AND bicycle IN ('yes', 'designated'))
			OR (highway = 'service' AND bicycle IN ('yes', 'designated') AND access = 'no' AND (psv IN ('yes', 'designated') OR bus IN ('yes', 'designated')))
			OR (highway IN ('pedestrian', 'unclassified', 'residential', 'service', 'primary', 'secondary', 'tertiary') AND cyclestreet = 'yes')
			OR (highway = 'steps' AND ramp_bicycle = 'yes')
		THEN 'yes'
		ELSE 'false'
	END filtre_ac,