/**********************************************************************************************
*
*	Vyrobil: Jirka Povolný, 2.2.2024
*
*	Pouští se z fce auto_rel_pub_inc, protože potřebuje tabulku rel_pub.crp_geom_panel
*	naplněnou správnými midy. Naplní tabulku rel_pub.crp_ims_input_inc, která se použije
*	jako vstup automatickému výpočtu CCF na denní bázi.
*
**********************************************************************************************/
DECLARE
  i_cnt integer;				-- Počet záznamů ke zpracování
  
BEGIN
--*****--		
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Function ims_input_inc() is starting.';

  TRUNCATE TABLE rel_pub.crp_ims_input_inc;
	INSERT INTO rel_pub.crp_ims_input_inc
  (
    pid,
    mid,
    facepid,
    face_userid,
    vacid,
    faceid_db,
    err_vac_res
  )
  SELECT 
    a.pid,
    a.mid,
    f.facepid,
    f.face_userid,
    v.vacid,
    f.faceid,
    v.err_vac_res
  FROM 
    pnl.pnl_main a
    JOIN vai.vai_vac_res v ON v.mid = a.mid
    JOIN pnl_facemid f ON v.faceid = f.faceid
  WHERE 
    a.mid IN(SELECT mid FROM rel_pub.crp_geom_panel)
  	AND v.vacid IN(SELECT DISTINCT ON (a.faceid) a.vacid
                    FROM vai_vac_res a JOIN rel_pub.crp_geom_panel b ON a.mid = b.mid
                    --WHERE a.valid = 1
                    ORDER BY a.faceid,
                             a.vacid DESC
                    )
  ;

	SELECT COUNT(*) INTO i_cnt FROM rel_pub.crp_ims_input_inc;
--*****--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Function ims_input_inc(), num of recs: ' || i_cnt ||'.';

  UPDATE rel_pub.crp_ims_input_inc  SET pnlvaienv = 'Roadside' WHERE pnlvaienv = 'EA';
  UPDATE rel_pub.crp_ims_input_inc  SET pnlvaienv = 'Indoor' WHERE pnlvaienv = 'EB';

  -- vysledky z round ------------------------------------------------------------------
  UPDATE rel_pub.crp_ims_input_inc a 
  SET 
    vac_brutto_daily_all = b.vac
  FROM 
    res.res_online_ea_output_round b
  WHERE
    a.vacid = b.vacid 
    AND b.periodid = 2 
    AND b.transptype = 'ALL'
  ;
  UPDATE rel_pub.crp_ims_input_inc a 
    SET 
      vac_brutto_daily_veh = b.vac
  FROM 
    res.res_online_ea_output_round b
  WHERE
    a.vacid = b.vacid 
    AND b.periodid = 2 
    AND b.transptype = 'VEH'
  ;
  UPDATE rel_pub.crp_ims_input_inc a 
  SET 
    vac_brutto_daily_ped = b.vac
  FROM 
    res.res_online_ea_output_round b
  WHERE
    a.vacid = b.vacid
    AND b.periodid = 2 
    AND b.transptype = 'PED'
  ;
  UPDATE rel_pub.crp_ims_input_inc a 
  SET 
    rots_brutto_daily_all = b.rots
  FROM 
    res.res_online_ea_output_round b
  WHERE
    a.vacid = b.vacid 
    AND b.periodid = 2 
    AND b.transptype = 'ALL'
  ;
  UPDATE rel_pub.crp_ims_input_inc a 
  SET 
    rots_brutto_daily_veh = b.rots
  FROM 
    res.res_online_ea_output_round b
  WHERE	
    a.vacid = b.vacid 
    AND b.periodid = 2 
    AND b.transptype='VEH'
  ;
  UPDATE rel_pub.crp_ims_input_inc a 
  SET 
    rots_brutto_daily_ped = b.rots
  FROM 
    res.res_online_ea_output_round b
  WHERE
    a.vacid = b.vacid 
    AND b.periodid = 2 
    AND b.transptype = 'PED'
  ;
     
  UPDATE rel_pub.crp_ims_input_inc  
  SET 
    va_brutto_daily_all = vac_brutto_daily_all:: DOUBLE PRECISION/rots_brutto_daily_all:: DOUBLE PRECISION
  WHERE 
    vac_brutto_daily_all <> 0
    AND rots_brutto_daily_all <> 0
  ;
  UPDATE rel_pub.crp_ims_input_inc 
  SET 
    va_brutto_daily_veh = vac_brutto_daily_veh:: DOUBLE PRECISION/rots_brutto_daily_veh:: DOUBLE PRECISION
  WHERE 
    rots_brutto_daily_veh <> 0
    AND rots_brutto_daily_veh <> 0
  ;
  UPDATE rel_pub.crp_ims_input_inc  
  SET 
    va_brutto_daily_ped = vac_brutto_daily_ped :: DOUBLE PRECISION/rots_brutto_daily_ped :: DOUBLE PRECISION
  WHERE 
    rots_brutto_daily_ped <> 0
    AND rots_brutto_daily_ped <> 0
  ;
  
	SELECT COUNT(*) INTO i_cnt FROM rel_pub.crp_ims_input_inc;
--*****--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Function ims_input_inc(), num of recs on the end: ' || i_cnt ||'.';

--*****--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Function ims_input_inc() end.';

  PERFORM rel_pub.log_msg('INFO', 'fce:create_ims_input_inc', 'The function completed successfully.');
  
EXCEPTION
WHEN others THEN
  RAISE NOTICE 'exception: %', SQLERRM;
	PERFORM rel_pub.log_msg('ERROR', 'create_ims_input_inc', 'Error occured: ' || SQLERRM);

END;