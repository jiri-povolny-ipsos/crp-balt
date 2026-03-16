/**********************************************************************************************
*
*	Vyrobil: Jirka Povolný, 2.2.2024
*
*	Pouští se z fce auto_rel_pub, protože potřebuje tabulku rel_pub.crp_all_panel
*	naplněnou správnými midy. Naplní tabulku rel_pub.crp_ims_input
*
**********************************************************************************************/
DECLARE
  i_cnt integer;				-- Počet záznamů ke zpracování
  
BEGIN

--*****--		
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Function ims_input_ccf() is starting.';

  TRUNCATE TABLE rel_pub.crp_ims_input;
  INSERT INTO rel_pub.crp_ims_input
  (
    pid,
    mid,
    facepid,
    face_userid,
    vacid,
    faceid_db,
    err_vac_res,
    pnlvaienv
  )
  SELECT 
      a.pid,
      a.mid,
      f.facepid,
      f.face_userid,
      v.vacid,
      f.faceid,
      v.err_vac_res,
      s.pnlvaienv
    FROM 
      pnl.pnl_main a
      JOIN vai.vai_vac_res v ON v.mid = a.mid
      JOIN pnl.pnl_facemid f ON v.faceid = f.faceid
      JOIN pnl.pnl_faces f2 ON f.facepid = f2.facepid
      JOIN code.code_pnlsubtype s ON a.pnlsubtype = s.pnlsubtype
    WHERE 
      a.mid IN(SELECT mid FROM rel_pub.crp_all_panel)
      AND(a.pnlmotion <> 'D' OR (a.pnlmotion = 'D' AND f2.pos = 1))
      AND v.vacid IN(SELECT DISTINCT ON (a.faceid) a.vacid
                      FROM vai_vac_res a JOIN rel_pub.crp_all_panel b ON a.mid = b.mid
                      --WHERE a.valid = 1
                      ORDER BY a.faceid,
                               a.vacid DESC
                      )
  ;
  
  
	SELECT COUNT(*) INTO i_cnt FROM rel_pub.crp_ims_input;
--*****--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Function ims_input_ccf(), num of recs: ' || i_cnt ||'.';

  UPDATE rel_pub.crp_ims_input  SET pnlvaienv = 'Roadside' WHERE pnlvaienv = 'EA';
  UPDATE rel_pub.crp_ims_input  SET pnlvaienv = 'Indoor' WHERE pnlvaienv = 'EB';

  -- vysledky z round ------------------------------------------------------------------
  -- VAC
  UPDATE rel_pub.crp_ims_input a 
  SET 
  	vac_week_all = b.vac
	FROM 
  	res.res_online_ea_output_round b
	WHERE
    a.vacid = b.vacid AND
    b.periodid = 1 AND
    b.transptype = 'ALL'
  ;
	UPDATE rel_pub.crp_ims_input a 
  SET 
  	vac_week_veh = b.vac
	FROM 
  	res.res_online_ea_output_round b
	WHERE
    a.vacid = b.vacid AND
    b.periodid = 1 AND
    b.transptype = 'VEH'
  ;
	UPDATE rel_pub.crp_ims_input a 
  SET 
  	vac_week_pub = b.vac
	FROM 
  	res.res_online_ea_output_round b
	WHERE
    a.vacid = b.vacid AND
    b.periodid = 1 AND
    b.transptype = 'PUB'
  ;
	UPDATE rel_pub.crp_ims_input a 
  SET 
  	vac_week_ped = b.vac
	FROM 
  	res.res_online_ea_output_round b
	WHERE
    a.vacid = b.vacid and
    b.periodid = 1 and
    b.transptype = 'PED'
  ;
	UPDATE rel_pub.crp_ims_input a 
  SET 
  	vac_week_bic = b.vac
	FROM 
  	res.res_online_ea_output_round b
	WHERE
    a.vacid = b.vacid and
    b.periodid = 1 and
    b.transptype = 'BIC'
  ;
		
  ----------------------------------------------
  -- ROTS
  UPDATE rel_pub.crp_ims_input a 
  SET 
    rots_week_all = b.rots
  FROM 
    res.res_online_ea_output_round b
  WHERE
    a.vacid = b.vacid 
    AND b.periodid = 1 
    AND b.transptype = 'ALL'
  ;
  UPDATE rel_pub.crp_ims_input a 
  SET 
    rots_week_veh = b.rots
  FROM 
    res.res_online_ea_output_round b
  WHERE	
    a.vacid = b.vacid 
    AND b.periodid = 1 
    AND b.transptype = 'VEH'
  ;
  UPDATE rel_pub.crp_ims_input a 
  SET 
    rots_week_pub = b.rots
  FROM 
    res.res_online_ea_output_round b
  WHERE
    a.vacid = b.vacid 
    AND b.periodid = 1 
    AND b.transptype = 'PUB'
  ;UPDATE rel_pub.crp_ims_input a 
  SET 
    rots_week_ped = b.rots
  FROM 
    res.res_online_ea_output_round b
  WHERE
    a.vacid = b.vacid 
    AND b.periodid = 1 
    AND b.transptype = 'PED'
  ;UPDATE rel_pub.crp_ims_input a 
  SET 
    rots_week_bic = b.rots
  FROM 
    res.res_online_ea_output_round b
  WHERE
    a.vacid = b.vacid 
    AND b.periodid = 1 
    AND b.transptype = 'BIC'
  ;
     
  ----------------------------------------------
  -- VA
  UPDATE rel_pub.crp_ims_input  
  SET 
    va_week_all = vac_week_all:: DOUBLE PRECISION/rots_week_all:: DOUBLE PRECISION
  WHERE 
    vac_week_all <> 0
    AND rots_week_all <> 0
  ;
  UPDATE rel_pub.crp_ims_input 
  SET 
    va_week_veh = vac_week_veh:: DOUBLE PRECISION/rots_week_veh:: DOUBLE PRECISION
  WHERE 
    rots_week_veh <> 0
    AND rots_week_veh <> 0
  ;
  UPDATE rel_pub.crp_ims_input  
  SET 
    va_week_pub = vac_week_pub :: DOUBLE PRECISION/rots_week_pub :: DOUBLE PRECISION
  WHERE 
    rots_week_pub <> 0
    AND rots_week_pub <> 0
  ;
	UPDATE rel_pub.crp_ims_input  
  SET 
    va_week_ped = vac_week_ped :: DOUBLE PRECISION/rots_week_ped :: DOUBLE PRECISION
  WHERE 
    rots_week_ped <> 0
    AND rots_week_ped <> 0
  ;
	UPDATE rel_pub.crp_ims_input  
  SET 
    va_week_bic = vac_week_bic :: DOUBLE PRECISION/rots_week_bic :: DOUBLE PRECISION
  WHERE 
    rots_week_bic <> 0
    AND rots_week_bic <> 0
  ;
	
	SELECT COUNT(*) INTO i_cnt FROM rel_pub.crp_ims_input;
--*****--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Function ims_input_ccf(), num of recs on the end: ' || i_cnt ||'.';

--*****--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Function ims_input_ccf() end.';
	
	PERFORM rel_pub.log_msg('INFO', 'fce:create_ims_input_ccf', 'The function completed successfully.');
  
EXCEPTION
WHEN others THEN
  RAISE NOTICE 'exception: %', SQLERRM;
  PERFORM rel_pub.log_msg('ERROR', 'create_ims_input_ccf', 'Error occured: ' || SQLERRM);

END;