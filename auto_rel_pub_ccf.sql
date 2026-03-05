/**********************************************************************************************
*
*	Vyrobil: Jirka Povolný, 2.2.2024
*
*	Používá se k výpočtu CCF.
*
*	
*
*
**********************************************************************************************/
DECLARE
  exe_str varchar;			-- String pro poskládání SQL příkazů
  last_load_id integer;	-- Id posledního loadu do etl tabulky pro IDS
  i_cnt integer;				-- Počet záznamů ke zpracování
  navteqver_str varchar;-- Poslední navteqver z params()
  debug_mode boolean;		-- Řídí debug mód
  
BEGIN
-------------------------------------------------------------------------------------------------------------------
--** Tady je NOTICE **--
RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Function auto_rel_pub_ccf start.';

-- Init proměnných
debug_mode = true;

--** Tady je NOTICE **--
RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Debug mode: ' || debug_mode;

-- Načtení aktuální navteqver pro kontrolu správného navteqver u panelů
SELECT navteq_version INTO navteqver_str FROM fnc.f_params();
--SELECT navteq_version FROM fnc.f_params();--25q1

--** Tady je NOTICE **--
RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Navteq version: ' || navteqver_str;

-- INSERT všech panelů
-- Rovněž se přidají panely z tabulky crp_missing, kam se dají přidávat panely "z venku", ručně.
-- Tabulka crp_all_panel obsahuje seznam panelů s jejich geometrií (tabulka s panely a jejich kolaci, vzdalenost maxdist + 200m)

--** Tady je NOTICE **--
RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Create all panels list (crp_all_panels)';

TRUNCATE rel_pub.crp_all_panel;
INSERT INTO 
  rel_pub.crp_all_panel
(
  mid,
  pid,
  the_geom,
  buffer_geom,
  maxdist
)
SELECT
  a.mid,
  a.pid,
  b.the_geom,
  geometry(st_buffer(geography(b.the_geom),a.maxdist + 200)) AS buffer_geom,
  a.maxdist
FROM
  pnl.pnl_main a
  INNER JOIN pnl.pnl_geom b ON a.mid = b.mid
  INNER JOIN pnl.mv_pnl_main_maxdate c ON a.mid = c.mid
WHERE 
	(c.status = 3 AND c.timeattid = 0)
  OR a.pid IN(SELECT pid FROM rel_pub.crp_missing)
;

-- Odtranění duplicit v seznamu panelů.
TRUNCATE TABLE rel_pub.crp_geom_panel_temp;
INSERT INTO rel_pub.crp_geom_panel_temp SELECT DISTINCT * FROM rel_pub.crp_all_panel;
TRUNCATE TABLE rel_pub.crp_all_panel;
INSERT INTO rel_pub.crp_all_panel SELECT * FROM rel_pub.crp_geom_panel_temp;

-- Test, jestli je co zpracovat, jinak se to skočí.
SELECT COUNT(*) INTO i_cnt FROM rel_pub.crp_all_panel;

-- Panely ze crp_missing jsou vloženy pro výpočet možno smazat.
TRUNCATE rel_pub.crp_missing;

-- Hlavní zpracování, výpočty ------------------------------------------------------------------------------------------
IF i_cnt > 0 THEN
	
	-- Založí se nový load a sosne se jeho id do proměnné load_id.
  -- Pro debug mode je load_id = -1
  INSERT INTO rel_pub.etl_ccf_rel_pub_loads ("current_user")
  VALUES (CURRENT_USER);

  SELECT load_id INTO last_load_id FROM rel_pub.etl_ccf_rel_pub_loads ORDER BY load_id DESC;
    
--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' New load_id:' || last_load_id;

--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Recs found:' || i_cnt;

--** Tady je NOTICE **--	
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Updating last_vacid, err_vac_res.';
  
	UPDATE
    rel_pub.crp_all_panel x
  SET
    last_vacid = sqry.vacid,
    err_vac_res = sqry.err_vac_res
  FROM
  (
    SELECT DISTINCT ON (v.mid)
      v.vacid,
      v.mid,
      v.err_vac_res
    FROM 
      vai.vai_vac_res v JOIN rel_pub.crp_all_panel p ON v.mid = p.mid
    WHERE
      1 = 1
      --AND p.mid = 53837
    ORDER BY
      v.mid,
      v.vacid DESC
      ) sqry
    WHERE x.mid = sqry.mid
  ;
  
--** Tady je NOTICE **--	
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Updating VACs.';
	
  --Doupdate VAC k panelům, bere se transptype = 'ALL' a periodid = 2, proto se pole jmenuje "vac_all_2"
  UPDATE
    rel_pub.crp_all_panel x
  SET
    vac_all_2 = sqry.vac
  FROM
  (
    SELECT DISTINCT ON (v.mid)
      v.mid,
      v.vacid,
      r.vac
    FROM
      vai.vai_vac_res v
      JOIN res.res_online_ea_output_round r ON v.vacid = r.vacid
    WHERE
      r.periodid = 1
      AND r.transptype = 'ALL'
    ORDER BY
      v.mid,
      v.vacid DESC
  ) sqry
  WHERE
    x.last_vacid = sqry.vacid
  ;
  
--** Tady je NOTICE **--	
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Non calculating panels management.';

	--Zaznamenám panely, které nemají napočítáno, ty se, mimo jiné, nedostanou do výstupu
  INSERT INTO rel_pub.crp_no_calc (pid, err_vac_res)
  SELECT 
    pid,
    err_vac_res
  FROM 
    rel_pub.crp_all_panel 
  WHERE
    (vac_all_2 = 0
    OR vac_all_2 IS NULL
    OR err_vac_res IS NOT NULL)
    AND pid NOT IN (SELECT pid FROM rel_pub.crp_no_calc)
  ;
  -- A vyřadím je z dalších akcí.
	DELETE FROM rel_pub.crp_all_panel WHERE pid IN (SELECT pid FROM rel_pub.crp_no_calc);
  
--** Tady je NOTICE **--	
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Create IMS input.';

	-- Zde se vytvoří crp_ims_input
  PERFORM rel_pub.create_ims_input_ccf();

--** Tady je NOTICE **--	
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Calculate intersection.';

  ---- Výpočet všech intersekcí, prusecik kolacu bufferu a tripu. (Časově náročné).
  TRUNCATE rel_pub.crp_all_intersection;
  INSERT INTO rel_pub.crp_all_intersection
  SELECT
    a.mid, b.sid
  FROM
    rel_pub.crp_all_panel a
    INNER JOIN rel_pub.crp_routing b ON st_intersects(a.buffer_geom, b.geom) AND a.buffer_geom && b.geom
  ;
	
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Update intersection type.';
  
  --Doupdate 'ORIGINAL' a 'SUBSTITUTE' v hlavní tabulce podle spočítaných intersekcí.
  --Panely, které nemají v crp_all_intersection záznam se musí dohledat (níže).
  --Doupdate 'ORIGINAL'
  UPDATE rel_pub.crp_all_panel
  SET intersection_type = 'ORIGINAL'
  WHERE mid IN(SELECT DISTINCT mid FROM rel_pub.crp_all_intersection);

	--Doupdate 'SUBSTITUTE'
  UPDATE rel_pub.crp_all_panel
  SET intersection_type = 'SUBSTITUTE'
  WHERE intersection_type IS NULL;

--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Filling crp_evidence from crp_all_intersection (ORIGINAL).';

  --Celkový seznam, naplnění tabulky, nejprve ty, co mají průsečík s trasou, pak se sem pak přidají panely bez průsečíku
  TRUNCATE rel_pub.crp_evidence;
  INSERT INTO rel_pub.crp_evidence
  SELECT mid, sid
  FROM rel_pub.crp_all_intersection;
	
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Listing panels wo intersection to crp_panel_wo_its.';

  --Panely bez intersekce
  TRUNCATE rel_pub.crp_panel_wo_its;
  INSERT INTO rel_pub.crp_panel_wo_its
  SELECT 
    mid,
    pid,
    the_geom,
    buffer_geom,
    vac_all_2
  FROM
    rel_pub.crp_all_panel 
  WHERE intersection_type = 'SUBSTITUTE';
	
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Finding nearest panels for panels wo intersection (best of 20).';

  --Nalezeni nejblizsiho MIDu (pro vsechny verze tripu) a doplneni sidu z tech tripu do evidence delanych midu
  TRUNCATE TABLE rel_pub.crp_nearest20;
  WITH nearest20 AS (
      -- 1. Pro každý bod T2 najdeme 20 geometricky nejbližších bodů T1
      SELECT
          t2.mid AS mid_woi,
          t2.vac_all_2 AS vac_woi,
          t1.mid AS mid_near,
          t1.vac_all_2 AS vac_near,
          ST_Distance(t2.the_geom, t1.the_geom) AS distance,
          ROW_NUMBER() OVER (
              PARTITION BY t2.mid
              ORDER BY t2.the_geom <-> t1.the_geom -- Řazení podle vzdálenosti s GiST indexem
          ) as rn_geom  -- Pořadí podle geometrické vzdálenosti
      FROM
          rel_pub.crp_panel_wo_its t2,
          rel_pub.crp_all_panel t1 -- Používáme implicitní JOIN pro KNN dotaz
      -- WHERE ST_DWithin(t2.geom, t1.geom, 5000) -- Volitelné omezení maximální vzdálenosti (např. 5 km)
      WHERE
        t1.intersection_type = 'ORIGINAL'
  )
  , best_match AS (
      -- 2. Z 20 nejbližších vybereme ten s minimálním rozdílem hodnot
      SELECT
          n20.*,
          ABS(n20.vac_woi - n20.vac_near) AS value_difference,
          ROW_NUMBER() OVER (
              PARTITION BY n20.mid_woi
              ORDER BY
                  ABS(n20.vac_woi - n20.vac_near) ASC, -- Primární řazení: nejmenší rozdíl hodnot
                  n20.distance ASC                     -- Sekundární řazení (tie-breaker): nejmenší geometrická vzdálenost
          ) as rn_value -- Pořadí podle rozdílu hodnot
      FROM
          nearest20 n20
      WHERE
          n20.rn_geom <= 20 -- Omezíme na 20 nejbližších z hlediska geometrie
  )
  INSERT INTO rel_pub.crp_nearest20
  SELECT
      bm.mid_woi,
      bm.vac_woi,
      bm.mid_near AS best_mid_near,
      bm.vac_near AS best_vac_near,
      bm.distance AS geometric_distance,
      bm.value_difference
  FROM
      best_match bm
  WHERE
      bm.rn_value = 1;
	
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Filling substituted trips';
  
  INSERT INTO rel_pub.crp_evidence  
  SELECT 
    mid_woi,
    sid
  FROM 
    rel_pub.crp_nearest20 a
    JOIN rel_pub.crp_all_intersection b ON a.best_mid_near = b.mid
  ;

--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' End of panel preparation.';

--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Creating ccf_input.';

  TRUNCATE rel_pub.crp_ccf_input;
  INSERT INTO rel_pub.crp_ccf_input
  	(
    	mid,
      sid,
      respondent_id,
      routingmode,
      pid,
      desc_text)
  SELECT 
    e.mid,
    e.sid,
    o.respondent_id,
    o.routingmode,
    m.pid,
    e.desc_text
  FROM 
    rel_pub.crp_evidence e
    JOIN rel_pub.crp_routing o ON e.sid = o.sid
    JOIN pnl.pnl_main m ON e.mid = m.mid
  WHERE
  	m.pid IS NOT NULL AND m.pid <> 0 --Potlačím nespočítané panely.
  ;
  
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Creating ccf_output.';

	TRUNCATE TABLE rel_pub.crp_ts_rots_temp;
  TRUNCATE TABLE rel_pub.crp_ccf_output;
  TRUNCATE TABLE rel_pub.crp_ims_temp;

	-- Optimalizováno, místo původních 4 dotazů na jeden, všechny tripy vloženy do crp_routing
  INSERT INTO rel_pub.crp_ccf_output (pid, sid, respweight, modality, dayofweek, sbjnum)
	SELECT 
    ci.pid, 
    ci.sid, 
    rwi.benchwgt_pp, 
    ci.routingmode, 
    rou.dayofweek,
    rwi.sbjnum
  FROM 
    rel_pub.crp_routing rou
    JOIN rel_pub.crp_ccf_input ci ON rou.sid = ci.sid
    JOIN rel_pub.crp_resp_weight_input rwi ON rou.sbjnum = rwi.sbjnum
  WHERE  
    ci.sbjnum = rwi.sbjnum
  ;

  -- qry000_05_update_weekDay
  UPDATE rel_pub.crp_ccf_output SET dayofweek = LEFT(dayofweek,2);
	
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Trips filled to crp_ccf_output.';
	

  -- Naplní crp_ims_temp
  INSERT INTO rel_pub.crp_ims_temp ( 
    pid, mid, pnlkey, pnldesc2, 
    facepid, ownid_desc, pnlillum_desc, 
    pnlmotion_desc, frmwidth, frmheight, pnlvaienv, 
    pnldesc, district, vac_brutto_weekly_all, vac_brutto_weekly_veh, 
    vac_brutto_weekly_ped, rots_brutto_weekly_all, rots_brutto_weekly_veh, rots_brutto_weekly_ped)
  SELECT 
    pid, mid, pnlkey, pnldesc2, 
    facepid, ownid_desc, pnlillum_desc, 
    pnlmotion_desc, frmwidth, frmheight, pnlvaienv, 
    pnldesc, district, vac_brutto_daily_all*7, vac_brutto_daily_veh*7, 
    vac_brutto_daily_ped*7, rots_brutto_daily_all*7, rots_brutto_daily_veh*7, rots_brutto_daily_ped*7
  FROM rel_pub.crp_ims_input;

--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Table crp_ims_temp filled.';

  -- Naplní crp_ts_rots_temp
  INSERT INTO rel_pub.crp_ts_rots_temp ( pid, ts_rots )
  SELECT pid, SUM(respweight)
  FROM rel_pub.crp_ccf_output
  GROUP BY pid;

--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Table crp_ts_rots_temp filled.';

  -- Updatne TIM_performance
  UPDATE rel_pub.crp_ts_rots_temp trt
  SET tim_rots = it.rots_brutto_weekly_all, tim_vac = it.vac_brutto_weekly_all
  FROM rel_pub.crp_ims_temp AS it 
  WHERE trt.pid = it.pid;

--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Column tim_rots filled.';

  -- Updatne TS_VA
  UPDATE rel_pub.crp_ts_rots_temp SET ts_va = tim_vac/ts_rots;

  -- Updatne TS_Freq
  UPDATE rel_pub.crp_ts_rots_temp SET ts_freq = CASE WHEN ts_va > 1 THEN ts_va + 1 ELSE 1 END;

  -- Updatne VA_Adj
  UPDATE rel_pub.crp_ts_rots_temp SET ts_va_adj = ts_va/ts_freq;
    
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Table crp_ts_rots_temp updated.';

  -- Updatne VA a VAC
  UPDATE rel_pub.crp_ccf_output c
  SET 
    va = 	CASE WHEN trt.ts_freq < 20 THEN 1-(1-trt.ts_va_adj)^trt.ts_freq 
            ELSE 1
            END, 
    vac = trt.ts_va_adj*trt.ts_freq*respweight
  FROM rel_pub.crp_ts_rots_temp AS trt
  WHERE c.pid = trt.pid;


--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Table crp_ccf_output updated.';
	
  IF NOT debug_mode THEN
  	-- Final load
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Final load.';

    exe_str = 'INSERT INTO 
                  rel_pub.etl_ccf_rel_pub
                 (
                  pid,
                  mid,
                  facepid,
                  faceid,
                  respondent_id,
                  weekdayname,
                  resp_weight,
                  va,
                  vac,
                  load_id
                ) 
                  SELECT 
                    crp_ccf_output.pid,
                    crp_ims_temp.mid, 
                    crp_ims_temp.facepid, 
                    0 AS facepid, 
                    crp_ccf_output.sbjnum, 
                    LOWER(crp_ccf_output.dayofweek), 
                    crp_ccf_output.respweight, 
                    crp_ccf_output.va, 
                    round(crp_ccf_output.vac), ' || last_load_id || '
                  FROM rel_pub.crp_ccf_output 
                    INNER JOIN rel_pub.crp_ims_temp ON crp_ccf_output.pid = crp_ims_temp.pid; ';
    EXECUTE exe_str;
		
		-- Evidence zpracovaných panelů v jednotlivých loadech
		INSERT INTO 
			rel_pub.crp_geom_panel_log
		(
			mid,
			pid,
			the_geom,
			buffer_geom,
			load_id,
			desc_text
		)
		SELECT 
			mid,
			pid,
			the_geom,
			buffer_geom,
			last_load_id,
			'Processed by load'
		FROM 
			rel_pub.crp_all_panel
		;
	ELSE
  	-- Debug load
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Debug load.';

		TRUNCATE rel_pub.dbg_etl_ccf_rel_pub;
    exe_str = 'INSERT INTO 
                  rel_pub.dbg_etl_ccf_rel_pub
                 (
                  pid,
                  mid,
                  facepid,
                  faceid,
                  respondent_id,
                  weekdayname,
                  resp_weight,
                  va,
                  vac,
                  load_id
                ) 
                  SELECT 
                    crp_ccf_output.pid,
                    crp_ims_temp.mid, 
                    crp_ims_temp.facepid, 
                    0 AS facepid, 
                    crp_ccf_output.sbjnum, 
                    LOWER(crp_ccf_output.dayofweek), 
                    crp_ccf_output.respweight, 
                    crp_ccf_output.va, 
                    round(crp_ccf_output.vac), ' || last_load_id || '
                  FROM rel_pub.crp_ccf_output 
                    INNER JOIN rel_pub.crp_ims_temp ON crp_ccf_output.pid = crp_ims_temp.pid; ';
    EXECUTE exe_str;
		
		-- Evidence zpracovaných panelů v jednotlivých loadech
		INSERT INTO 
			rel_pub.dbg_geom_panel_log
		(
			mid,
			pid,
			the_geom,
			buffer_geom,
			load_id,
			desc_text
		)
		SELECT 
			mid,
			pid,
			the_geom,
			buffer_geom,
			last_load_id,
			'Processed by load'
		FROM 
			rel_pub.crp_all_panel
		;
		END IF;
  
--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Log of processed panels.';

-- Znovu se načte, některé panely mohly být vyřazeny
	SELECT COUNT(*) INTO i_cnt FROM rel_pub.crp_all_panel;

--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Load ID: ' || last_load_id || ' - successfuly done. Num of recs: ' || i_cnt || '.';
  PERFORM rel_pub.log_msg('INFO', 'fce:auto_rel_pub', 'Load ID: ' || last_load_id || ' - successfuly done. Num of recs: ' || i_cnt || '.');
ELSE
--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' recs not found, exiting.';
	PERFORM rel_pub.log_msg('INFO', 'fce:auto_rel_pub', 'Load did not run, no panel records.');
END IF;

EXCEPTION
WHEN others THEN
  RAISE NOTICE 'exception: %', SQLERRM;
  PERFORM rel_pub.log_msg('ERROR', 'auto_rel_pub', 'Error occured: ' || SQLERRM);
END;