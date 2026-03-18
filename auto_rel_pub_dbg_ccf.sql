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
  
BEGIN
-------------------------------------------------------------------------------------------------------------------
--** Tady je NOTICE **--
RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Function auto_rel_pub_dbg_ccf start.';
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
TRUNCATE rel_pub_dbg.crp_all_panel;
INSERT INTO 
  rel_pub_dbg.crp_all_panel
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
  OR a.pid IN(SELECT pid FROM rel_pub_dbg.crp_missing)
;
-- Odtranění duplicit v seznamu panelů.
TRUNCATE TABLE rel_pub_dbg.crp_geom_panel_temp;
INSERT INTO rel_pub_dbg.crp_geom_panel_temp SELECT DISTINCT * FROM rel_pub_dbg.crp_all_panel;
TRUNCATE TABLE rel_pub_dbg.crp_all_panel;
INSERT INTO rel_pub_dbg.crp_all_panel SELECT * FROM rel_pub_dbg.crp_geom_panel_temp;
-- Test, jestli je co zpracovat, jinak se to skočí.
SELECT COUNT(*) INTO i_cnt FROM rel_pub_dbg.crp_all_panel;
-- Panely ze crp_missing jsou vloženy pro výpočet možno smazat.
TRUNCATE rel_pub_dbg.crp_missing;
-- Hlavní zpracování, výpočty ------------------------------------------------------------------------------------------
IF i_cnt > 0 THEN
	
	-- Založí se nový load a sosne se jeho id do proměnné load_id.
  -- Pro debug mode je load_id = -1
  INSERT INTO rel_pub_dbg.etl_ccf_rel_pub_dbg_loads ("current_user")
  VALUES (CURRENT_USER);
  SELECT load_id INTO last_load_id FROM rel_pub_dbg.etl_ccf_rel_pub_dbg_loads ORDER BY load_id DESC;
    
--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' New load_id:' || last_load_id;
--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Recs found:' || i_cnt;
--** Tady je NOTICE **--	
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Updating last_vacid, err_vac_res.';
  
	UPDATE
    rel_pub_dbg.crp_all_panel x
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
      vai.vai_vac_res v JOIN rel_pub_dbg.crp_all_panel p ON v.mid = p.mid
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
	
  --Doupdate VAC k panelům, bere se transptype = 'ALL' a periodid = 1, proto se pole jmenuje "vac_all_1"
  UPDATE
    rel_pub_dbg.crp_all_panel x
  SET
    vac_all_1 = sqry.vac
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
  INSERT INTO rel_pub_dbg.crp_no_calc (pid, err_vac_res)
  SELECT 
    pid,
    err_vac_res
  FROM 
    rel_pub_dbg.crp_all_panel 
  WHERE
    (vac_all_1 = 0
    OR vac_all_1 IS NULL
    OR err_vac_res IS NOT NULL)
    AND pid NOT IN (SELECT pid FROM rel_pub_dbg.crp_no_calc)
  ;
  -- A vyřadím je z dalších akcí.
	DELETE FROM rel_pub_dbg.crp_all_panel WHERE pid IN (SELECT pid FROM rel_pub_dbg.crp_no_calc);
  
--** Tady je NOTICE **--	
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Create IMS input.';
	-- Zde se vytvoří crp_ims_input
  PERFORM rel_pub_dbg.create_ims_input_ccf();
	--SELECT * FROM rel_pub_dbg.create_ims_input_ccf();
--** Tady je NOTICE **--	
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Calculate intersection.';
  -- Výpočet všech intersekcí, prusecik kolacu bufferu a tripu.
  TRUNCATE rel_pub_dbg.crp_all_intersection;
  INSERT INTO rel_pub_dbg.crp_all_intersection
  	(mid, sid, week_day_name, week_day_type)
  SELECT
    a.mid, b.sid, b.week_day_name, b.week_day_type
  FROM
    rel_pub_dbg.crp_all_panel a
    INNER JOIN rel_pub_dbg.crp_routing b 
      ON a.buffer_geom && b.geom       					-- Indexový test obálek
      AND ST_Intersects(a.buffer_geom, b.geom)	-- Přesný prostorový test
  ;
	
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Update intersection type.';
  
  --Doupdate 'ORIGINAL' a 'SUBSTITUTE' v hlavní tabulce podle spočítaných intersekcí.
  --Panely, které nemají v crp_all_intersection záznam se musí dohledat (níže).
  --Doupdate 'ORIGINAL'
  UPDATE rel_pub_dbg.crp_all_panel
  SET intersection_type = 'ORIGINAL'
  WHERE mid IN(SELECT DISTINCT mid FROM rel_pub_dbg.crp_all_intersection);
	--Doupdate 'SUBSTITUTE'
  UPDATE rel_pub_dbg.crp_all_panel
  SET intersection_type = 'SUBSTITUTE'
  WHERE intersection_type IS NULL;
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Filling crp_evidence from crp_all_intersection (ORIGINAL).';
  --Celkový seznam, naplnění tabulky, nejprve ty, co mají průsečík s trasou, pak se sem pak přidají panely bez průsečíku
  TRUNCATE rel_pub_dbg.crp_evidence;
  INSERT INTO rel_pub_dbg.crp_evidence
  	(mid, sid, week_day_name, week_day_type)
  SELECT mid, sid, week_day_name, week_day_type
  FROM rel_pub_dbg.crp_all_intersection;
	
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Listing panels wo intersection to crp_panel_wo_its.';
  --Panely bez intersekce
  TRUNCATE rel_pub_dbg.crp_panel_wo_its;
  INSERT INTO rel_pub_dbg.crp_panel_wo_its
  SELECT 
    mid,
    pid,
    the_geom,
    buffer_geom,
    vac_all_1
  FROM
    rel_pub_dbg.crp_all_panel 
  WHERE intersection_type = 'SUBSTITUTE';
	
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Finding nearest panels for panels wo intersection (best of 20).';
  --Nalezeni nejblizsiho MIDu (pro vsechny verze tripu) a doplneni sidu z tech tripu do evidence delanych midu
  TRUNCATE TABLE rel_pub_dbg.crp_nearest20;
  WITH nearest20 AS (
      -- 1. Pro každý bod T2 najdeme 20 geometricky nejbližších bodů T1
      SELECT
          t2.mid AS mid_woi,
          t2.vac_all_1 AS vac_woi,
          t1.mid AS mid_near,
          t1.vac_all_1 AS vac_near,
          ST_Distance(t2.the_geom, t1.the_geom) AS distance,
          ROW_NUMBER() OVER (
              PARTITION BY t2.mid
              ORDER BY t2.the_geom <-> t1.the_geom -- Řazení podle vzdálenosti s GiST indexem
          ) as rn_geom  -- Pořadí podle geometrické vzdálenosti
      FROM
          rel_pub_dbg.crp_panel_wo_its t2,
          rel_pub_dbg.crp_all_panel t1 -- Používáme implicitní JOIN pro KNN dotaz
      WHERE ST_DWithin(t2.the_geom, t1.the_geom, 5000) -- Volitelné omezení maximální vzdálenosti (např. 5 km)
      AND t1.intersection_type = 'ORIGINAL'
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
  INSERT INTO rel_pub_dbg.crp_nearest20
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
  
  INSERT INTO rel_pub_dbg.crp_evidence  
  SELECT 
    mid_woi, sid, b.week_day_name, b.week_day_type
  FROM 
    rel_pub_dbg.crp_nearest20 a
    JOIN rel_pub_dbg.crp_all_intersection b ON a.best_mid_near = b.mid
  ;
--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' End of panel preparation.'; 
  
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Creating ccf_output.';
	TRUNCATE TABLE rel_pub_dbg.crp_ts_rots_temp;
  TRUNCATE TABLE rel_pub_dbg.crp_ccf_output;
  
	-- Optimalizováno, místo původních 4 dotazů na jeden, všechny tripy vloženy do crp_routing
  INSERT INTO rel_pub_dbg.crp_ccf_output (pid, sid, respweight, modality, dayofweek, respondent_id, ts_rots, facepid, faceid)  
  SELECT 
    m.pid, 
    rou.sid, 
    rwi.benchwgt_pp, 
    rou.routingmode, 
    rou.week_day_name,
    rwi.respondent_id,
    rwi.benchwgt_pp * ftv.prob AS ts_rots,
    f.facepid,
    f.faceid
  FROM
    rel_pub_dbg.crp_evidence e
    JOIN rel_pub_dbg.crp_routing rou ON e.sid = rou.sid
    JOIN pnl.pnl_main m ON e.mid = m.mid
    JOIN pnl.pnl_facemid f ON e.mid = f.mid
    JOIN rel_pub_dbg.crp_resp_weight_input rwi ON rou.respondent_id = rwi.respondent_id
    JOIN rel_pub_dbg.crp_freq_trips_vector ftv ON rou.week_day_type = ftv.weekdaytype
  WHERE 
  	rou.trip_freq > ftv.freq_from
    AND rou.trip_freq <= ftv.freq_to
    AND rou.week_day_type = ftv.weekdaytype
  ;
  --UPDATE rel_pub_dbg.crp_ccf_output SET dayofweek = LEFT(dayofweek,2);
	
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Trips filled to crp_ccf_output.';

  -- Naplní crp_ts_rots_temp
  INSERT INTO rel_pub_dbg.crp_ts_rots_temp (pid, faceid, ts_rots)
  SELECT pid, faceid, SUM(ts_rots)
  FROM rel_pub_dbg.crp_ccf_output
  GROUP BY pid, faceid;

--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Table crp_ts_rots_temp filled.';
  
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Column ims_rots filled.';
  
  -- Updatne ims_performance
	UPDATE rel_pub_dbg.crp_ts_rots_temp trt
  SET 
  	ims_rots = it.rots_week_all,
    ims_vac  = it.vac_week_all,
    ims_va   = it.vac_week_all::DOUBLE PRECISION / it.rots_week_all
  FROM rel_pub_dbg.crp_ims_input AS it 
  WHERE trt.faceid = it.faceid_db;
  
  UPDATE rel_pub_dbg.crp_ts_rots_temp trt
	SET ts_va = ims_vac::DOUBLE PRECISION / ts_rots;
  
  UPDATE rel_pub_dbg.crp_ts_rots_temp trt
	SET 
  	ts_freq = 
    	CASE WHEN ts_va > 1 
      	THEN FLOOR(ts_va) + 1
      	ELSE 1
      END
  ;
  
  UPDATE rel_pub_dbg.crp_ts_rots_temp trt
	SET ts_va_adj = ts_va::DOUBLE PRECISION / ts_freq;
    
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Table crp_ts_rots_temp updated.';
  
  -- Updatne VA a VAC v crp_ccf_output
  UPDATE rel_pub_dbg.crp_ccf_output c
  SET 
    va = CASE 
      -- Pokud je technická viditelnost 0, výsledek je vždy 0
      WHEN trt.ts_va_adj <= 0 THEN 0
      -- Pokud je technická viditelnost >= 1, uvidí to hned napoprvé
      WHEN trt.ts_va_adj >= 1 THEN 1
      -- Pokud je frekvence příliš vysoká, uvidí to s jistotou (prevence Underflow)
      WHEN trt.ts_freq >= 100 THEN 1
      -- Jinak standardní CCF vzorec
      ELSE 1 - POWER(1 - trt.ts_va_adj, trt.ts_freq)
    END, 
		vac = trt.ts_va_adj * trt.ts_freq * c.ts_rots
  FROM rel_pub_dbg.crp_ts_rots_temp AS trt
  WHERE c.faceid = trt.faceid;
  
  TRUNCATE TABLE rel_pub_dbg.crp_control_sum;
  INSERT INTO rel_pub_dbg.crp_control_sum
  SELECT
  	pid,
    faceid,
    SUM(ts_rots) AS sum_of_ts_rots,
    SUM(vac) AS sum_of_ts_vac,
    last_load_id
  FROM rel_pub_dbg.crp_ccf_output c
  GROUP BY pid, faceid;
  
--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Table crp_ccf_output updated.';
	
  -- Final load
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Final load.';
  exe_str = 'INSERT INTO 
                rel_pub_dbg.etl_ccf_rel_pub_dbg
                (
                  pid,
                  mid,
                  facepid,
                  faceid,
                  vacid,
                  respondent_id,
                  country,
                  weekdayname,
                  resp_weight,
                  ts_rots,
                  va,
                  vac,
                  load_id
                ) 
                SELECT --DISTINCT
                  cco.pid,
                  cap.mid, 
                  cco.facepid, 
                  cco.faceid,
                  cap.last_vacid,
                  cco.respondent_id,
                  LEFT(cco.respondent_id, 3) AS country, 
                  LOWER(cco.dayofweek), 
                  cco.respweight,
                  cco.ts_rots,
                  cco.va, 
                  round(cco.vac), ' || last_load_id || '
                FROM rel_pub_dbg.crp_ccf_output cco
				  				JOIN rel_pub_dbg.crp_all_panel cap ON cco.pid = cap.pid
                  ; ';
                  
  EXECUTE exe_str;
		
  -- Evidence zpracovaných panelů v jednotlivých loadech
  INSERT INTO 
    rel_pub_dbg.crp_geom_panel_log
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
    rel_pub_dbg.crp_all_panel
  ;
  
--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Log of processed panels.';
-- Znovu se načte, některé panely mohly být vyřazeny
	SELECT COUNT(*) INTO i_cnt FROM rel_pub_dbg.crp_all_panel;
--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Load ID: ' || last_load_id || ' - successfuly done. Num of recs: ' || i_cnt || '.';
  PERFORM rel_pub_dbg.log_msg('INFO', 'fce:auto_rel_pub_dbg', 'Load ID: ' || last_load_id || ' - successfuly done. Num of recs: ' || i_cnt || '.');
ELSE
--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' recs not found, exiting.';
	PERFORM rel_pub_dbg.log_msg('INFO', 'fce:auto_rel_pub_dbg', 'Load did not run, no panel records.');
END IF;
EXCEPTION
WHEN others THEN
  RAISE NOTICE 'exception: %', SQLERRM;
  PERFORM rel_pub_dbg.log_msg('ERROR', 'auto_rel_pub_dbg', 'Error occured: ' || SQLERRM);
END;