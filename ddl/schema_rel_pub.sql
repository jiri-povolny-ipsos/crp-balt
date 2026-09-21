--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

--
-- Name: rel_pub; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA rel_pub;


SET search_path = rel_pub, pg_catalog;

--
-- Name: auto_rel_pub_ccf(); Type: FUNCTION; Schema: rel_pub; Owner: -
--

CREATE FUNCTION auto_rel_pub_ccf() RETURNS void
    LANGUAGE plpgsql
    AS $$
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
RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Function auto_rel_pub_ccf start.';
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
	(c.status = 3 AND c.timeattid = 0 AND c.ownid = c.subownid)
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
	
  --Doupdate VAC k panelům, bere se transptype = 'ALL' a periodid = 1, proto se pole jmenuje "vac_all_1"
  UPDATE
    rel_pub.crp_all_panel x
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
  INSERT INTO rel_pub.crp_no_calc (pid, err_vac_res)
  SELECT 
    pid,
    err_vac_res
  FROM 
    rel_pub.crp_all_panel 
  WHERE
    (vac_all_1 = 0
    OR vac_all_1 IS NULL
    OR err_vac_res IS NOT NULL)
    AND pid NOT IN (SELECT pid FROM rel_pub.crp_no_calc)
  ;
  -- A vyřadím je z dalších akcí.
	DELETE FROM rel_pub.crp_all_panel WHERE pid IN (SELECT pid FROM rel_pub.crp_no_calc);
  
--** Tady je NOTICE **--	
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Create IMS input.';
	-- Zde se vytvoří crp_ims_input
  PERFORM rel_pub.create_ims_input_ccf();
	--SELECT * FROM rel_pub.create_ims_input_ccf();
--** Tady je NOTICE **--	
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Calculate intersection.';
  -- Výpočet všech intersekcí, prusecik kolacu bufferu a tripu.
  TRUNCATE rel_pub.crp_all_intersection;
  INSERT INTO rel_pub.crp_all_intersection
  	(mid, sid, week_day_name, week_day_type)
  SELECT
    a.mid, b.sid, b.week_day_name, b.week_day_type
  FROM
    rel_pub.crp_all_panel a
    INNER JOIN rel_pub.crp_routing b 
      ON a.buffer_geom && b.geom       					-- Indexový test obálek
      AND ST_Intersects(a.buffer_geom, b.geom)	-- Přesný prostorový test
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
  	(mid, sid, week_day_name, week_day_type)
  SELECT mid, sid, week_day_name, week_day_type
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
    vac_all_1
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
          t2.vac_all_1 AS vac_woi,
          t1.mid AS mid_near,
          t1.vac_all_1 AS vac_near,
          ST_Distance(t2.the_geom, t1.the_geom) AS distance,
          ROW_NUMBER() OVER (
              PARTITION BY t2.mid
              ORDER BY t2.the_geom <-> t1.the_geom -- Řazení podle vzdálenosti s GiST indexem
          ) as rn_geom  -- Pořadí podle geometrické vzdálenosti
      FROM
          rel_pub.crp_panel_wo_its t2,
          rel_pub.crp_all_panel t1 -- Používáme implicitní JOIN pro KNN dotaz
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
    mid_woi, sid, b.week_day_name, b.week_day_type
  FROM 
    rel_pub.crp_nearest20 a
    JOIN rel_pub.crp_all_intersection b ON a.best_mid_near = b.mid
  ;
--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' End of panel preparation.'; 
  
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Creating ccf_output.';
	TRUNCATE TABLE rel_pub.crp_ts_rots_temp;
  TRUNCATE TABLE rel_pub.crp_ccf_output;
  
	-- Optimalizováno, místo původních 4 dotazů na jeden, všechny tripy vloženy do crp_routing
  INSERT INTO rel_pub.crp_ccf_output (pid, sid, respweight, modality, dayofweek, respondent_id, ts_rots, facepid, faceid)  
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
    rel_pub.crp_evidence e
    JOIN rel_pub.crp_routing rou ON e.sid = rou.sid
    JOIN pnl.pnl_main m ON e.mid = m.mid
    JOIN pnl.pnl_facemid f ON e.mid = f.mid
    JOIN pnl.pnl_faces f2 ON f.facepid = f2.facepid
    JOIN rel_pub.crp_resp_weight_input rwi ON rou.respondent_id = rwi.respondent_id
    JOIN rel_pub.crp_freq_trips_vector ftv ON rou.week_day_type = ftv.weekdaytype
  WHERE 
  	rou.trip_freq > ftv.freq_from
    AND rou.trip_freq <= ftv.freq_to
    AND rou.week_day_type = ftv.weekdaytype
    AND(m.pnlmotion <> 'D' OR (m.pnlmotion = 'D' AND f2.pos = 1))
  ;
  --UPDATE rel_pub.crp_ccf_output SET dayofweek = LEFT(dayofweek,2);
	
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Trips filled to crp_ccf_output.';

  -- Naplní crp_ts_rots_temp
  INSERT INTO rel_pub.crp_ts_rots_temp (pid, faceid, ts_rots)
  SELECT pid, faceid, SUM(ts_rots)
  FROM rel_pub.crp_ccf_output
  GROUP BY pid, faceid;

--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Table crp_ts_rots_temp filled.';
  
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Column ims_rots filled.';
  
  -- Updatne ims_performance
	UPDATE rel_pub.crp_ts_rots_temp trt
  SET 
  	ims_rots = it.rots_week_all,
    ims_vac  = it.vac_week_all,
    ims_va   = it.vac_week_all::DOUBLE PRECISION / it.rots_week_all
  FROM rel_pub.crp_ims_input AS it 
  WHERE trt.faceid = it.faceid_db;
  
  UPDATE rel_pub.crp_ts_rots_temp trt
	SET ts_va = ims_vac::DOUBLE PRECISION / ts_rots;
  
  UPDATE rel_pub.crp_ts_rots_temp trt
	SET 
  	ts_freq = 
    	CASE WHEN ts_va > 1 
      	THEN FLOOR(ts_va) + 1
      	ELSE 1
      END
  ;
  
  UPDATE rel_pub.crp_ts_rots_temp trt
	SET ts_va_adj = ts_va::DOUBLE PRECISION / ts_freq;
    
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Table crp_ts_rots_temp updated.';
  
  -- Updatne VA a VAC v crp_ccf_output
  UPDATE rel_pub.crp_ccf_output c
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
  FROM rel_pub.crp_ts_rots_temp AS trt
  WHERE c.faceid = trt.faceid;
  
  --TRUNCATE TABLE rel_pub.crp_control_sum;
  INSERT INTO rel_pub.crp_control_sum
  SELECT
  	pid,
    faceid,
    SUM(ts_rots) AS sum_of_ts_rots,
    SUM(vac) AS sum_of_ts_vac,
    last_load_id,
    'INIT'
  FROM rel_pub.crp_ccf_output c
  GROUP BY pid, faceid;
  
--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Table crp_ccf_output updated.';
	
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
                FROM rel_pub.crp_ccf_output cco
				  				JOIN rel_pub.crp_all_panel cap ON cco.pid = cap.pid
                  ; ';
                  
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
$$;


--
-- Name: auto_rel_pub_inc(); Type: FUNCTION; Schema: rel_pub; Owner: -
--

CREATE FUNCTION auto_rel_pub_inc() RETURNS void
    LANGUAGE plpgsql
    AS $$
/**********************************************************************************************
*
*	Vyrobil: Jirka Povolný, 8.3.2026
*
*	Používá se k automatickému výpočtu CCF na denní bázi.
*	A už i k dennímu, kontinuálnímu publikování.dbg_ims_input_inc
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
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Function auto_rel_pub_inc start.';

	-- Načtení aktuální navteqver pro kontrolu správného navteqver u panelů.
	SELECT navteq_version INTO navteqver_str FROM fnc.f_params();

--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Navteq version: ' || navteqver_str;


  -- INSERT panelů, kde proběhla za poslední den změna v timestamp.
  -- Rovněž se přidají panely z tabulky crp_missing, kam se dají přidávat panely "z venku", ručně.
  -- Do tabulky crp_missing se také budou ukládat panely, 
  -- které se budou přepočítávat online strojkem, aby se zprocesovaly v dalším běhu (viz níže).
  -- Tabulka crp_geom_panel obsahuje seznam panelů s jejich geometrií (tabulka s panely a jejich kolaci, vzdalenost maxdist + 200m).

  TRUNCATE rel_pub.crp_geom_panel;

--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Create panels list (crp_geom_panel).';
  -- Insert panelů, den starých, se statusem 3.
  -- Insert panelů z tabulky crp_missing. Tam se dávají panely ručně, ty, které musím z nějakého důvodu zpracovat. 
  -- Insert panelů pro owner=51 ve stavu 1. Nějaká výjimka.
  INSERT INTO 
    rel_pub.crp_geom_panel
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
    (c.stamp > CURRENT_DATE - 1 AND c.status = 3 AND c.timeattid = 0 AND c.ownid = c.subownid)
    OR a.pid IN(SELECT pid FROM rel_pub.crp_missing)
  ;

  -- INSERT panelů, které byly poslední den "oddemolovány".
  INSERT INTO 
    rel_pub.crp_geom_panel
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
    (c.status = 3 AND a.pid IN (SELECT pid FROM pnl.pnl_timeatts t WHERE t.stamp > CURRENT_DATE - 1 AND t.timeattid = 0))
  ;

--** Tady je NOTICE **--	
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Updating last_vacid, err_vac_res.';

  --Doupdate posledního vacid k panelům a případné chyby, pokud při výpočtu panelu vznikla
  UPDATE
    rel_pub.crp_geom_panel x
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
      vai.vai_vac_res v JOIN rel_pub.crp_geom_panel p ON v.mid = p.mid
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
    rel_pub.crp_geom_panel x
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

  -- Zaznamenám panely, které nemají napočítáno nebo skončily s chybou, ty se, mimo jiné, nedostanou do výstupu
  -- V tabulce rel_pub.crp_no_calc_inc se tyto panely budou kumulovat. 
  -- Postupně se pak musí spočítat a následně zpracovat.
  -- Vymažu již zpracované, ty co se mají ignorovat tam nechávám na věčnou památku a taky abych si je pamatoval na příště.
--** Tady je NOTICE **--	
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Deleting processed panels from crp_no_calc_inc.';
  DELETE FROM rel_pub.crp_no_calc_inc WHERE processed = TRUE AND ignore = FALSE;

--** Tady je NOTICE **--	
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Inserting panels to from crp_no_calc_inc. (vac_all_1 = 0 OR vac_all_1 IS NULL OR err_vac_res IS NOT NULL)';
  INSERT INTO rel_pub.crp_no_calc_inc (pid, mid, err_vac_res)
  SELECT 
    pid,
    mid,
    err_vac_res
  FROM 
    rel_pub.crp_geom_panel 
  WHERE
    (vac_all_1 = 0 OR vac_all_1 IS NULL OR err_vac_res IS NOT NULL)
    AND pid NOT IN (SELECT pid FROM rel_pub.crp_no_calc_inc) -- Vkládám jen ty, co v tabulce ještě nejsou.
  ;
  -- A vyřadím je z dalších akcí.
  DELETE FROM rel_pub.crp_geom_panel WHERE pid IN (SELECT pid FROM rel_pub.crp_no_calc_inc/* WHERE calculated = FALSE*/);

  -- Doupdatnu errory, protože se mohly změnit i u těch záznamů, které už tam (crp_no_calc_inc) jsou.
  UPDATE rel_pub.crp_no_calc_inc n SET err_vac_res = p.err_vac_res FROM rel_pub.crp_geom_panel p WHERE n.pid = p.pid;

  ---------------------------------------------------------------------------------------------------------------------
  -- Update ocenění (náročnosti) výpočtu v crp_no_calc_inc
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Calculation of panels calc cost.';

  WITH TargetMids AS (
    -- Základní sada panelů
    SELECT n.pid, n.mid 
    FROM rel_pub.crp_no_calc_inc n
    WHERE 
      1 = 1
      AND n.in_queue = FALSE
  ),
  CleanedSegments AS (
    -- Výběr unikátních segmentů s vyřazením linetype ('70','71','72')
    SELECT DISTINCT ON (s.mid, s.navteqid, s.orient)
        tm.pid,
        s.mid,
        s.navteqid,
        s.orient,
        ROUND(s.x1::numeric, 6) as x1, ROUND(s.y1::numeric, 6) as y1,
        ROUND(s.x2::numeric, 6) as x2, ROUND(s.y2::numeric, 6) as y2
    FROM vai.vai_seg s
    INNER JOIN TargetMids tm ON s.mid = tm.mid
    -- Přidání JOINu pro filtraci linetype (Comment, BALT zatím nemá)
    /*INNER JOIN topo.topo_streets_pli b ON s.navteqid = b.link_id AND s.navteqver = b.navteqver
    WHERE 
        b.linetype NOT IN ('70', '71', '72')*/
  ),
  UniqueNodes AS (
    SELECT mid, x1 AS x, y1 AS y FROM CleanedSegments
    UNION
    SELECT mid, x2 AS x, y2 AS y FROM CleanedSegments
  ),
  Stats AS (
    SELECT 
        c.pid,
        c.mid,
        COUNT(*)::float AS total_segments,
        (SELECT COUNT(*)::float FROM UniqueNodes u WHERE u.mid = c.mid) AS total_nodes
    FROM CleanedSegments c
    GROUP BY c.pid, c.mid
  ),
  CalculatedResults AS (
    SELECT 
        pid,
        mid,
        total_segments,
        total_nodes,
        ROUND((total_segments / NULLIF(total_nodes, 0))::numeric, 3) AS complexity_index
    FROM Stats
  )
  UPDATE
  	rel_pub.crp_no_calc_inc n
  SET 
  	complexity_index = sqry.complexity_index,
  	routing_difficulty = sqry.routing_difficulty
  FROM
  (
    SELECT 
        pid,
        mid,
        total_segments,
        total_nodes,
        complexity_index,
        CASE 
            WHEN complexity_index < 1.5 THEN 'LOW (Linear/Simple)'
            WHEN complexity_index BETWEEN 1.5 AND 2.2 THEN 'MEDIUM (Standard)'
            WHEN complexity_index > 2.2 THEN 'HIGH (Complex Mesh - slow)'
            ELSE 'UNDEFINED'
        END AS routing_difficulty
    FROM CalculatedResults
  ) sqry
  WHERE
  n.pid = sqry.pid;

  --------------------------------------
  -- Přesun problematických do tabulky problematických (rel_pub.crp_pnl_to_revise) = insert -> delete
  -- INSERT
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' INSERT of costly panels to table crp_pnl_to_revise and DELETE from crp_no_calc_inc.';

  INSERT INTO 
    rel_pub.crp_pnl_to_revise
  (
    pid,
    mid,
    err_vac_res,
    complexity_index,
    routing_difficulty
  )
  SELECT
    pid,
    mid,
    err_vac_res,
    complexity_index,
    routing_difficulty
  FROM
    rel_pub.crp_no_calc_inc n
  WHERE
    n.in_queue = FALSE
    AND n.ignore = FALSE
    --AND (n.complexity_index > 2.2 OR n.err_vac_res IS NOT NULL)
    AND n.err_vac_res IS NOT NULL
    AND pid NOT IN (SELECT pid FROM rel_pub.crp_pnl_to_revise) -- Jen ty, co tam ještě nejsou.
  ;

  -- DELETE
  DELETE FROM
    rel_pub.crp_no_calc_inc n
  WHERE
    n.in_queue = FALSE
    --AND (n.complexity_index > 2.2 OR n.err_vac_res IS NOT NULL)
    AND n.err_vac_res IS NOT NULL
  ;

  ---------------------------------------------------------------------------------------------------------------------

  -- Postup pro nespočítané panely
  -- V tabulce crp_no_calc_inc jsou 4 sloupce
  -- - in_queue = zařazeno v oper_vac_queue k výpočtu
  -- - calculated = panel je spočítám, může se zpracovat. Kromě těch, co mají v počítání chybu v err_vac_res
  -- - processed = panel zprocesován
  -- - ignore = panel se zcela ignoruje, např. pid = 534, který má ve výpčtu vždy = 0
  -- Princip je, že nespočítané panely ze crp_no_calc_inc vložím do strojku (oper_vac_queue, table = 50020) a zaškrtnu in_queue
  -- Před tímto skriptem se pustí jiný skript, který zkontroluje panely ve frontě a zaškrtne jim calculated
  -- To způsobí, že se panely dostanou do zpracování a to tak, že se do tabulky crp_missing zapíší všechny panely
  -- ze crp_no_calc_inc, které mají zaškrtnuto in_queue a calculated, ale processed = false (krom chyby v err_vac_res)
  -- Panelům se zaškrtne processed, tím jsou hotový, tímhle skriptem zpravovaný

  -- Začátek propočítávání panelů -------------------------
  -- Vložím do queue

  INSERT INTO oper.oper_vac_queue (vacid,faceid,mid,usrn,stamp_in_queue,priority,rmloadid,temp_table)
  SELECT 0, sqry.faceid, sqry.mid, 'init_continous_release', CURRENT_TIMESTAMP, 1, 0, 50820 FROM
    ( 
      SELECT DISTINCT ON (pid)
        p.pid,
        p.mid,
        f.faceid
      FROM
        rel_pub.crp_no_calc_inc z
        JOIN pnl.pnl_main p ON z.pid = p.pid
        JOIN pnl.pnl_facemid f ON p.mid = f.mid
      WHERE
        z.pid IN(SELECT pid FROM rel_pub.crp_no_calc_inc WHERE in_queue = FALSE AND ignore = FALSE AND err_vac_res IS NULL ORDER BY pid)
      ORDER BY p.pid, p.mid DESC
    )sqry
  ;

  -- Označím vložené do queue
  UPDATE
    rel_pub.crp_no_calc_inc
  SET
    in_queue = TRUE
  WHERE 
    in_queue = FALSE  
  ;

  -- Když jsou panely spočítané, nahodí se jim příznak "calculated" ....
  UPDATE rel_pub.crp_no_calc_inc nc
    SET calculated = TRUE
  WHERE
    nc.pid IN(
    SELECT DISTINCT 
      p.pid 
    FROM 
      oper.oper_vac_queue q
      JOIN pnl.pnl_main p ON q.mid = p.mid
      JOIN rel_pub.crp_no_calc_inc n ON p.pid = n.pid
    WHERE
      q.usrn = 'init_continous_release'
      AND q.temp_table = 50820
      AND q.stamp_end IS NOT NULL
      AND n.err_vac_res IS NULL
      AND n.ignore = FALSE
      AND q.stamp_in_queue > CURRENT_DATE - 4
      AND n.processed = FALSE
    )
  ;

  --  .... a zařadí se do zpracování.
  INSERT INTO 
    rel_pub.crp_geom_panel
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
    LEFT JOIN rel_pub.crp_geom_panel d ON a.pid = d.pid
  WHERE 
    a.pid IN(SELECT pid FROM rel_pub.crp_no_calc_inc WHERE in_queue = TRUE AND calculated = TRUE AND processed = FALSE AND ignore = FALSE)
    AND d.pid IS NULL
  ;

  -- Teď jsem je zařadil ke zpracování a nahodím jim "processed".
  -- Označím vložené do crp_geom_panel
  UPDATE
    rel_pub.crp_no_calc_inc
  SET
    processed = TRUE
  WHERE 
    calculated = TRUE 
    AND processed = FALSE
    AND ignore = FALSE 
  ;

  -- Konec propočítávání panelů ---------------------------

  -- Odtranění duplicit v seznamu panelů.
  TRUNCATE TABLE rel_pub.crp_geom_panel_temp;
  INSERT INTO rel_pub.crp_geom_panel_temp SELECT DISTINCT * FROM rel_pub.crp_geom_panel;
  TRUNCATE TABLE rel_pub.crp_geom_panel;
  INSERT INTO rel_pub.crp_geom_panel SELECT * FROM rel_pub.crp_geom_panel_temp;

  -- Počet panelů ke zpracování, použije se v IF níže pro test, jestli je co zpracovat, jinak se to skočí.
  SELECT COUNT(*) INTO i_cnt FROM rel_pub.crp_geom_panel;

--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Panels to process:' || i_cnt;

--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Creating unpublish list.';
  -------------------------------------------------------------------------------------------------------------------------
  -- Unpublish Start ------------------------------------------------------------------------------------------------------
  -- Všechny panely k odpublikování se nejdříve načtou do tabulky crp_unpublish_list
  -- Z publish smažu, je když nemám debug mód.
  -- Přesunuto z hlavního IFu fce, musí se spustit i když se nenajdou žádné panely CURRENT_TIME -1

  -- Nejdříve promáznu seznam od posledně
  TRUNCATE rel_pub.crp_unpublish_list;

  -- Z publishe smáznu panely se změněným statusem a zdemolované, abych si pak 
  -- nesmáznul ty, který jsem zpracoval.
  -- Odpublikování Status 1, 2, 4
  INSERT INTO 
    rel_pub.crp_unpublish_list
  (
    mid,
    pid,
    desc_text
  )
  SELECT
    m.pid,
    m.pid,
    'Unpublished - Status changed to 1, 2 or 4'
  FROM
    pnl.mv_pnl_main_maxdate m
  WHERE 
    m.stamp > CURRENT_DATE - 1
    AND m.status IN(1, 2, 4)
  ;

  -- Přidám ty, co nám mohly v minulosti proklouznout
  INSERT INTO 
    rel_pub.crp_unpublish_list
  (
    pid,
    desc_text
  )
  SELECT mx.pid, 'Unpublished - demolished, past cases' FROM pnl.mv_pnl_main_maxdate mx
  WHERE 
    mx.pid IN (SELECT DISTINCT x.pid
               FROM
                (SELECT pid, stamp, status, timeattid 
                  FROM pnl.mv_pnl_main_maxdate 
                  WHERE 
                  (status = 3 AND timeattid = 0)
                ) m
                RIGHT JOIN 
                (SELECT DISTINCT pid 
                  FROM res.res_release_publish 
                  WHERE rvloadid IN(100, 101)) x ON m.pid = x.pid
              WHERE
                m.pid IS NULL
      						
  )
  AND mx.timeattid <> 0
  AND mx.stamp <= CURRENT_DATE - 1
  ORDER BY
    mx.pid
  ;

  -- Přidám zdemolované na seznam
  INSERT INTO 
    rel_pub.crp_unpublish_list
  (
    pid,
    desc_text
  )
  SELECT 
    pid,
    'Unpublish - demolished' 
  FROM 
    pnl.pnl_timeatts t 
  WHERE 
    t.stamp > CURRENT_DATE - 1
    AND t.timeattid <> 0
  ;

  -- Smažu pidy které jdou do zpracování
  DELETE FROM rel_pub.crp_unpublish_list u
  USING
    (
      SELECT 
        pid
      FROM 
        rel_pub.crp_geom_panel
    ) sqry
  WHERE u.pid = sqry.pid
  ;

  -- Odpublikování podle seznamu v crp_unpublish_list
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Unpublish by the list in crp_unpublish_list table.';

  DELETE FROM res.res_release_publish p
  USING
    (
      SELECT 
        pid 
      FROM 
        rel_pub.crp_unpublish_list
    ) sqry
  WHERE p.pid = sqry.pid AND p.rvloadid IN (100, 101)
  ;

--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Logging unpublished panel list.';
  -- A ještě evidence / log
  INSERT INTO 
    rel_pub.crp_geom_panel_log
    (
      mid,
      pid,
      desc_text
    )
    SELECT 
      mid,
      pid,
      desc_text
    FROM 
      rel_pub.crp_unpublish_list
  ;
  -- Unpublish Konec -----------------------------------------------------------------------------------------------------


  -- Panely ze crp_missing jsou vloženy pro výpočet a použity pro unpublish, možno smazat.
  TRUNCATE rel_pub.crp_missing;

  -- Hlavní zpracování, výpočty ------------------------------------------------------------------------------------------
  IF i_cnt > 0 THEN
    -- Založí se nový load a sosne se jeho id do proměnné load_id.
    INSERT INTO rel_pub.etl_ccf_rel_pub_loads_inc ("current_user")
    VALUES (CURRENT_USER);

    SELECT load_id INTO last_load_id FROM rel_pub.etl_ccf_rel_pub_loads_inc ORDER BY load_id DESC;
        
--** Tady je NOTICE **--
		RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' New load_id:' || last_load_id;

--** Tady je NOTICE **--
		RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Recs found:' || i_cnt;

  
--** Tady je NOTICE **--
  	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Bad navteq management.';

    -- Test na správný navteq version.
    -- Pokud má panel špatný navteqver, uloží se záznam do tabulky crp_bad_navteq a vyhodí z počítání.
    -- Nejdříve smažeme panely zprocesované posledně
    DELETE FROM rel_pub.crp_bad_navteq WHERE processed = 1;

    -- Zkontroluji a uložim špatný navteqver na seznam
    INSERT INTO rel_pub.crp_bad_navteq
    (
      mid,
      pid,
      navteqver
    )
    SELECT
      a.mid,
      a.pid,
      a.navteqver
    FROM
      pnl.pnl_main a
      INNER JOIN rel_pub.crp_geom_panel b ON a.mid = b.mid
    WHERE
      a.navteqver <> navteqver_str
    ;
      
    -- Špatný navteq se nebude počítat
    DELETE FROM rel_pub.crp_geom_panel a
    WHERE a.mid IN (SELECT mid FROM rel_pub.crp_bad_navteq);
  	
    -- Potom se označí opravené pidy
    UPDATE 
      rel_pub.crp_bad_navteq 
    SET 
      processed = 1
    WHERE 
      pid IN
      (SELECT z.pid 
        FROM rel_pub.crp_bad_navteq z 
        JOIN pnl.pnl_main p ON z.pid = p.pid 
        WHERE p.navteqver = navteqver_str
      )
      AND processed = 0;
    
    -- A opravené se vrátí/nově vloží na seznam
    INSERT INTO 
      rel_pub.crp_geom_panel
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
      a.pid IN(SELECT pid FROM rel_pub.crp_bad_navteq WHERE processed = 1)
    ;	

--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Creating records for crp_ims_input_inc.';
      	
    -- Zde se vytvoří crp_ims_input_inc (původně ims_input) pro nalezené změněné panely
    PERFORM rel_pub.create_ims_input_inc();
    
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Deleting record from cache (ids_vac_calc_campaign_cache2).';
		---------------------------------------------------------
    -- Odlogujeme mazané záznamy
    INSERT INTO rel_pub.crp_cache_del_log (
        rvloadid, faceid, spot_length, spot_pause, vacid, facepid, 
        vac, rots, vac_orig, rots_orig, group_def, period_hours
    )
    SELECT 
        rvloadid, faceid, spot_length, spot_pause, vacid, facepid, 
        vac, rots, vac_orig, rots_orig, group_def, period_hours
    FROM ids.ids_vac_calc_campaign_cache2
    WHERE facepid IN (SELECT facepid FROM rel_pub.crp_ims_input_inc WHERE pnlmotion = 'D') 
      AND rvloadid = 100;
      
    -- Smažeme záznamy z cache / zatím pokusná tabulka ve vrf
    DELETE FROM ids.ids_vac_calc_campaign_cache2 
		WHERE facepid IN(SELECT facepid FROM rel_pub.crp_ims_input_inc WHERE pnlmotion = 'D') AND rvloadid = 100;


    ---------------------------------------------------------
    -- Prusecik kolacu bufferu a tripu. obe tabulky by mely byt naindexovane. panely o dotaz vyse, tripy od RM

--** Tady je NOTICE **--	
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Calculating intersection.';

    -- Výpočet všech intersekcí, prusecik kolacu bufferu a tripu.
    TRUNCATE rel_pub.crp_intersection;
    INSERT INTO rel_pub.crp_intersection
      (mid, sid, week_day_name, week_day_type)
    SELECT
      a.mid, b.sid, b.week_day_name, b.week_day_type
    FROM
      rel_pub.crp_geom_panel a
      INNER JOIN rel_pub.crp_routing b 
        ON a.buffer_geom && b.geom       					-- Indexový test obálek
        AND ST_Intersects(a.buffer_geom, b.geom)	-- Přesný prostorový test
    ;
    	
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Updating intersection type in panel list (crp_geom_panel).';
      
    --Doupdate 'ORIGINAL' a 'SUBSTITUTE' podle spočítaných intersekcí.
    --Panely, které nemají v crp_intersection_inc záznam se musí dohledat (níže).
    --Doupdate 'ORIGINAL'
    UPDATE rel_pub.crp_geom_panel
    SET intersection_type = 'ORIGINAL'
    WHERE mid IN(SELECT DISTINCT mid FROM rel_pub.crp_intersection);

    --Doupdate 'SUBSTITUTE'
    UPDATE rel_pub.crp_geom_panel
    SET intersection_type = 'SUBSTITUTE'
    WHERE intersection_type IS NULL;
    	
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Filling crp_evidence from crp_intersection (ORIGINAL).';

    --Celkový seznam, naplnění tabulky, nejprve ty, co mají průsečík s trasou, pak se sem pak přidají panely bez průsečíku
    TRUNCATE rel_pub.crp_evidence;
    INSERT INTO rel_pub.crp_evidence
  		(mid, sid, week_day_name, week_day_type)
    SELECT mid, sid, week_day_name, week_day_type
    FROM rel_pub.crp_intersection;

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
      vac_all_1
    FROM
      rel_pub.crp_geom_panel 
    WHERE intersection_type = 'SUBSTITUTE';
	
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Finding nearest panels for panels wo intersection (best of 20).';

    --Nalezeni nejblizsiho MIDu (pro vsechny verze tripu) a doplneni sidu z tech tripu do evidence delanych midu
    TRUNCATE TABLE rel_pub.crp_nearest20;
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
            ) as rn_geom,  -- Pořadí podle geometrické vzdálenosti
            t1.pid AS pid_near
        FROM
            rel_pub.crp_panel_wo_its t2,
            rel_pub.crp_all_panel t1 -- Používáme implicitní JOIN pro KNN dotaz
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
    INSERT INTO rel_pub.crp_nearest20
    SELECT
        bm.mid_woi,
        bm.vac_woi,
        bm.mid_near AS best_mid_near,
        bm.vac_near AS best_vac_near,
        bm.distance AS geometric_distance,
        bm.value_difference,
        bm.pid_near AS best_pid_near
    FROM
        best_match bm
    WHERE
        bm.rn_value = 1;

		--**--!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

  	
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Filling substituted panels';
    
    --Tady je to trochu zašmodrchaný, nejdříve update the_geom a buffer_geom podle near midů (best_mid_near)
    UPDATE rel_pub.crp_nearest20 n
    SET 
      the_geom = sqry.the_geom,
      buffer_geom = sqry.buffer_geom
    FROM (
      SELECT
        a.mid,
        a.pid,
        b.the_geom,
        geometry(st_buffer(geography(b.the_geom),a.maxdist + 200)) AS buffer_geom,
        a.maxdist,
        c.mid_woi
      FROM
        pnl.pnl_main a
        INNER JOIN pnl.pnl_geom b ON a.mid = b.mid
        INNER JOIN rel_pub.crp_nearest20 c ON a.mid = c.best_mid_near
      ) sqry
      WHERE n.mid_woi = sqry.mid_woi
    ;
    
    -- Doplním do crp_intersection, geom mám z best nearest, tak se najdou průsečíky,
    -- ale mid je z toho neprotnutýho panelu (mid_woi - mid without intersection)
    INSERT INTO rel_pub.crp_intersection
    SELECT
      a.mid_woi, b.sid
    FROM
      rel_pub.crp_nearest20 a
      INNER JOIN rel_pub.crp_routing b ON st_intersects(a.buffer_geom, b.geom) AND a.buffer_geom && b.geom
    ;
    
    INSERT INTO rel_pub.crp_evidence  
    SELECT 
      mid_woi,
      sid
    FROM 
      rel_pub.crp_nearest20 a
      JOIN rel_pub.crp_intersection b ON a.mid_woi = b.mid
    ;
    -- Teď mám v crp_intersection vložený i panely bez kontaktů, nefejkovaný podle panelů, který kontakt mají, 
    -- jsou blízko a mají podobný hodnoty
    
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Filling panels to heap of all panels (crp_all_panel)';
  		
    -- UPSERT: Aktualizujte existující řádky v crp_all_panel daty z crp_geom_panel,
    -- a přidá ty, které chybí, vč. vac, vacid, intersection_type..
    UPDATE rel_pub.crp_all_panel AS a
    SET
        mid = b.mid,
        the_geom = b.the_geom,
        buffer_geom = b.buffer_geom,
        vac_all_1 = b.vac_all_1,
        maxdist = b.maxdist,
        intersection_type = b.intersection_type,
        last_vacid = b.last_vacid
    FROM
        rel_pub.crp_geom_panel AS b
    WHERE
        a.mid = b.mid;

    INSERT INTO rel_pub.crp_all_panel
    SELECT
        b.mid,
        b.pid,
        b.the_geom,
        b.buffer_geom,
        b.vac_all_1,
        b.maxdist,
        b.intersection_type,
        b.last_vacid
    FROM
        rel_pub.crp_geom_panel AS b
    LEFT JOIN
        rel_pub.crp_all_panel AS a ON b.mid = a.mid
    WHERE
        a.mid IS NULL; -- Klíč z TABLE_A je NULL, pokud řádek nebyl nalezen

--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' End of panel preparation.';

--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Creating ccf_output.';

    TRUNCATE TABLE rel_pub.crp_ts_rots_temp;
    TRUNCATE TABLE rel_pub.crp_ccf_output;
    
    -- Optimalizováno, místo původních 4 dotazů na jeden, všechny tripy vloženy do crp_routing
    INSERT INTO rel_pub.crp_ccf_output  (pid, sid, respweight, modality, dayofweek, respondent_id, ts_rots, facepid, faceid)
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
      rel_pub.crp_evidence e
      JOIN rel_pub.crp_routing rou ON e.sid = rou.sid
      JOIN pnl.pnl_main m ON e.mid = m.mid
      JOIN pnl.pnl_facemid f ON e.mid = f.mid
      JOIN pnl.pnl_faces f2 ON f.facepid = f2.facepid
    	JOIN rel_pub.crp_resp_weight_input rwi ON rou.respondent_id = rwi.respondent_id
      JOIN rel_pub.crp_freq_trips_vector ftv ON rou.week_day_type = ftv.weekdaytype
    WHERE 
      rou.trip_freq > ftv.freq_from
      AND rou.trip_freq <= ftv.freq_to
      AND rou.week_day_type = ftv.weekdaytype
      AND(m.pnlmotion <> 'D' OR (m.pnlmotion = 'D' AND f2.pos = 1))
    ;

    -- Updatne weekDay
    --UPDATE rel_pub.crp_ccf_output SET dayofweek = LEFT(dayofweek,2);
    	
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Trips filled to crp_ccf_output.';
  	
    -- Naplní crp_ts_rots_temp
  INSERT INTO rel_pub.crp_ts_rots_temp (pid, faceid, ts_rots)
  SELECT pid, faceid, SUM(ts_rots)
  FROM rel_pub.crp_ccf_output
  GROUP BY pid, faceid;

--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Table crp_ts_rots_temp filled.';
  
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Column ims_rots filled.';
  
  -- Updatne ims_performance
	UPDATE rel_pub.crp_ts_rots_temp trt
  SET 
  	ims_rots = it.rots_week_all,
    ims_vac  = it.vac_week_all,
    ims_va   = it.vac_week_all::DOUBLE PRECISION / it.rots_week_all
  FROM rel_pub.crp_ims_input_inc AS it 
  WHERE trt.faceid = it.faceid_db;
  
  UPDATE rel_pub.crp_ts_rots_temp trt
	SET ts_va = ims_vac::DOUBLE PRECISION / ts_rots;
  
  UPDATE rel_pub.crp_ts_rots_temp trt
	SET 
  	ts_freq = 
    	CASE WHEN ts_va > 1 
      	THEN FLOOR(ts_va) + 1
      	ELSE 1
      END
  ;
  
  UPDATE rel_pub.crp_ts_rots_temp trt
	SET ts_va_adj = ts_va::DOUBLE PRECISION / ts_freq;
    
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Table crp_ts_rots_temp updated.';
  
  -- Updatne VA a VAC v crp_ccf_output
  UPDATE rel_pub.crp_ccf_output c
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
  FROM rel_pub.crp_ts_rots_temp AS trt
  WHERE c.faceid = trt.faceid;
  
  --TRUNCATE TABLE rel_pub.crp_control_sum;
  INSERT INTO rel_pub.crp_control_sum
  SELECT
  	pid,
    faceid,
    SUM(ts_rots) AS sum_of_ts_rots,
    SUM(vac) AS sum_of_ts_vac,
    last_load_id,
    'INCR'
  FROM rel_pub.crp_ccf_output c
  GROUP BY pid, faceid;
  
--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Table crp_ccf_output updated.';
	
  -- Final CCF load
--** Tady je NOTICE **--
	  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Final CCF load.';

    exe_str = 'INSERT INTO 
                  rel_pub.etl_ccf_rel_pub_inc
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
                    cgp.mid, 
                    cco.facepid, 
                    cco.faceid,
                    cgp.last_vacid,
                    cco.respondent_id,
                    LEFT(cco.respondent_id, 3) AS country, 
                    LOWER(cco.dayofweek), 
                    cco.respweight,
                    cco.ts_rots,
                    cco.va, 
                    round(cco.vac), ' || last_load_id || '
                  FROM rel_pub.crp_ccf_output cco
                    JOIN rel_pub.crp_geom_panel cgp ON cco.pid = cgp.pid
                    ; ';
    
    
    EXECUTE exe_str;
    
    -- Ošetření NULL hodnot ve výsledku
    -- Nejdřív uklidim panely, co mají NULL do tabulky k revizi
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Treating NULL values ​​in the result.';

    INSERT INTO 
      rel_pub.crp_pnl_to_revise
    (
      pid,
      mid,
      load_id,
      va,
      vac,
      null_zero_result
    )
    SELECT DISTINCT
      pid,
      mid,
      load_id,
      va,
      vac,
      TRUE
    FROM 
      rel_pub.etl_ccf_rel_pub_inc 
    WHERE
      (va IS NULL OR vac IS NULL OR va < 0 OR vac < 0)
      AND pid NOT IN (SELECT pid FROM rel_pub.crp_pnl_to_revise WHERE load_id IS NOT NULL) -- Jen ty, co tam ještě nejsou.
    ;

    GET DIAGNOSTICS i_cnt = ROW_COUNT;
    
    -- Pak je smažu z outputu
    -- DELETE
    DELETE FROM
      rel_pub.etl_ccf_rel_pub_inc
    WHERE
      (va IS NULL OR vac IS NULL OR va < 0 OR vac < 0)
    ;
  
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Treating NULL values done. Affected rows: ' || i_cnt;

--** Tady je NOTICE **--
		RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Publishing.';
    -- Automatický publish --------------------------------------------------------------------------------------------------
    -- Delete publishe, pokud panely už mají publikované výsledky
    DELETE FROM 
      res.res_release_publish
    WHERE
      pid IN(SELECT pid FROM rel_pub.crp_ims_input_inc)
      AND rvloadid IN(100, 101)
    ;
      
    -- Continual Release IDS
    INSERT INTO res.res_release_publish 
    SELECT DISTINCT ON (sqry.facepid, sqry.transptype)
      100::integer as rvloadid, --!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      sqry.facepid,
      sqry.vacid,
      sqry.selsetid,
      sqry.dayicid,
      sqry.hourcid,
      sqry.fdurid,
      sqry.periodid,
      sqry.modeltype,
      sqry.transptype,
      sqry.rminvacver,
      sqry.vac,
      sqry.rots,
      sqry.va,
      sqry.pop_total,
      sqry.grp_vac_total,
      sqry.grp_rots_total,
      sqry.pid,
      sqry.mid,
      sqry.faceid
    FROM (
      SELECT DISTINCT
        fm.facepid,
        r.*,
        f.pid,
        fm.mid,
        fm.faceid,
        f.pos,
        v.pnlmotion
      FROM 
        res.res_online_ea_output_round r
        JOIN (SELECT DISTINCT ON (vvr.faceid)
                vvr.vacid,
                vvr.faceid,
                vvr.mid,
                vvr.err_vac_res,
              	nm.pnlmotion
              FROM 
                rel_pub.crp_ims_input_inc nm
                JOIN pnl.pnl_facemid fm ON nm.mid = fm.mid
                JOIN vai.vai_vac_res vvr ON fm.faceid = vvr.faceid
              ORDER BY  
                vvr.faceid,
                vvr.vacid DESC
              ) v ON r.vacid = v.vacid
        JOIN pnl.pnl_facemid fm ON v.faceid = fm.faceid
        JOIN pnl.pnl_faces f ON fm.facepid = f.facepid
      WHERE 
        r.periodid = 1
    )sqry
    WHERE 
  		1 = 1
      AND(sqry.pnlmotion <> 'D' OR (sqry.pnlmotion = 'D' AND sqry.pos = 1))
    ORDER BY
      sqry.facepid, sqry.transptype, sqry.vacid DESC
    ;
		
    GET DIAGNOSTICS i_cnt = ROW_COUNT;
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Records published for rvload_id = 100. Affected rows: ' || i_cnt;
    
    -- Continual Release IMS
    INSERT INTO res.res_release_publish 
    SELECT DISTINCT ON (sqry.facepid, sqry.transptype, sqry.periodid)
      101::integer as rvloadid, --!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      sqry.facepid,
      sqry.vacid,
      sqry.selsetid,
      sqry.dayicid,
      sqry.hourcid,
      sqry.fdurid,
      sqry.periodid,
      sqry.modeltype,
      sqry.transptype,
      sqry.rminvacver,
      sqry.vac,
      sqry.rots,
      sqry.va,
      sqry.pop_total,
      sqry.grp_vac_total,
      sqry.grp_rots_total,
      sqry.pid,
      sqry.mid,
      sqry.faceid
    FROM (
      SELECT DISTINCT
        fm.facepid,
        r.*,
        f.pid,
        fm.mid,
        fm.faceid,
        f.pos,
        v.pnlmotion
      FROM 
        res.res_online_ea_output_round r
        JOIN (SELECT DISTINCT ON (vvr.faceid)
                vvr.vacid,
                vvr.faceid,
                vvr.mid,
                vvr.err_vac_res,
              	nm.pnlmotion
              FROM 
                rel_pub.crp_ims_input_inc nm
                JOIN pnl.pnl_facemid fm ON nm.mid = fm.mid
                JOIN vai.vai_vac_res vvr ON fm.faceid = vvr.faceid
              ORDER BY  
                vvr.faceid,
                vvr.vacid DESC
              ) v ON r.vacid = v.vacid
        JOIN pnl.pnl_facemid fm ON v.faceid = fm.faceid
        JOIN pnl.pnl_faces f ON fm.facepid = f.facepid
      WHERE 
        r.periodid = 200
    )sqry
    WHERE 
      1 = 1
      AND(sqry.pnlmotion <> 'D' OR (sqry.pnlmotion = 'D' AND sqry.pos = 1))
    ORDER BY
      sqry.facepid, sqry.transptype, sqry.periodid, sqry.vacid DESC
    ;

    GET DIAGNOSTICS i_cnt = ROW_COUNT;
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Records published for rvload_id = 101. Affected rows: ' || i_cnt;
    
    -- Evidence / log zpracovaných panelů v tomto loadu
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
      rel_pub.crp_geom_panel
    ;
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Publishing end.';
  	
	  -- Znovu se načte, některé panely mohly být vyřazeny
    SELECT COUNT(*) INTO i_cnt FROM rel_pub.crp_geom_panel;

--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Load ID: ' || last_load_id || ' - successfuly done. Num of recs: ' || i_cnt || '.';
    PERFORM rel_pub.log_msg('INFO', 'fce:auto_rel_pub_inc', 'Load ID: ' || last_load_id || ' - successfuly done. Num of recs: ' || i_cnt || '.');

----------------------------------------------------------------------------------------------------------------------------------
		-- Nové, cache - test režim
--** Tady je NOTICE **--
		RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Perform cache filling, just temp table.';
  	
		TRUNCATE rel_pub.crp_spotplans;

    INSERT INTO rel_pub.crp_spotplans
    SELECT a.rvloadid, a.spot_length, a.spot_pause 
    FROM ids.ids_vac_calc_campaign_cache2 a 
    WHERE a.rvloadid = 100
    GROUP BY a.rvloadid, a.spot_length, a.spot_pause
    ORDER BY a.spot_length, a.spot_pause
    ;

		PERFORM rel_pub.f_fill_crp_cache_temp();

--** Tady je NOTICE **--    
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Cache filling end.';
    
--** Tady je NOTICE **--    
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Copying data from temp table to cache table.';
    INSERT INTO ids.ids_vac_calc_campaign_cache2 SELECT * FROM rel_pub.crp_cache_temp;
--** Tady je NOTICE **--    
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Data copying end.';    
   
---------------------------------------------------------------------------------------------------------------------------------
	ELSE
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Recs(panels) not found, exiting.';
    PERFORM rel_pub.log_msg('INFO', 'fce:auto_rel_pub_inc', 'Load did not run, no panel records.');
  END IF; -- IF i_cnt >

--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' The end.';

  EXCEPTION
  WHEN others THEN
    RAISE NOTICE 'exception: %', SQLERRM;
    PERFORM rel_pub.log_msg('ERROR', 'auto_rel_pub_inc', 'Error occured: ' || SQLERRM);

END;
$$;


--
-- Name: create_ims_input_ccf(); Type: FUNCTION; Schema: rel_pub; Owner: -
--

CREATE FUNCTION create_ims_input_ccf() RETURNS void
    LANGUAGE plpgsql
    AS $$
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
    pnlvaienv,
    pnlmotion
  )
  SELECT 
      a.pid,
      a.mid,
      f.facepid,
      f.face_userid,
      v.vacid,
      f.faceid,
      v.err_vac_res,
      s.pnlvaienv,
      a.pnlmotion
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
$$;


--
-- Name: create_ims_input_inc(); Type: FUNCTION; Schema: rel_pub; Owner: -
--

CREATE FUNCTION create_ims_input_inc() RETURNS void
    LANGUAGE plpgsql
    AS $$
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
    err_vac_res,
    pnlvaienv,
    pnlmotion
  )
  SELECT 
      a.pid,
      a.mid,
      f.facepid,
      f.face_userid,
      v.vacid,
      f.faceid,
      v.err_vac_res,
      s.pnlvaienv,
    	a.pnlmotion
    FROM 
      pnl.pnl_main a
      JOIN vai.vai_vac_res v ON v.mid = a.mid
      JOIN pnl.pnl_facemid f ON v.faceid = f.faceid
      JOIN pnl.pnl_faces f2 ON f.facepid = f2.facepid
      JOIN code.code_pnlsubtype s ON a.pnlsubtype = s.pnlsubtype
    WHERE 
      a.mid IN(SELECT mid FROM rel_pub.crp_geom_panel)
      AND(a.pnlmotion <> 'D' OR (a.pnlmotion = 'D' AND f2.pos = 1))
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
  -- VAC
  UPDATE rel_pub.crp_ims_input_inc a 
  SET 
  	vac_week_all = b.vac
	FROM 
  	res.res_online_ea_output_round b
	WHERE
    a.vacid = b.vacid AND
    b.periodid = 1 AND
    b.transptype = 'ALL'
  ;
	UPDATE rel_pub.crp_ims_input_inc a 
  SET 
  	vac_week_veh = b.vac
	FROM 
  	res.res_online_ea_output_round b
	WHERE
    a.vacid = b.vacid AND
    b.periodid = 1 AND
    b.transptype = 'VEH'
  ;
	UPDATE rel_pub.crp_ims_input_inc a 
  SET 
  	vac_week_pub = b.vac
	FROM 
  	res.res_online_ea_output_round b
	WHERE
    a.vacid = b.vacid AND
    b.periodid = 1 AND
    b.transptype = 'PUB'
  ;
	UPDATE rel_pub.crp_ims_input_inc a 
  SET 
  	vac_week_ped = b.vac
	FROM 
  	res.res_online_ea_output_round b
	WHERE
    a.vacid = b.vacid and
    b.periodid = 1 and
    b.transptype = 'PED'
  ;
	UPDATE rel_pub.crp_ims_input_inc a 
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
  UPDATE rel_pub.crp_ims_input_inc a 
  SET 
    rots_week_all = b.rots
  FROM 
    res.res_online_ea_output_round b
  WHERE
    a.vacid = b.vacid 
    AND b.periodid = 1 
    AND b.transptype = 'ALL'
  ;
  UPDATE rel_pub.crp_ims_input_inc a 
  SET 
    rots_week_veh = b.rots
  FROM 
    res.res_online_ea_output_round b
  WHERE	
    a.vacid = b.vacid 
    AND b.periodid = 1 
    AND b.transptype = 'VEH'
  ;
  UPDATE rel_pub.crp_ims_input_inc a 
  SET 
    rots_week_pub = b.rots
  FROM 
    res.res_online_ea_output_round b
  WHERE
    a.vacid = b.vacid 
    AND b.periodid = 1 
    AND b.transptype = 'PUB'
  ;UPDATE rel_pub.crp_ims_input_inc a 
  SET 
    rots_week_ped = b.rots
  FROM 
    res.res_online_ea_output_round b
  WHERE
    a.vacid = b.vacid 
    AND b.periodid = 1 
    AND b.transptype = 'PED'
  ;UPDATE rel_pub.crp_ims_input_inc a 
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
  UPDATE rel_pub.crp_ims_input_inc  
  SET 
    va_week_all = vac_week_all:: DOUBLE PRECISION/rots_week_all:: DOUBLE PRECISION
  WHERE 
    vac_week_all <> 0
    AND rots_week_all <> 0
  ;
  UPDATE rel_pub.crp_ims_input_inc 
  SET 
    va_week_veh = vac_week_veh:: DOUBLE PRECISION/rots_week_veh:: DOUBLE PRECISION
  WHERE 
    rots_week_veh <> 0
    AND rots_week_veh <> 0
  ;
  UPDATE rel_pub.crp_ims_input_inc  
  SET 
    va_week_pub = vac_week_pub :: DOUBLE PRECISION/rots_week_pub :: DOUBLE PRECISION
  WHERE 
    rots_week_pub <> 0
    AND rots_week_pub <> 0
  ;
	UPDATE rel_pub.crp_ims_input_inc  
  SET 
    va_week_ped = vac_week_ped :: DOUBLE PRECISION/rots_week_ped :: DOUBLE PRECISION
  WHERE 
    rots_week_ped <> 0
    AND rots_week_ped <> 0
  ;
	UPDATE rel_pub.crp_ims_input_inc  
  SET 
    va_week_bic = vac_week_bic :: DOUBLE PRECISION/rots_week_bic :: DOUBLE PRECISION
  WHERE 
    rots_week_bic <> 0
    AND rots_week_bic <> 0
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
$$;


--
-- Name: f_fill_crp_cache_temp(); Type: FUNCTION; Schema: rel_pub; Owner: -
--

CREATE FUNCTION f_fill_crp_cache_temp() RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
  cur_vacid    CURSOR FOR
    SELECT vacid
    FROM rel_pub.crp_ims_input_inc;

  cur_spotplan CURSOR FOR
    SELECT rvloadid, spot_length, spot_pause
    FROM rel_pub.crp_spotplans;

  v_vacid      integer;
  r_spotplan   rel_pub.crp_spotplans%ROWTYPE;
  v_inserted   integer;
  v_vacid_cnt  integer := 0;
  v_total_rows integer := 0;

BEGIN
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Function f_fill_crp_cache_temp start.';
  TRUNCATE TABLE rel_pub.crp_cache_temp;

  OPEN cur_vacid;
  LOOP
    FETCH cur_vacid INTO v_vacid;
    EXIT WHEN NOT FOUND;

    v_vacid_cnt := v_vacid_cnt + 1;

    OPEN cur_spotplan;
    LOOP
      FETCH cur_spotplan INTO r_spotplan;
      EXIT WHEN NOT FOUND;

      INSERT INTO rel_pub.crp_cache_temp (
        rvloadid, faceid, spot_length, spot_pause, vacid, facepid,
        vac, rots, vac_orig, rots_orig, group_def, period_hours
      )
      WITH tdynva AS (
        SELECT
          c.mid,
          c.facepid,
          b.faceid,
          b.vacid,
          a.fa_calc,
          a.rsid,
          a.route_va,
          a.transptype,
          vac8s._f_vac_calc_ids_dynvac_fepe(
            a.transptype,
            a.pnlmotion,
            a.route_kcd,
            a.route_va,
            r_spotplan.spot_length,
            r_spotplan.spot_length + r_spotplan.spot_pause,
            a.daf_angle
          ) AS dynva
        FROM vai_subl.vai_subl_route_plan_dynva a
          JOIN vai.vai_vac_res b ON (a.rsid = b.rsid)
          JOIN pnl.pnl_facemid c ON (b.faceid = c.faceid)
        WHERE
          a.pnlmotion = 'D'
          AND b.vacid = v_vacid
      )
      SELECT
        100::integer,
        max(t.faceid),
        r_spotplan.spot_length::double precision,
        r_spotplan.spot_pause::double precision,
        max(t.vacid),
        max(t.facepid),
        sum(t.dynva * t.fa_calc),
        round(CAST(sum(t.fa_calc) AS numeric), 0),
        sum(t.dynva * t.fa_calc),
        round(CAST(sum(t.fa_calc) AS numeric), 0),
        null::varchar,
        max(f.period_hours)
      FROM tdynva t
        JOIN res.res_online_ea_output e ON (e.vacid = t.vacid AND e.transptype = t.transptype)
        JOIN res.res_period f ON (f.periodid = e.periodid)
      WHERE
        t.route_va > 0
        AND t.fa_calc > 0
      HAVING
        max(t.faceid) IS NOT NULL;

      GET DIAGNOSTICS v_inserted = ROW_COUNT;
      v_total_rows := v_total_rows + v_inserted;

    END LOOP;
    CLOSE cur_spotplan;

  END LOOP;
  CLOSE cur_vacid;

  -- Přesun do logu
  INSERT INTO rel_pub.crp_cache_log (
    created,
    rvloadid, faceid, spot_length, spot_pause, vacid, facepid,
    vac, rots, vac_orig, rots_orig, group_def, period_hours
  )
  SELECT
    now(),
    rvloadid, faceid, spot_length, spot_pause, vacid, facepid,
    vac, rots, vac_orig, rots_orig, group_def, period_hours
  FROM rel_pub.crp_cache_temp;

  INSERT INTO rel_pub.crp_ims_input_inc_log (
    created,
    pid, mid, pnlkey, pnldesc2, facepid, face_userid, ownid, ownid_desc,
    pnlillum_desc, pnlmotion, pnlmotion_desc, faces, pos, display_time,
    loop_time, sizeid, frmwidth, frmheight, pnlsubtype_desc, pnlvaienv,
    sheet_id, location_id, location_name, location_aadf, agl_name,
    geoareaname, district, iarea_name, pnldesc, release,
    vac_week_all, vac_week_veh, vac_week_pub, vac_week_ped, vac_week_bic,
    va_week_all, va_week_veh, va_week_pub, va_week_ped, va_week_bic,
    rots_week_all, rots_week_veh, rots_week_pub, rots_week_ped, rots_week_bic,
    vacid, faceid_db, err_vac_res
  )
  SELECT
    now(),
    pid, mid, pnlkey, pnldesc2, facepid, face_userid, ownid, ownid_desc,
    pnlillum_desc, pnlmotion, pnlmotion_desc, faces, pos, display_time,
    loop_time, sizeid, frmwidth, frmheight, pnlsubtype_desc, pnlvaienv,
    sheet_id, location_id, location_name, location_aadf, agl_name,
    geoareaname, district, iarea_name, pnldesc, release,
    vac_week_all, vac_week_veh, vac_week_pub, vac_week_ped, vac_week_bic,
    va_week_all, va_week_veh, va_week_pub, va_week_ped, va_week_bic,
    rots_week_all, rots_week_veh, rots_week_pub, rots_week_ped, rots_week_bic,
    vacid, faceid_db, err_vac_res
  FROM rel_pub.crp_ims_input_inc;

  UPDATE rel_pub.crp_ims_input_inc_log i
  SET cache_ok = TRUE
  WHERE i.created >= NOW() - INTERVAL '1 hour'
    AND EXISTS (
      SELECT 1
      FROM rel_pub.crp_cache_temp t
      WHERE t.vacid = i.vacid
    );

  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Function f_fill_crp_cache_temp end. Processed vacid: ' || v_vacid_cnt || ', inserted to crp_cache_temp: ' || v_total_rows || '.';

END;
$$;


--
-- Name: log_msg(character varying, character varying, character varying); Type: FUNCTION; Schema: rel_pub; Owner: -
--

CREATE FUNCTION log_msg(type_in character varying DEFAULT 'UNDEFINED'::character varying, source_in character varying DEFAULT 'NO_NAME'::character varying, desc_in character varying DEFAULT 'NO_DESC'::character varying) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
  --variable_name datatype;
BEGIN
  INSERT INTO
  rel_pub.crp_log_table
(
  desc_text,
  type,
  source
)
VALUES (
  desc_in,
  type_in,
  source_in
);
EXCEPTION
WHEN others THEN
  --RAISE INFO 'Error Name:%',SQLERRM;
  --RAISE INFO 'Error State:%', SQLSTATE;
  --RAISE INFO 'Error Context:%', err_context;
END;
$$;


--
-- Name: re_publish_crp(); Type: FUNCTION; Schema: rel_pub; Owner: -
--

CREATE FUNCTION re_publish_crp() RETURNS void
    LANGUAGE plpgsql
    AS $$
/**********************************************************************************************
*
*	Vyrobil: Jirka Povolný, 10.03.2026
*
*	Přepláchne res_release_publish pro rvloadid 100, 101
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
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Function re_publish_crp start.';

--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Publishing process start.';
  -- Automatický publish --------------------------------------------------------------------------------------------------
  -- Delete publishe, pokud panely už mají publikované výsledky
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Unpublish 100, 101 in res_release_publish.';

  DELETE FROM 
    res.res_release_publish
  WHERE
    rvloadid IN(100, 101)
  ;
  
  GET DIAGNOSTICS i_cnt = ROW_COUNT;
  RAISE NOTICE 'Records deleted from publish: %', i_cnt;
 
  -- Continual Release IDS
  INSERT INTO res.res_release_publish 
  SELECT DISTINCT ON (sqry.facepid, sqry.transptype)
    100::integer as rvloadid, --!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    sqry.facepid,
    sqry.vacid,
    sqry.selsetid,
    sqry.dayicid,
    sqry.hourcid,
    sqry.fdurid,
    sqry.periodid,
    sqry.modeltype,
    sqry.transptype,
    sqry.rminvacver,
    sqry.vac,
    sqry.rots,
    sqry.va,
    sqry.pop_total,
    sqry.grp_vac_total,
    sqry.grp_rots_total,
    sqry.pid,
    sqry.mid,
    sqry.faceid
  FROM (
    SELECT DISTINCT
      fm.facepid,
      r.*,
      f.pid,
      fm.mid,
      fm.faceid,
      f.pos,
      v.pnlmotion
    FROM 
      res.res_online_ea_output_round r
      JOIN (SELECT DISTINCT ON (vvr.faceid)
              vvr.vacid,
              vvr.faceid,
              vvr.mid,
              vvr.err_vac_res,
              nm.pnlmotion
            FROM 
              pnl.mv_pnl_main_maxdate nm
              JOIN pnl.pnl_facemid fm ON nm.mid = fm.mid
              JOIN vai.vai_vac_res vvr ON fm.faceid = vvr.faceid
            WHERE
            	nm.status = 3 AND nm.timeattid = 0
            ORDER BY  
              vvr.faceid,
              vvr.vacid DESC
            ) v ON r.vacid = v.vacid
      JOIN pnl.pnl_facemid fm ON v.faceid = fm.faceid
      JOIN pnl.pnl_faces f ON fm.facepid = f.facepid
    WHERE 
      r.periodid = 1
  )sqry
  WHERE 
  	1 = 1
    AND(sqry.pnlmotion <> 'D' OR (sqry.pnlmotion = 'D' AND sqry.pos = 1))
  ORDER BY
    sqry.facepid, sqry.transptype, sqry.vacid DESC
  ;

--** Tady je NOTICE **--
	GET DIAGNOSTICS i_cnt = ROW_COUNT;
  RAISE NOTICE 'Records (rvloadid = 100, IDS) inserted to publish: %', i_cnt;

  -- Continual Release IMS
  INSERT INTO res.res_release_publish 
  SELECT DISTINCT ON (sqry.facepid, sqry.transptype, sqry.periodid)
    101::integer as rvloadid, --!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    sqry.facepid,
    sqry.vacid,
    sqry.selsetid,
    sqry.dayicid,
    sqry.hourcid,
    sqry.fdurid,
    sqry.periodid,
    sqry.modeltype,
    sqry.transptype,
    sqry.rminvacver,
    sqry.vac,
    sqry.rots,
    sqry.va,
    sqry.pop_total,
    sqry.grp_vac_total,
    sqry.grp_rots_total,
    sqry.pid,
    sqry.mid,
    sqry.faceid
  FROM (
    SELECT DISTINCT
      fm.facepid,
      r.*,
      f.pid,
      fm.mid,
      fm.faceid,
      f.pos,
      v.pnlmotion
    FROM 
      res.res_online_ea_output_round r
      JOIN (SELECT DISTINCT ON (vvr.faceid)
              vvr.vacid,
              vvr.faceid,
              vvr.mid,
              vvr.err_vac_res,
              nm.pnlmotion
            FROM 
              pnl.mv_pnl_main_maxdate nm
              JOIN pnl.pnl_facemid fm ON nm.mid = fm.mid
              JOIN vai.vai_vac_res vvr ON fm.faceid = vvr.faceid
            WHERE
            	nm.status = 3 AND nm.timeattid = 0
            ORDER BY  
              vvr.faceid,
              vvr.vacid DESC
            ) v ON r.vacid = v.vacid
      JOIN pnl.pnl_facemid fm ON v.faceid = fm.faceid
      JOIN pnl.pnl_faces f ON fm.facepid = f.facepid
    WHERE 
      r.periodid = 200
  )sqry
  WHERE 
  	1 = 1
    AND(sqry.pnlmotion <> 'D' OR (sqry.pnlmotion = 'D' AND sqry.pos = 1))
  ORDER BY
    sqry.facepid, sqry.transptype, sqry.periodid, sqry.vacid DESC
  ;

--** Tady je NOTICE **--
	GET DIAGNOSTICS i_cnt = ROW_COUNT;
  RAISE NOTICE 'Records (rvloadid = 101, IMS) inserted to publish: %', i_cnt;


--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' The end.';

  EXCEPTION
  WHEN others THEN
    RAISE NOTICE 'exception: %', SQLERRM;
    PERFORM rel_pub.log_msg('ERROR', 'auto_rel_pub_inc', 'Error occured: ' || SQLERRM);

END;
$$;


SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: crp_all_intersection; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_all_intersection (
    mid bigint,
    sid integer,
    week_day_name character varying(4),
    week_day_type character varying(4)
);


--
-- Name: crp_all_panel; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_all_panel (
    mid bigint,
    pid bigint,
    the_geom topo.geometry,
    buffer_geom topo.geometry,
    vac_all_1 double precision,
    maxdist double precision,
    intersection_type character varying,
    last_vacid bigint,
    err_vac_res character varying
);


--
-- Name: crp_bad_navteq; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_bad_navteq (
    mid bigint,
    pid bigint,
    navteqver character varying(4),
    processed integer DEFAULT 0,
    stamp timestamp(0) without time zone DEFAULT now()
);


--
-- Name: crp_cache_del_log; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_cache_del_log (
    row_id integer NOT NULL,
    deleted_at timestamp with time zone DEFAULT now(),
    rvloadid integer,
    faceid bigint,
    spot_length double precision,
    spot_pause double precision,
    vacid bigint,
    facepid bigint,
    vac double precision,
    rots double precision,
    vac_orig double precision,
    rots_orig double precision,
    group_def character varying,
    period_hours integer
);


--
-- Name: crp_cache_log; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_cache_log (
    row_id integer NOT NULL,
    created timestamp without time zone DEFAULT now(),
    rvloadid integer,
    faceid integer,
    spot_length double precision,
    spot_pause double precision,
    vacid integer,
    facepid integer,
    vac double precision,
    rots numeric,
    vac_orig double precision,
    rots_orig numeric,
    group_def character varying,
    period_hours double precision
);


--
-- Name: crp_cache_log_row_id_seq; Type: SEQUENCE; Schema: rel_pub; Owner: -
--

CREATE SEQUENCE crp_cache_log_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: crp_cache_log_row_id_seq; Type: SEQUENCE OWNED BY; Schema: rel_pub; Owner: -
--

ALTER SEQUENCE crp_cache_log_row_id_seq OWNED BY crp_cache_log.row_id;


--
-- Name: crp_cache_temp; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_cache_temp (
    rvloadid integer,
    faceid integer,
    spot_length double precision,
    spot_pause double precision,
    vacid integer,
    facepid integer,
    vac double precision,
    rots numeric,
    vac_orig double precision,
    rots_orig numeric,
    group_def character varying,
    period_hours double precision
);


--
-- Name: crp_cahe_del_log_row_id_seq; Type: SEQUENCE; Schema: rel_pub; Owner: -
--

CREATE SEQUENCE crp_cahe_del_log_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: crp_cahe_del_log_row_id_seq; Type: SEQUENCE OWNED BY; Schema: rel_pub; Owner: -
--

ALTER SEQUENCE crp_cahe_del_log_row_id_seq OWNED BY crp_cache_del_log.row_id;


--
-- Name: crp_ccf_input; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_ccf_input (
    mid integer,
    sid integer,
    respondent_id character varying,
    routingmode character varying(10),
    pid bigint,
    tsyear character varying,
    desc_text character varying
);


--
-- Name: crp_ccf_output; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_ccf_output (
    pid bigint,
    faceid bigint,
    facepid bigint,
    sid integer,
    respondent_id character varying,
    respweight double precision,
    modality character varying(255),
    dayofweek character varying(255),
    va double precision,
    vac double precision,
    ts_rots double precision
);


--
-- Name: crp_ccf_output_inc; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_ccf_output_inc (
    pid bigint,
    faceid bigint,
    facepid bigint,
    sid integer,
    respondent_id character varying,
    respweight double precision,
    modality character varying(255),
    dayofweek character varying(255),
    va double precision,
    vac double precision,
    ts_rots double precision
);


--
-- Name: crp_control_sum; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_control_sum (
    pid bigint,
    faceid bigint,
    sum_of_ts_rots double precision,
    sum_of_ts_vac double precision,
    load_id integer,
    load_type character varying(4),
    inserted timestamp(0) without time zone DEFAULT now()
);


--
-- Name: crp_evidence; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_evidence (
    mid integer,
    sid integer,
    week_day_name character varying(4),
    week_day_type character varying(4),
    prob double precision
);


--
-- Name: crp_freq_trips; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_freq_trips (
    day_type character varying(255),
    trip_freq_from double precision,
    trip_freq_to double precision,
    mo double precision,
    tu double precision,
    we double precision,
    th double precision,
    fr double precision,
    sa double precision,
    su double precision
);


--
-- Name: crp_freq_trips_vector; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_freq_trips_vector (
    weekdaytype character varying(2),
    weekdayname character varying(2),
    freq_from double precision,
    freq_to double precision,
    prob double precision
);


--
-- Name: crp_geom_panel; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_geom_panel (
    mid bigint,
    pid bigint,
    the_geom topo.geometry(Point,4326),
    buffer_geom topo.geometry,
    vac_all_1 double precision,
    maxdist double precision,
    intersection_type character varying,
    last_vacid bigint,
    err_vac_res character varying
);


--
-- Name: crp_geom_panel_log; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_geom_panel_log (
    mid bigint,
    pid bigint,
    the_geom topo.geometry,
    buffer_geom topo.geometry,
    load_id bigint,
    desc_text character varying,
    stamp timestamp(0) without time zone DEFAULT now()
);


--
-- Name: crp_geom_panel_temp; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_geom_panel_temp (
    mid bigint,
    pid bigint,
    the_geom topo.geometry,
    buffer_geom topo.geometry,
    vac_all_2 double precision,
    maxdist double precision,
    intersection_type character varying,
    last_vacid bigint,
    err_vac_res character varying
);


--
-- Name: crp_ims_input; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_ims_input (
    pid bigint,
    mid bigint,
    pnlkey character varying,
    pnldesc2 character varying,
    facepid bigint,
    face_userid character varying,
    ownid integer,
    ownid_desc character varying,
    pnlillum_desc character varying,
    pnlmotion character varying,
    pnlmotion_desc character varying,
    faces integer,
    pos integer,
    display_time integer,
    loop_time integer,
    sizeid integer,
    frmwidth double precision,
    frmheight double precision,
    pnlsubtype_desc character varying,
    pnlvaienv character varying,
    sheet_id integer,
    location_id integer,
    location_name character varying,
    location_aadf character varying,
    agl_name character varying(40) DEFAULT NULL::character varying,
    geoareaname character varying,
    district character varying,
    iarea_name character varying,
    pnldesc character varying,
    release character varying,
    vac_week_all integer,
    vac_week_veh integer,
    vac_week_pub integer,
    vac_week_ped integer,
    vac_week_bic integer,
    va_week_all numeric(4,3) DEFAULT NULL::numeric,
    va_week_veh numeric(4,3) DEFAULT NULL::numeric,
    va_week_pub numeric(4,3),
    va_week_ped numeric(4,3) DEFAULT NULL::numeric,
    va_week_bic numeric(4,3),
    rots_week_all integer,
    rots_week_veh integer,
    rots_week_pub integer,
    rots_week_ped integer,
    rots_week_bic integer,
    vacid bigint,
    faceid_db bigint,
    err_vac_res character varying
);


--
-- Name: crp_ims_input2; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_ims_input2 (
    pid bigint,
    mid bigint,
    pnlkey character varying,
    pnldesc2 character varying,
    facepid bigint,
    face_userid character varying,
    ownid integer,
    ownid_desc character varying,
    pnlillum_desc character varying,
    pnlmotion character varying,
    pnlmotion_desc character varying,
    faces integer,
    pos integer,
    display_time integer,
    loop_time integer,
    sizeid integer,
    frmwidth double precision,
    frmheight double precision,
    pnlsubtype_desc character varying,
    pnlvaienv character varying,
    sheet_id integer,
    location_id integer,
    location_name character varying,
    location_aadf character varying,
    agl_name character varying(40) DEFAULT NULL::character varying,
    geoareaname character varying,
    district character varying,
    iarea_name character varying,
    pnldesc character varying,
    release character varying,
    vac_brutto_daily_all integer,
    vac_brutto_daily_veh integer,
    vac_brutto_daily_ped integer,
    va_brutto_daily_all numeric(4,3) DEFAULT NULL::numeric,
    va_brutto_daily_veh numeric(4,3) DEFAULT NULL::numeric,
    va_brutto_daily_ped numeric(4,3) DEFAULT NULL::numeric,
    rots_brutto_daily_all integer,
    rots_brutto_daily_veh integer,
    rots_brutto_daily_ped integer,
    vacid bigint,
    faceid_db bigint,
    err_vac_res character varying
);


--
-- Name: crp_ims_input_inc; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_ims_input_inc (
    pid bigint,
    mid bigint,
    pnlkey character varying,
    pnldesc2 character varying,
    facepid bigint,
    face_userid character varying,
    ownid integer,
    ownid_desc character varying,
    pnlillum_desc character varying,
    pnlmotion character varying,
    pnlmotion_desc character varying,
    faces integer,
    pos integer,
    display_time integer,
    loop_time integer,
    sizeid integer,
    frmwidth double precision,
    frmheight double precision,
    pnlsubtype_desc character varying,
    pnlvaienv character varying,
    sheet_id integer,
    location_id integer,
    location_name character varying,
    location_aadf character varying,
    agl_name character varying(40) DEFAULT NULL::character varying,
    geoareaname character varying,
    district character varying,
    iarea_name character varying,
    pnldesc character varying,
    release character varying,
    vac_week_all integer,
    vac_week_veh integer,
    vac_week_pub integer,
    vac_week_ped integer,
    vac_week_bic integer,
    va_week_all numeric(4,3) DEFAULT NULL::numeric,
    va_week_veh numeric(4,3) DEFAULT NULL::numeric,
    va_week_pub numeric(4,3),
    va_week_ped numeric(4,3) DEFAULT NULL::numeric,
    va_week_bic numeric(4,3),
    rots_week_all integer,
    rots_week_veh integer,
    rots_week_pub integer,
    rots_week_ped integer,
    rots_week_bic integer,
    vacid bigint,
    faceid_db bigint,
    err_vac_res character varying
);


--
-- Name: crp_ims_input_inc_log; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_ims_input_inc_log (
    row_id integer NOT NULL,
    created timestamp without time zone DEFAULT now(),
    pid bigint,
    mid bigint,
    pnlkey character varying,
    pnldesc2 character varying,
    facepid bigint,
    face_userid character varying,
    ownid integer,
    ownid_desc character varying,
    pnlillum_desc character varying,
    pnlmotion character varying,
    pnlmotion_desc character varying,
    faces integer,
    pos integer,
    display_time integer,
    loop_time integer,
    sizeid integer,
    frmwidth double precision,
    frmheight double precision,
    pnlsubtype_desc character varying,
    pnlvaienv character varying,
    sheet_id integer,
    location_id integer,
    location_name character varying,
    location_aadf character varying,
    agl_name character varying(40),
    geoareaname character varying,
    district character varying,
    iarea_name character varying,
    pnldesc character varying,
    release character varying,
    vac_week_all integer,
    vac_week_veh integer,
    vac_week_pub integer,
    vac_week_ped integer,
    vac_week_bic integer,
    va_week_all numeric(4,3),
    va_week_veh numeric(4,3),
    va_week_pub numeric(4,3),
    va_week_ped numeric(4,3),
    va_week_bic numeric(4,3),
    rots_week_all integer,
    rots_week_veh integer,
    rots_week_pub integer,
    rots_week_ped integer,
    rots_week_bic integer,
    vacid bigint,
    faceid_db bigint,
    err_vac_res character varying,
    cache_ok boolean DEFAULT false
);


--
-- Name: crp_ims_input_inc_log_row_id_seq; Type: SEQUENCE; Schema: rel_pub; Owner: -
--

CREATE SEQUENCE crp_ims_input_inc_log_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: crp_ims_input_inc_log_row_id_seq; Type: SEQUENCE OWNED BY; Schema: rel_pub; Owner: -
--

ALTER SEQUENCE crp_ims_input_inc_log_row_id_seq OWNED BY crp_ims_input_inc_log.row_id;


--
-- Name: crp_ims_temp; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_ims_temp (
    pid integer,
    pnlkey character varying,
    pnldesc2 character varying,
    ownid_desc character varying,
    pnlvaienv character varying,
    pnlillum_desc character varying,
    pnlmotion_desc character varying,
    frmwidth double precision,
    frmheight double precision,
    pnldesc character varying,
    district character varying,
    mid integer,
    facepid integer,
    vac_brutto_weekly_all integer,
    vac_brutto_weekly_veh integer,
    vac_brutto_weekly_ped integer,
    rots_brutto_weekly_all integer,
    rots_brutto_weekly_veh integer,
    rots_brutto_weekly_ped integer
);


--
-- Name: crp_intersection; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_intersection (
    mid bigint,
    sid integer,
    week_day_name character varying(4),
    week_day_type character varying(4)
);


--
-- Name: crp_log_table; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_log_table (
    row_id integer NOT NULL,
    "timestamp" timestamp(6) without time zone DEFAULT now(),
    desc_text text,
    type character varying(20),
    source character varying(50),
    current_usr character varying(200) DEFAULT "current_user"()
);


--
-- Name: crp_missing; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_missing (
    pid bigint
);


--
-- Name: crp_nearest20; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_nearest20 (
    mid_woi bigint,
    vac_woi double precision,
    best_mid_near bigint,
    best_vac_near double precision,
    geometric_distance double precision,
    value_difference double precision,
    best_pid_near bigint,
    the_geom topo.geometry,
    buffer_geom topo.geometry
);


--
-- Name: crp_new_mids; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_new_mids (
    mid bigint,
    pid bigint
);


--
-- Name: crp_no_calc; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_no_calc (
    pid bigint,
    in_queue boolean DEFAULT false NOT NULL,
    calculated boolean DEFAULT false NOT NULL,
    processed boolean DEFAULT false NOT NULL,
    err_vac_res character varying
);


--
-- Name: crp_no_calc_inc; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_no_calc_inc (
    pid bigint,
    mid bigint,
    in_queue boolean DEFAULT false NOT NULL,
    calculated boolean DEFAULT false NOT NULL,
    processed boolean DEFAULT false NOT NULL,
    ignore boolean DEFAULT false NOT NULL,
    err_vac_res character varying,
    created timestamp(0) without time zone DEFAULT now(),
    updated timestamp(0) without time zone DEFAULT now(),
    created_by character varying DEFAULT "current_user"(),
    updated_by character varying DEFAULT "current_user"(),
    complexity_index numeric,
    routing_difficulty text
);


--
-- Name: crp_panel_wo_its; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_panel_wo_its (
    mid bigint,
    pid bigint,
    the_geom topo.geometry(Point,4326),
    buffer_geom topo.geometry,
    vac_all_1 double precision
);


--
-- Name: crp_pnl_to_revise; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_pnl_to_revise (
    pid bigint,
    mid bigint,
    resolved boolean DEFAULT false NOT NULL,
    processed boolean DEFAULT false NOT NULL,
    err_vac_res character varying,
    complexity_index numeric,
    routing_difficulty text,
    null_zero_result boolean DEFAULT false,
    va double precision,
    vac double precision,
    load_id integer,
    created timestamp(0) without time zone DEFAULT now(),
    updated timestamp(0) without time zone DEFAULT now(),
    created_by character varying DEFAULT "current_user"(),
    updated_by character varying DEFAULT "current_user"()
);


--
-- Name: crp_resp_weight_input; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_resp_weight_input (
    respondent_id character varying,
    respondent_num character varying,
    country character varying(3),
    benchwgt_pp double precision
);


--
-- Name: crp_routing; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_routing (
    id character varying NOT NULL,
    geom topo.geometry(MultiLineString,4326),
    pkid bigint,
    sid integer,
    wave character varying(10),
    xc double precision,
    yc double precision,
    mqid character varying(12),
    tripid integer,
    segmentid integer,
    tripdate character varying(16),
    country character varying(6),
    respfromlist integer,
    answerfreq character varying(36),
    modality character varying(32),
    week_day_num integer,
    week_day_name character varying(4),
    week_day_type character varying(4),
    trip_freq double precision,
    respondent_id character varying,
    routingmode character varying(3)
);


--
-- Name: crp_spotplans; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_spotplans (
    rvloadid integer,
    spot_length double precision,
    spot_pause double precision
);


--
-- Name: crp_ts_rots_temp; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_ts_rots_temp (
    pid integer,
    faceid integer,
    ims_rots integer,
    ims_vac integer,
    ims_va double precision,
    ts_rots double precision,
    ts_vac integer,
    ts_va double precision,
    ts_va_adj double precision,
    ts_freq integer
);


--
-- Name: crp_unpublish_list; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE crp_unpublish_list (
    pid bigint,
    mid bigint,
    desc_text character varying,
    stamp timestamp(0) without time zone DEFAULT now()
);
ALTER TABLE ONLY crp_unpublish_list ALTER COLUMN pid SET STATISTICS 0;
ALTER TABLE ONLY crp_unpublish_list ALTER COLUMN mid SET STATISTICS 0;
ALTER TABLE ONLY crp_unpublish_list ALTER COLUMN desc_text SET STATISTICS 0;


--
-- Name: etl_ccf_rel_pub; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE etl_ccf_rel_pub (
    pid integer,
    mid integer,
    faceid integer,
    facepid integer,
    vacid bigint,
    respondent_id character varying,
    country character varying(3),
    weekdayname character varying,
    resp_weight double precision,
    ts_rots double precision,
    va double precision,
    vac double precision,
    load_id integer
);


--
-- Name: etl_ccf_rel_pub_inc; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE etl_ccf_rel_pub_inc (
    pid integer,
    mid integer,
    faceid integer,
    facepid integer,
    vacid bigint,
    respondent_id character varying,
    country character varying(3),
    weekdayname character varying,
    resp_weight double precision,
    ts_rots double precision,
    va double precision,
    vac double precision,
    load_id integer
);


--
-- Name: load_id_seq; Type: SEQUENCE; Schema: rel_pub; Owner: -
--

CREATE SEQUENCE load_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: etl_ccf_rel_pub_loads; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE etl_ccf_rel_pub_loads (
    "timestamp" timestamp(6) without time zone DEFAULT now(),
    "current_user" character varying,
    load_id bigint DEFAULT nextval('load_id_seq'::regclass),
    processed integer DEFAULT 0
);
ALTER TABLE ONLY etl_ccf_rel_pub_loads ALTER COLUMN "timestamp" SET STATISTICS 0;


--
-- Name: load_id_inc_seq; Type: SEQUENCE; Schema: rel_pub; Owner: -
--

CREATE SEQUENCE load_id_inc_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: etl_ccf_rel_pub_loads_inc; Type: TABLE; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE TABLE etl_ccf_rel_pub_loads_inc (
    "timestamp" timestamp(6) without time zone DEFAULT now(),
    "current_user" character varying,
    load_id bigint DEFAULT nextval('load_id_inc_seq'::regclass),
    processed integer DEFAULT 0
);
ALTER TABLE ONLY etl_ccf_rel_pub_loads_inc ALTER COLUMN "timestamp" SET STATISTICS 0;


--
-- Name: z_log_table_row_id_seq; Type: SEQUENCE; Schema: rel_pub; Owner: -
--

CREATE SEQUENCE z_log_table_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: z_log_table_row_id_seq; Type: SEQUENCE OWNED BY; Schema: rel_pub; Owner: -
--

ALTER SEQUENCE z_log_table_row_id_seq OWNED BY crp_log_table.row_id;


--
-- Name: row_id; Type: DEFAULT; Schema: rel_pub; Owner: -
--

ALTER TABLE ONLY crp_cache_del_log ALTER COLUMN row_id SET DEFAULT nextval('crp_cahe_del_log_row_id_seq'::regclass);


--
-- Name: row_id; Type: DEFAULT; Schema: rel_pub; Owner: -
--

ALTER TABLE ONLY crp_cache_log ALTER COLUMN row_id SET DEFAULT nextval('crp_cache_log_row_id_seq'::regclass);


--
-- Name: row_id; Type: DEFAULT; Schema: rel_pub; Owner: -
--

ALTER TABLE ONLY crp_ims_input_inc_log ALTER COLUMN row_id SET DEFAULT nextval('crp_ims_input_inc_log_row_id_seq'::regclass);


--
-- Name: row_id; Type: DEFAULT; Schema: rel_pub; Owner: -
--

ALTER TABLE ONLY crp_log_table ALTER COLUMN row_id SET DEFAULT nextval('z_log_table_row_id_seq'::regclass);


--
-- Name: idx_panel_buffer; Type: INDEX; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE INDEX idx_panel_buffer ON crp_geom_panel USING gist (buffer_geom);


--
-- Name: idx_routing; Type: INDEX; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE INDEX idx_routing ON crp_routing USING gist (geom);


--
-- Name: z_all_panel_indx_gist_buffer; Type: INDEX; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE INDEX z_all_panel_indx_gist_buffer ON crp_all_panel USING gist (buffer_geom);


--
-- Name: z_geom_panel_evidence_indx_gist_buffer; Type: INDEX; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE INDEX z_geom_panel_evidence_indx_gist_buffer ON crp_geom_panel_log USING gist (buffer_geom);


--
-- Name: z_geom_panel_temp_indx_gist_buffer; Type: INDEX; Schema: rel_pub; Owner: -; Tablespace: 
--

CREATE INDEX z_geom_panel_temp_indx_gist_buffer ON crp_geom_panel_temp USING gist (buffer_geom);


--
-- Name: update_timestamp_trigger; Type: TRIGGER; Schema: rel_pub; Owner: -
--

CREATE TRIGGER update_timestamp_trigger BEFORE UPDATE ON crp_no_calc_inc FOR EACH ROW EXECUTE PROCEDURE fnc.set_updated_timestamp();


--
-- Name: update_timestamp_trigger; Type: TRIGGER; Schema: rel_pub; Owner: -
--

CREATE TRIGGER update_timestamp_trigger BEFORE UPDATE ON crp_pnl_to_revise FOR EACH ROW EXECUTE PROCEDURE fnc.set_updated_timestamp();


--
-- Name: update_updated_by_trigger; Type: TRIGGER; Schema: rel_pub; Owner: -
--

CREATE TRIGGER update_updated_by_trigger BEFORE UPDATE ON crp_no_calc_inc FOR EACH ROW EXECUTE PROCEDURE fnc.set_updated_by();


--
-- Name: update_updated_by_trigger; Type: TRIGGER; Schema: rel_pub; Owner: -
--

CREATE TRIGGER update_updated_by_trigger BEFORE UPDATE ON crp_pnl_to_revise FOR EACH ROW EXECUTE PROCEDURE fnc.set_updated_by();


--
-- PostgreSQL database dump complete
--

