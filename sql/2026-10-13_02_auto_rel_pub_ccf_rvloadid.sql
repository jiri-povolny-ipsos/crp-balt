--
-- Full CCF výpočet pro zadaný release (rvloadid) ve schématu rel_pub
--
-- Vytvořil: Jiří Povolný
-- Vytvořeno dne: 13.10.2026
--
-- Změnil: Jiří Povolný
-- Změněno dne: 13.10.2026
-- Popis změny: auto_rel_pub_ccf má třetí parametr p_publish (DEFAULT false) - publish do
--              res.res_release_publish pro nové CRP (rvloadid 102 = IDS periodid 1, 103 = IMS periodid 200),
--              viz bod 9. Odstraňuje se i dvouparametrová verze nasazená 13.10.2026 odpoledne.
-- Změnil: Jiří Povolný
-- Změněno dne: 13.10.2026
-- Popis změny: při p_publish = true se po publish smaže cache spotplánů ids.ids_vac_calc_campaign_cache2
--              pro rvloadid 102 (s logem do crp_cache_del_log) a spotplány, které v ní byly, se spočítají
--              znovu campaign funkcí vac8s._f_vac_calc_ids_campaign, viz bod 10.
--
-- Nahrazuje rel_pub.auto_rel_pub_ccf() a rel_pub.create_ims_input_ccf() verzemi s parametry
-- p_rvloadid, p_periodid (a p_publish). Vychází z exportu ddl/schema_rel_pub.sql (stav k 21.9.2026, v DB k 13.10.2026 beze změny).
-- Oproti němu je změněno jen toto, vše ostatní je znak po znaku shodné (konce řádků sjednocené na LF):
--   auto_rel_pub_ccf(p_rvloadid integer, p_periodid integer DEFAULT NULL, p_publish boolean DEFAULT false)
--     1) panely (pid, mid) z res.res_release_publish WHERE rvloadid = p_rvloadid
--        místo pnl.mv_pnl_main_maxdate (status = 3 ...) a crp_missing
--     2) last_vacid a vac_all_1 jen z vacidů releasu
--     3) crp_all_panel -> crp_panel_full (všechny výskyty)
--     4) odstraněno TRUNCATE crp_missing, přidáno TRUNCATE crp_no_calc na začátku
--     5) kontrola existence releasu na začátku
--     6) blok pro panely s nulovým rots_week_all (do crp_no_calc) + NULLIF u ims_va a ts_va
--     7) crp_geom_panel_log.desc_text = 'Full load, rvloadid N, periodid M', rvloadid a periodid v textu logu
--     8) periodid IMS hodnot (vac_all_1 a create_ims_input_ccf) = p_periodid, pokud je NULL, tak periodid
--        releasu z res.res_release_publish (místo pevného periodid = 1), včetně kontroly na začátku
--     9) při p_publish = true na konci publish do res.res_release_publish: celý obsah rvloadid 102 a 103
--        se nahradí všemi facemi releasu p_rvloadid (vč. digitálních fací s pozicí > 1) s IMS hodnotami
--        z res.res_online_ea_output_round (102 = periodid 1, 103 = periodid 200)
--    10) při p_publish = true po publish: cache spotplánů pro rvloadid 102 se smaže (log do crp_cache_del_log)
--        a spotplány, které v ní byly, se spočítají znovu campaign funkcí (seznam přes rel_pub.crp_spotplans)
--   create_ims_input_ccf(p_rvloadid integer, p_periodid integer)
--     1) vacidy fací z res.res_release_publish WHERE rvloadid = p_rvloadid
--     2) crp_all_panel -> crp_panel_full
--     3) periodid = p_periodid místo periodid = 1 (VAC a ROTS podle transptype)
--
-- Předpoklad: existuje rel_pub.crp_panel_full (2026-10-13_01_create_crp_panel_full.sql).
-- Nasazení: spustit celý soubor. Předchozí verze funkcí (bez parametrů, s 2 i se 3 parametry) se odstraní,
--           skript je možné spustit opakovaně.
-- Rollback: DROP obou funkcí s parametry a znovu nasadit původní definice z ddl/schema_rel_pub.sql
--           (auto_rel_pub_ccf od řádku 26, create_ims_input_ccf od řádku 1751).
--

BEGIN;

DROP FUNCTION IF EXISTS rel_pub.auto_rel_pub_ccf();
DROP FUNCTION IF EXISTS rel_pub.auto_rel_pub_ccf(integer, integer);
DROP FUNCTION IF EXISTS rel_pub.auto_rel_pub_ccf(integer, integer, boolean);
DROP FUNCTION IF EXISTS rel_pub.create_ims_input_ccf();
DROP FUNCTION IF EXISTS rel_pub.create_ims_input_ccf(integer, integer);

CREATE FUNCTION rel_pub.create_ims_input_ccf(p_rvloadid integer, p_periodid integer) RETURNS void
    LANGUAGE plpgsql
    AS $$
/**********************************************************************************************
*
*	Vyrobil: Jirka Povolný, 2.2.2024
*
*	Změnil: Jiří Povolný
*	Změněno dne: 13.10.2026
*	Popis změny:
*	  1) Parametr p_rvloadid. Vacidy fací se berou z res.res_release_publish pro daný rvloadid
*	     (dřív poslední vacid face z vai_vac_res).
*	  2) Seznam panelů se čte z crp_panel_full místo crp_all_panel.
*	  3) Parametr p_periodid. IMS hodnoty (VAC, ROTS) z res.res_online_ea_output_round se berou
*	     pro p_periodid (dřív pevně periodid = 1). Určuje ho volající auto_rel_pub_ccf.
*
*	Pouští se z fce auto_rel_pub_ccf, protože potřebuje tabulku rel_pub.crp_panel_full
*	naplněnou správnými midy. Naplní tabulku rel_pub.crp_ims_input
*
**********************************************************************************************/
DECLARE
  i_cnt integer;				-- Počet záznamů ke zpracování
  
BEGIN

--*****--		
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Function ims_input_ccf() is starting, rvloadid: ' || p_rvloadid || ', periodid: ' || p_periodid || '.';

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
      a.mid IN(SELECT mid FROM rel_pub.crp_panel_full)
      AND(a.pnlmotion <> 'D' OR (a.pnlmotion = 'D' AND f2.pos = 1))
      AND v.vacid IN(SELECT vacid FROM res.res_release_publish WHERE rvloadid = p_rvloadid)
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
    b.periodid = p_periodid AND
    b.transptype = 'ALL'
  ;
	UPDATE rel_pub.crp_ims_input a 
  SET 
  	vac_week_veh = b.vac
	FROM 
  	res.res_online_ea_output_round b
	WHERE
    a.vacid = b.vacid AND
    b.periodid = p_periodid AND
    b.transptype = 'VEH'
  ;
	UPDATE rel_pub.crp_ims_input a 
  SET 
  	vac_week_pub = b.vac
	FROM 
  	res.res_online_ea_output_round b
	WHERE
    a.vacid = b.vacid AND
    b.periodid = p_periodid AND
    b.transptype = 'PUB'
  ;
	UPDATE rel_pub.crp_ims_input a 
  SET 
  	vac_week_ped = b.vac
	FROM 
  	res.res_online_ea_output_round b
	WHERE
    a.vacid = b.vacid and
    b.periodid = p_periodid and
    b.transptype = 'PED'
  ;
	UPDATE rel_pub.crp_ims_input a 
  SET 
  	vac_week_bic = b.vac
	FROM 
  	res.res_online_ea_output_round b
	WHERE
    a.vacid = b.vacid and
    b.periodid = p_periodid and
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
    AND b.periodid = p_periodid 
    AND b.transptype = 'ALL'
  ;
  UPDATE rel_pub.crp_ims_input a 
  SET 
    rots_week_veh = b.rots
  FROM 
    res.res_online_ea_output_round b
  WHERE	
    a.vacid = b.vacid 
    AND b.periodid = p_periodid 
    AND b.transptype = 'VEH'
  ;
  UPDATE rel_pub.crp_ims_input a 
  SET 
    rots_week_pub = b.rots
  FROM 
    res.res_online_ea_output_round b
  WHERE
    a.vacid = b.vacid 
    AND b.periodid = p_periodid 
    AND b.transptype = 'PUB'
  ;UPDATE rel_pub.crp_ims_input a 
  SET 
    rots_week_ped = b.rots
  FROM 
    res.res_online_ea_output_round b
  WHERE
    a.vacid = b.vacid 
    AND b.periodid = p_periodid 
    AND b.transptype = 'PED'
  ;UPDATE rel_pub.crp_ims_input a 
  SET 
    rots_week_bic = b.rots
  FROM 
    res.res_online_ea_output_round b
  WHERE
    a.vacid = b.vacid 
    AND b.periodid = p_periodid 
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


CREATE FUNCTION rel_pub.auto_rel_pub_ccf(p_rvloadid integer, p_periodid integer DEFAULT NULL, p_publish boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql
    AS $$
/**********************************************************************************************
*
*	Vyrobil: Jirka Povolný, 2.2.2024
*
*	Změnil: Jiří Povolný
*	Změněno dne: 13.10.2026
*	Popis změny:
*	  1) Parametr p_rvloadid. Panely (pid, mid) a vacidy fací se berou z res.res_release_publish
*	     pro daný rvloadid (dřív panely podle statusu v pnl a poslední vacid z vai_vac_res).
*	     IMS hodnoty pro kalibraci se berou z res.res_online_ea_output_round pro p_periodid
*	     (viz bod 7).
*	  2) Seznam panelů full běhu je v nové tabulce crp_panel_full. crp_all_panel patří
*	     inkrementálnímu běhu (halda panelů pro auto_rel_pub_inc), full ji už nemaže.
*	  3) Odstraněna práce s crp_missing - patří inkrementálnímu běhu a full ji mazal.
*	  4) crp_no_calc se vyprazdňuje na začátku běhu (zůstávaly v ní panely z minulých běhů).
*	  5) Ošetření dělení nulou (NULLIF) u ims_va a ts_va a vyřazení panelů s nulovým
*	     rots_week_all do crp_no_calc - stejně jako v auto_rel_pub_inc() od 22.9.2026.
*	  6) crp_geom_panel_log se plní s desc_text 'Full load, rvloadid N'. load_id full
*	     a inkrementálního běhu jsou z různých sekvencí a jinak je nejde rozlišit.
*	  7) Parametr p_periodid (DEFAULT NULL) - periodid IMS hodnot pro kalibraci (vac_all_1,
*	     crp_ims_input). NULL = periodid releasu z res.res_release_publish (rvloadid 13 -> 1,
*	     rvloadid 14 -> 200). Dřív pevně periodid = 1.
*	  8) Parametr p_publish (DEFAULT false). Při true se na konci běhu nahradí celý obsah
*	     res.res_release_publish pro rvloadid 102 (IDS, periodid 1) a 103 (IMS, periodid 200)
*	     všemi facemi releasu p_rvloadid (vč. digitálních fací s pozicí > 1) s IMS hodnotami
*	     z res.res_online_ea_output_round. 102 a 103 jsou rvloadid nového CRP (release 9, CRP 26.1).
*	  9) Při p_publish = true se po publish smaže cache spotplánů ids.ids_vac_calc_campaign_cache2
*	     pro rvloadid 102 (jinak by v ní zůstaly staré vacidy - campaign funkce neprázdnou cache
*	     spotplánu nepřepočítá) a spotplány, které v ní byly, se hned spočítají znovu oficiální
*	     funkcí vac8s._f_vac_calc_ids_campaign. Chyba campaign funkce (vacid = -999) se jen zaloguje
*	     jako WARNING, IDS si takový spotplán dopočítá samo při prvním dotazu.
*
*	Používá se k výpočtu CCF pro celý release. Výsledek jde do rel_pub.etl_ccf_rel_pub,
*	do res.res_release_publish jen při p_publish = true.
*	Volání: SELECT rel_pub.auto_rel_pub_ccf(13, 1, true);  -- release 13, periodid 1, publish do 102/103
*	        SELECT rel_pub.auto_rel_pub_ccf(13);           -- jen výpočet, periodid releasu (1), bez publish
*
**********************************************************************************************/
DECLARE
  exe_str varchar;			-- String pro poskládání SQL příkazů
  last_load_id integer;	-- Id posledního loadu do etl tabulky pro IDS
  i_cnt integer;				-- Počet záznamů ke zpracování
  navteqver_str varchar;-- Poslední navteqver z params()
  v_periodid integer;   -- periodid IMS hodnot pro kalibraci
  i_pub_ids integer;    -- Počet záznamů publikovaných do rvloadid 102
  i_pub_ims integer;    -- Počet záznamů publikovaných do rvloadid 103
  r_spot record;        -- Spotplán pro přepočet cache
  i_cache_rows integer := 0; -- Počet řádků spočítaných do cache 102
  i_cache_spot integer := 0; -- Počet přepočítaných spotplánů cache 102
  i_cache_err integer := 0;  -- Počet spotplánů, u kterých campaign funkce vrátila chybu
  v_rows integer;
  v_err integer;
  
BEGIN
-------------------------------------------------------------------------------------------------------------------
--** Tady je NOTICE **--
RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Function auto_rel_pub_ccf start.';
-- Načtení aktuální navteqver pro kontrolu správného navteqver u panelů
SELECT navteq_version INTO navteqver_str FROM fnc.f_params();
--SELECT navteq_version FROM fnc.f_params();--25q1
--** Tady je NOTICE **--
RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Navteq version: ' || navteqver_str;
-- Kontrola, že release (rvloadid) existuje, jinak není co počítat.
SELECT COUNT(*) INTO i_cnt FROM res.res_release_publish WHERE rvloadid = p_rvloadid;
--** Tady je NOTICE **--
RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' rvloadid: ' || COALESCE(p_rvloadid::text, 'NULL') || ', records in res_release_publish: ' || i_cnt;
IF i_cnt = 0 THEN
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' rvloadid not found in res.res_release_publish, exiting.';
  PERFORM rel_pub.log_msg('ERROR', 'fce:auto_rel_pub', 'rvloadid ' || COALESCE(p_rvloadid::text, 'NULL') || ' not found in res.res_release_publish.');
  RETURN;
END IF;
-- Publish přepisuje 102 a 103, zdrojem proto nesmí být ony samy.
IF p_publish AND p_rvloadid IN (102, 103) THEN
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' rvloadid 102/103 cannot be the source for publish, exiting.';
  PERFORM rel_pub.log_msg('ERROR', 'fce:auto_rel_pub', 'rvloadid ' || p_rvloadid || ' cannot be the source for publish to 102/103.');
  RETURN;
END IF;
-- periodid IMS hodnot: zadaný parametr, jinak periodid releasu (release musí mít jen jeden).
IF p_periodid IS NOT NULL THEN
  v_periodid := p_periodid;
ELSE
  SELECT COUNT(DISTINCT periodid), MIN(periodid) INTO i_cnt, v_periodid
  FROM res.res_release_publish WHERE rvloadid = p_rvloadid;
  IF i_cnt <> 1 THEN
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' rvloadid has ' || i_cnt || ' periodids, p_periodid must be set, exiting.';
    PERFORM rel_pub.log_msg('ERROR', 'fce:auto_rel_pub', 'rvloadid ' || p_rvloadid || ' has ' || i_cnt || ' periodids, p_periodid must be set.');
    RETURN;
  END IF;
END IF;
-- Kontrola, že pro vacidy releasu existují IMS hodnoty v daném periodid.
SELECT COUNT(*) INTO i_cnt
FROM res.res_online_ea_output_round r
WHERE r.periodid = v_periodid
  AND r.transptype = 'ALL'
  AND r.vacid IN(SELECT vacid FROM res.res_release_publish WHERE rvloadid = p_rvloadid);
--** Tady je NOTICE **--
RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' periodid: ' || COALESCE(v_periodid::text, 'NULL') || ', IMS records (ALL): ' || i_cnt;
IF i_cnt = 0 THEN
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' No IMS values for periodid, exiting.';
  PERFORM rel_pub.log_msg('ERROR', 'fce:auto_rel_pub', 'rvloadid ' || p_rvloadid || ': no IMS values in res_online_ea_output_round for periodid ' || COALESCE(v_periodid::text, 'NULL') || '.');
  RETURN;
END IF;
-- INSERT všech panelů releasu, pid a mid se berou z res.res_release_publish pro p_rvloadid.
-- Tabulka crp_panel_full obsahuje seznam panelů s jejich geometrií (tabulka s panely a jejich kolaci, vzdalenost maxdist + 200m)
--** Tady je NOTICE **--
RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Create all panels list (crp_panel_full)';
TRUNCATE rel_pub.crp_panel_full;
-- crp_no_calc obsahuje nespočítané panely jen aktuálního běhu.
TRUNCATE rel_pub.crp_no_calc;
INSERT INTO 
  rel_pub.crp_panel_full
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
WHERE 
  a.mid IN(SELECT mid FROM res.res_release_publish WHERE rvloadid = p_rvloadid)
;
-- Odtranění duplicit v seznamu panelů.
TRUNCATE TABLE rel_pub.crp_geom_panel_temp;
INSERT INTO rel_pub.crp_geom_panel_temp SELECT DISTINCT * FROM rel_pub.crp_panel_full;
TRUNCATE TABLE rel_pub.crp_panel_full;
INSERT INTO rel_pub.crp_panel_full SELECT * FROM rel_pub.crp_geom_panel_temp;
-- Test, jestli je co zpracovat, jinak se to skočí.
SELECT COUNT(*) INTO i_cnt FROM rel_pub.crp_panel_full;
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
    rel_pub.crp_panel_full x
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
      vai.vai_vac_res v JOIN rel_pub.crp_panel_full p ON v.mid = p.mid
    WHERE
      -- Jen vacidy z releasu, ne poslední výpočet.
      v.vacid IN(SELECT vacid FROM res.res_release_publish WHERE rvloadid = p_rvloadid)
      --AND p.mid = 53837
    ORDER BY
      v.mid,
      v.vacid DESC
      ) sqry
    WHERE x.mid = sqry.mid
  ;
  
--** Tady je NOTICE **--	
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Updating VACs.';
	
  --Doupdate VAC k panelům, bere se transptype = 'ALL' a periodid = v_periodid (původně vždy 1, proto se pole jmenuje "vac_all_1")
  UPDATE
    rel_pub.crp_panel_full x
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
      r.periodid = v_periodid
      AND r.transptype = 'ALL'
      AND v.vacid IN(SELECT vacid FROM res.res_release_publish WHERE rvloadid = p_rvloadid)
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
    rel_pub.crp_panel_full 
  WHERE
    (vac_all_1 = 0
    OR vac_all_1 IS NULL
    OR err_vac_res IS NOT NULL)
    AND pid NOT IN (SELECT pid FROM rel_pub.crp_no_calc)
  ;
  -- A vyřadím je z dalších akcí.
	DELETE FROM rel_pub.crp_panel_full WHERE pid IN (SELECT pid FROM rel_pub.crp_no_calc);
  
--** Tady je NOTICE **--	
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Create IMS input.';
	-- Zde se vytvoří crp_ims_input
  PERFORM rel_pub.create_ims_input_ccf(p_rvloadid, v_periodid);

--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Checking panels with zero rots.';

  -- Panely s nulovým (nebo chybějícím) rots nelze zpracovat - dělení nulou v update ims_va/ts_va níže.
  -- Zaevidují se do crp_no_calc a vyřadí z běhu.
  INSERT INTO rel_pub.crp_no_calc (pid, err_vac_res)
  SELECT DISTINCT
    i.pid,
    COALESCE(i.err_vac_res, 'rots_week_all = 0')
  FROM
    rel_pub.crp_ims_input i
  WHERE
    (i.rots_week_all = 0 OR i.rots_week_all IS NULL)
    AND i.pid NOT IN (SELECT pid FROM rel_pub.crp_no_calc)
  ;

  GET DIAGNOSTICS i_cnt = ROW_COUNT;

  -- Pořadí je důležité, crp_ims_input se maže až nakonec.
  DELETE FROM rel_pub.crp_panel_full
  WHERE pid IN (SELECT pid FROM rel_pub.crp_ims_input
                WHERE rots_week_all = 0 OR rots_week_all IS NULL);

  DELETE FROM rel_pub.crp_ims_input
  WHERE rots_week_all = 0 OR rots_week_all IS NULL;

--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Panels with zero rots moved to crp_no_calc: ' || i_cnt || '.';
--** Tady je NOTICE **--	
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Calculate intersection.';
  -- Výpočet všech intersekcí, prusecik kolacu bufferu a tripu.
  TRUNCATE rel_pub.crp_all_intersection;
  INSERT INTO rel_pub.crp_all_intersection
  	(mid, sid, week_day_name, week_day_type)
  SELECT
    a.mid, b.sid, b.week_day_name, b.week_day_type
  FROM
    rel_pub.crp_panel_full a
    INNER JOIN rel_pub.crp_routing b 
      ON a.buffer_geom && b.geom       					-- Indexový test obálek
      AND ST_Intersects(a.buffer_geom, b.geom)	-- Přesný prostorový test
  ;
	
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Update intersection type.';
  
  --Doupdate 'ORIGINAL' a 'SUBSTITUTE' v hlavní tabulce podle spočítaných intersekcí.
  --Panely, které nemají v crp_all_intersection záznam se musí dohledat (níže).
  --Doupdate 'ORIGINAL'
  UPDATE rel_pub.crp_panel_full
  SET intersection_type = 'ORIGINAL'
  WHERE mid IN(SELECT DISTINCT mid FROM rel_pub.crp_all_intersection);
	--Doupdate 'SUBSTITUTE'
  UPDATE rel_pub.crp_panel_full
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
    rel_pub.crp_panel_full 
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
          rel_pub.crp_panel_full t1 -- Používáme implicitní JOIN pro KNN dotaz
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
    ims_va   = it.vac_week_all::DOUBLE PRECISION / NULLIF(it.rots_week_all, 0)
  FROM rel_pub.crp_ims_input AS it 
  WHERE trt.faceid = it.faceid_db;
  
  UPDATE rel_pub.crp_ts_rots_temp trt
	SET ts_va = ims_vac::DOUBLE PRECISION / NULLIF(ts_rots, 0);
  
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
				  				JOIN rel_pub.crp_panel_full cap ON cco.pid = cap.pid
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
    'Full load, rvloadid ' || p_rvloadid || ', periodid ' || v_periodid
  FROM 
    rel_pub.crp_panel_full
  ;
  
--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Log of processed panels.';

  -- Publish pro nové CRP --------------------------------------------------------------------------------------------
  -- Jen při p_publish = true. Celý obsah 102 a 103 se nahradí všemi facemi releasu p_rvloadid
  -- (vč. digitálních fací s pozicí > 1) s IMS hodnotami z res_online_ea_output_round.
  IF p_publish THEN
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Publishing to rvloadid 102, 103.';

    DELETE FROM 
      res.res_release_publish
    WHERE
      rvloadid IN(102, 103)
    ;

    -- Continual Release IDS
    INSERT INTO res.res_release_publish 
    (
      rvloadid, facepid, vacid, selsetid, dayicid, hourcid, fdurid, periodid, modeltype, transptype,
      rminvacver, vac, rots, va, pop_total, grp_vac_total, grp_rots_total, pid, mid, faceid
    )
    SELECT
      102::integer as rvloadid, --!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      f.facepid,
      r.vacid,
      r.selsetid,
      r.dayicid,
      r.hourcid,
      r.fdurid,
      r.periodid,
      r.modeltype,
      r.transptype,
      r.rminvacver,
      r.vac,
      r.rots,
      r.va,
      r.pop_total,
      r.grp_vac_total,
      r.grp_rots_total,
      f.pid,
      f.mid,
      f.faceid
    FROM
      (SELECT DISTINCT facepid, vacid, pid, mid, faceid
       FROM res.res_release_publish
       WHERE rvloadid = p_rvloadid) f
      JOIN res.res_online_ea_output_round r ON r.vacid = f.vacid
    WHERE
      r.periodid = 1
    ;

    GET DIAGNOSTICS i_pub_ids = ROW_COUNT;
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Records published for rvload_id = 102. Affected rows: ' || i_pub_ids;

    -- Continual Release IMS
    INSERT INTO res.res_release_publish 
    (
      rvloadid, facepid, vacid, selsetid, dayicid, hourcid, fdurid, periodid, modeltype, transptype,
      rminvacver, vac, rots, va, pop_total, grp_vac_total, grp_rots_total, pid, mid, faceid
    )
    SELECT
      103::integer as rvloadid, --!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      f.facepid,
      r.vacid,
      r.selsetid,
      r.dayicid,
      r.hourcid,
      r.fdurid,
      r.periodid,
      r.modeltype,
      r.transptype,
      r.rminvacver,
      r.vac,
      r.rots,
      r.va,
      r.pop_total,
      r.grp_vac_total,
      r.grp_rots_total,
      f.pid,
      f.mid,
      f.faceid
    FROM
      (SELECT DISTINCT facepid, vacid, pid, mid, faceid
       FROM res.res_release_publish
       WHERE rvloadid = p_rvloadid) f
      JOIN res.res_online_ea_output_round r ON r.vacid = f.vacid
    WHERE
      r.periodid = 200
    ;

    GET DIAGNOSTICS i_pub_ims = ROW_COUNT;
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Records published for rvload_id = 103. Affected rows: ' || i_pub_ims;

    -- Cache spotplánů pro 102 --------------------------------------------------------------------------------------
    -- Po plném publish jsou v cache staré vacidy. Spotplány, které v cache byly, se zapamatují,
    -- cache se smaže a spotplány se spočítají znovu oficiální campaign funkcí (stejně jako by to udělalo IDS).
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Spotplan cache 102 refresh.';

    TRUNCATE rel_pub.crp_spotplans;

    INSERT INTO rel_pub.crp_spotplans
    SELECT a.rvloadid, a.spot_length, a.spot_pause 
    FROM ids.ids_vac_calc_campaign_cache2 a 
    WHERE a.rvloadid = 102
    GROUP BY a.rvloadid, a.spot_length, a.spot_pause
    ORDER BY a.spot_length, a.spot_pause
    ;

    -- Odlogujeme mazané záznamy
    INSERT INTO rel_pub.crp_cache_del_log (
        rvloadid, faceid, spot_length, spot_pause, vacid, facepid, 
        vac, rots, vac_orig, rots_orig, group_def, period_hours
    )
    SELECT 
        rvloadid, faceid, spot_length, spot_pause, vacid, facepid, 
        vac, rots, vac_orig, rots_orig, group_def, period_hours
    FROM ids.ids_vac_calc_campaign_cache2
    WHERE rvloadid = 102;

    DELETE FROM ids.ids_vac_calc_campaign_cache2 WHERE rvloadid = 102;

    FOR r_spot IN
      SELECT spot_length, spot_pause FROM rel_pub.crp_spotplans ORDER BY spot_length, spot_pause
    LOOP
      SELECT COUNT(*), SUM(CASE WHEN c.vacid = -999 THEN 1 ELSE 0 END)
      INTO v_rows, v_err
      FROM vac8s._f_vac_calc_ids_campaign(102, r_spot.spot_length::integer, r_spot.spot_pause::integer) c;

      i_cache_spot := i_cache_spot + 1;
      IF COALESCE(v_err, 0) > 0 THEN
        i_cache_err := i_cache_err + 1;
        RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Spotplan cache 102 (' || r_spot.spot_length || ', ' || r_spot.spot_pause || ') failed.';
      ELSE
        i_cache_rows := i_cache_rows + v_rows;
      END IF;
    END LOOP;

--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Spotplan cache 102: spotplans ' || i_cache_spot || ', rows ' || i_cache_rows || ', failed ' || i_cache_err || '.';
    IF i_cache_err > 0 THEN
      PERFORM rel_pub.log_msg('WARNING', 'fce:auto_rel_pub', 'Spotplan cache 102: ' || i_cache_err || ' of ' || i_cache_spot || ' spotplans failed in vac8s._f_vac_calc_ids_campaign.');
    END IF;
  END IF;

-- Znovu se načte, některé panely mohly být vyřazeny
	SELECT COUNT(*) INTO i_cnt FROM rel_pub.crp_panel_full;
--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Load ID: ' || last_load_id || ' - successfuly done. Num of recs: ' || i_cnt || '.';
  PERFORM rel_pub.log_msg('INFO', 'fce:auto_rel_pub', 'Load ID: ' || last_load_id || ' (rvloadid ' || p_rvloadid || ', periodid ' || v_periodid || ') - successfuly done. Num of recs: ' || i_cnt || '.'
    || CASE WHEN p_publish THEN ' Published 102: ' || i_pub_ids || ', 103: ' || i_pub_ims || '. Spotplan cache 102: '
                                || i_cache_spot || ' spotplans, ' || i_cache_rows || ' rows.' ELSE ' Not published.' END);
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

COMMIT;
