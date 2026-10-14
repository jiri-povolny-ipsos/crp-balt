--
-- rel_pub.re_publish_crp() - přepnutí na nové CRP (rvloadid 102, 103)
--
-- Vytvořil: Jiří Povolný
-- Vytvořeno dne: 14.10.2026
--
-- Vychází z exportu ddl/schema_rel_pub.sql (v DB k 14.10.2026 obsahově shodná). Změny (podrobně v docstringu):
--   1) rvloadid 100/101 -> 102/103
--   2) publish všech fací (i pozice 2 digitálních panelů)
--   3) živé panely včetně ownid = subownid
--   4) po publish obnova cache spotplánů 102 campaign funkcí (~3 min)
--   5) oprava zdroje v logu chyb
--
-- Nasazení: spustit celý soubor. Funkce se nahradí atomicky, nic se nespouští.
-- Rollback: nasadit původní definici z ddl/schema_rel_pub.sql (re_publish_crp, od řádku 2394).
--

BEGIN;

CREATE OR REPLACE FUNCTION rel_pub.re_publish_crp() RETURNS void
    LANGUAGE plpgsql
    AS $$
/**********************************************************************************************
*
*	Vyrobil: Jirka Povolný, 10.03.2026
*
*	Změnil: Jiří Povolný
*	Změněno dne: 14.10.2026
*	Popis změny:
*	  1) Nové CRP: rvloadid 102 (IDS, periodid 1) a 103 (IMS, periodid 200) místo 100 a 101.
*	  2) Publikují se všechny face, u digitálních panelů i pozice 2 (stejně jako
*	     auto_rel_pub_inc a auto_rel_pub_ccf).
*	  3) Panely podle kritéria procesu včetně ownid = subownid (dřív jen status 3, timeattid 0,
*	     takže by se publikovaly i panely, které CRP vyřazuje).
*	  4) Po publish se obnoví cache spotplánů ids.ids_vac_calc_campaign_cache2 pro 102 - smaže
*	     se (log do crp_cache_del_log) a spotplány, které v ní byly, se spočítají znovu
*	     funkcí vac8s._f_vac_calc_ids_campaign. Jinak by v cache zůstaly staré vacidy a nové
*	     by chyběly (campaign funkce neprázdnou cache spotplánu nepřepočítá).
*	  5) Oprava zdroje v logu chyb (bylo 'auto_rel_pub_inc').
*
*	Přepláchne res_release_publish pro rvloadid 102, 103 (aktuální výpočty živých panelů,
*	bez CCF výpočtu). Spouští se ručně: SELECT rel_pub.re_publish_crp();
*
**********************************************************************************************/
DECLARE
  exe_str varchar;			-- String pro poskládání SQL příkazů
  last_load_id integer;	-- Id posledního loadu do etl tabulky pro IDS
  i_cnt integer;				-- Počet záznamů ke zpracování
  navteqver_str varchar;-- Poslední navteqver z params()
  r_spot record;        -- Spotplán pro přepočet cache
  i_cache_rows integer := 0; -- Počet řádků spočítaných do cache 102
  i_cache_spot integer := 0; -- Počet přepočítaných spotplánů cache 102
  i_cache_err integer := 0;  -- Počet spotplánů, u kterých campaign funkce vrátila chybu
  v_rows integer;
  v_err integer;
  
BEGIN
-------------------------------------------------------------------------------------------------------------------
--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Function re_publish_crp start.';

--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Publishing process start.';
  -- Automatický publish --------------------------------------------------------------------------------------------------
  -- Delete publishe, pokud panely už mají publikované výsledky
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Unpublish 102, 103 in res_release_publish.';

  DELETE FROM 
    res.res_release_publish
  WHERE
    rvloadid IN(102, 103)
  ;
  
  GET DIAGNOSTICS i_cnt = ROW_COUNT;
  RAISE NOTICE 'Records deleted from publish: %', i_cnt;
 
  -- Continual Release IDS (nové CRP, rvloadid 102)
  INSERT INTO res.res_release_publish 
  SELECT DISTINCT ON (sqry.facepid, sqry.transptype)
    102::integer as rvloadid, --!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
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
            	nm.status = 3 AND nm.timeattid = 0 AND nm.ownid = nm.subownid
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
    -- Publikují se všechny face, u digitálních panelů i pozice 2 (stejně jako release).
  ORDER BY
    sqry.facepid, sqry.transptype, sqry.vacid DESC
  ;

--** Tady je NOTICE **--
	GET DIAGNOSTICS i_cnt = ROW_COUNT;
  RAISE NOTICE 'Records (rvloadid = 102, IDS) inserted to publish: %', i_cnt;

  -- Continual Release IMS (nové CRP, rvloadid 103)
  INSERT INTO res.res_release_publish 
  SELECT DISTINCT ON (sqry.facepid, sqry.transptype, sqry.periodid)
    103::integer as rvloadid, --!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
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
            	nm.status = 3 AND nm.timeattid = 0 AND nm.ownid = nm.subownid
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
    -- Publikují se všechny face, u digitálních panelů i pozice 2 (stejně jako release).
  ORDER BY
    sqry.facepid, sqry.transptype, sqry.periodid, sqry.vacid DESC
  ;

--** Tady je NOTICE **--
	GET DIAGNOSTICS i_cnt = ROW_COUNT;
  RAISE NOTICE 'Records (rvloadid = 103, IMS) inserted to publish: %', i_cnt;

  -- Cache spotplánů pro 102 ------------------------------------------------------------------------------------------
  -- Spotplány, které v cache byly, se zapamatují, cache se smaže a spotplány se spočítají znovu
  -- oficiální campaign funkcí (stejně jako by to udělalo IDS).
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Spotplan cache 102 refresh.';

  TRUNCATE rel_pub.crp_spotplans;

  INSERT INTO rel_pub.crp_spotplans
  SELECT a.rvloadid, a.spot_length, a.spot_pause
  FROM ids.ids_vac_calc_campaign_cache2 a
  WHERE a.rvloadid = 102
  GROUP BY a.rvloadid, a.spot_length, a.spot_pause
  ORDER BY a.spot_length, a.spot_pause;

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
    PERFORM rel_pub.log_msg('WARNING', 'fce:re_publish_crp', 'Spotplan cache 102: ' || i_cache_err || ' of ' || i_cache_spot || ' spotplans failed in vac8s._f_vac_calc_ids_campaign.');
  END IF;
  PERFORM rel_pub.log_msg('INFO', 'fce:re_publish_crp', 'Republished 102, 103. Spotplan cache 102: ' || i_cache_spot || ' spotplans, ' || i_cache_rows || ' rows.');

--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' The end.';

  EXCEPTION
  WHEN others THEN
    RAISE NOTICE 'exception: %', SQLERRM;
    PERFORM rel_pub.log_msg('ERROR', 're_publish_crp', 'Error occured: ' || SQLERRM);

END;
$$;

COMMIT;
