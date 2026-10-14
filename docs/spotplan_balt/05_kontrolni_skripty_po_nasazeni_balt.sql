/*==================================================================================================
|| 05 - Ověření ids.f_spotplan_cache_update po nasazení na BALT (imsbalt_prod)
|| - sekce 1 a 3 jsou jen SELECT
|| - sekce 2 je zkušební běh v transakci, která se vrací zpět (ROLLBACK) - do cache nic nezůstane
|| - IDS rvloadid = 100, digitální panely pnlmotion = 'D' (viz skript 01, sekce 4)
||
|| Vytvořil: Jiří Povolný
|| Vytvořeno dne: 2026-10-13
||================================================================================================*/


/*--------------------------------------------------------------------------------------------------
|| 1. Ověření výpočtu proti hodnotám v cache
|| Dotaz z funkce přepočítá vzorek vacidů jednoho spotplánu a porovná je s cache.
|| Všude musí vyjít ok = true. Jiný spotplán / vzorek: změň CTE "param".
||------------------------------------------------------------------------------------------------*/
WITH param AS (
    SELECT 100 AS rvloadid,
           (SELECT MIN(spot_length) FROM ids.ids_vac_calc_campaign_cache2 WHERE rvloadid = 100) AS spot_length,
           (SELECT MIN(spot_pause) FROM ids.ids_vac_calc_campaign_cache2 c
             WHERE c.rvloadid = 100
               AND c.spot_length = (SELECT MIN(spot_length) FROM ids.ids_vac_calc_campaign_cache2 WHERE rvloadid = 100)) AS spot_pause
),
vzorek AS (
    SELECT c.vacid
      FROM ids.ids_vac_calc_campaign_cache2 c
      JOIN param p ON (c.rvloadid = p.rvloadid AND c.spot_length = p.spot_length AND c.spot_pause = p.spot_pause)
     ORDER BY c.vacid
     LIMIT 20
),
vypocet AS (
    SELECT MAX(t.faceid) AS faceid,
           t.vacid,
           MAX(t.facepid) AS facepid,
           SUM(t.dynva * t.fa_calc) AS vac,
           ROUND(CAST(SUM(t.fa_calc) AS numeric), 0) AS rots,
           MAX(t.period_hours) AS period_hours
      FROM (SELECT c.facepid,
                   b.faceid,
                   b.vacid,
                   a.fa_calc,
                   vac8s._f_vac_calc_ids_dynvac_fepe(a.transptype, a.pnlmotion, a.route_kcd, a.route_va,
                       p.spot_length, p.spot_length + p.spot_pause, a.daf_angle) AS dynva,
                   f.period_hours
              FROM param p,
                   vai_subl.vai_subl_route_plan_dynva a
              JOIN vai.vai_vac_res b ON (a.rsid = b.rsid)
              JOIN pnl.pnl_facemid c ON (b.faceid = c.faceid)
              JOIN (SELECT DISTINCT o.vacid, o.periodid
                      FROM res.res_online_ea_output o
                     WHERE o.vacid IN (SELECT vacid FROM vzorek)) e ON (e.vacid = b.vacid)
              JOIN res.res_period f ON (f.periodid = e.periodid)
             WHERE a.pnlmotion = 'D'
               AND a.route_va > 0
               AND a.fa_calc > 0
               AND b.vacid IN (SELECT vacid FROM vzorek)) t
     GROUP BY t.vacid
)
SELECT v.vacid,
       v.vac AS vac_vypocet,
       c.vac AS vac_cache,
       v.rots AS rots_vypocet,
       c.rots AS rots_cache,
       (v.faceid = c.faceid
        AND v.facepid = c.facepid
        AND v.period_hours = c.period_hours
        AND ABS(v.vac - c.vac) < 1e-9
        AND v.rots = c.rots::numeric) AS ok
  FROM vypocet v
  JOIN param p ON (TRUE)
  LEFT JOIN ids.ids_vac_calc_campaign_cache2 c ON (c.rvloadid = p.rvloadid
                                               AND c.spot_length = p.spot_length
                                               AND c.spot_pause = p.spot_pause
                                               AND c.vacid = v.vacid)
 ORDER BY v.vacid;


/*--------------------------------------------------------------------------------------------------
|| 2. Zkušební běh funkce v transakci (po ROLLBACK je cache beze změny)
|| Spouštěj jako celek (psql / jedno spojení bez autocommitu). Očekávání:
||   - A: počet řádků vložených funkcí = (počet chybějících vacidů s trasou) x (počet spotplánů)
||   - B: druhé volání hned po prvním vrátí 0 (idempotence)
||   - C: duplicity v cache = 0 řádků
||   - D: chybějících a zastaralých vacidů po běhu = 0 (kromě vacidů bez trasy, rsid = 0)
||------------------------------------------------------------------------------------------------*/
/*
BEGIN;

SELECT COUNT(*) AS radku_pred FROM ids.ids_vac_calc_campaign_cache2 WHERE rvloadid = 100;

SELECT ids.f_spotplan_cache_update(100) AS a_vlozeno_poprve;
SELECT ids.f_spotplan_cache_update(100) AS b_vlozeno_podruhe;   -- musí být 0

SELECT COUNT(*) AS radku_po FROM ids.ids_vac_calc_campaign_cache2 WHERE rvloadid = 100;

SELECT rvloadid, spot_length, spot_pause, vacid, COUNT(*) AS c_pocet
  FROM ids.ids_vac_calc_campaign_cache2
 WHERE rvloadid = 100
 GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1;                                             -- musí být prázdné

SELECT COUNT(DISTINCT p.vacid) AS d_chybi_v_cache
  FROM res.res_release_publish p
  JOIN pnl.pnl_main m ON (m.mid = p.mid)
 WHERE p.rvloadid = 100 AND m.pnlmotion = 'D'
   AND NOT EXISTS (SELECT 1 FROM ids.ids_vac_calc_campaign_cache2 c WHERE c.rvloadid = p.rvloadid AND c.vacid = p.vacid);

ROLLBACK;
*/


/*--------------------------------------------------------------------------------------------------
|| 3. Ruční režim (jen vybrané vacidy) - zkušební běh, vždy s ROLLBACK
|| Za <vacid1>, <vacid2> dosaď existující vacidy z cache. Očekávání: počet vložených řádků =
|| počet spotplánů x počet vacidů (s trasou) a hodnoty se shodují s původními.
||------------------------------------------------------------------------------------------------*/
/*
BEGIN;
SELECT ids.f_spotplan_cache_update(100, ARRAY[<vacid1>, <vacid2>]::bigint[]);
SELECT spot_length, spot_pause, vacid, vac, rots FROM ids.ids_vac_calc_campaign_cache2
 WHERE rvloadid = 100 AND vacid IN (<vacid1>, <vacid2>) ORDER BY 1, 2, 3 LIMIT 20;
ROLLBACK;
*/


/*--------------------------------------------------------------------------------------------------
|| 4. Po prvním ostrém běhu CRP: kontrola logu a stavu
||------------------------------------------------------------------------------------------------*/
SELECT *
  FROM rel_pub.crp_log_table
 WHERE source IN ('auto_rel_pub_inc', 'fce:auto_rel_pub_inc')
   AND desc_text LIKE 'Spotplan cache%'
 ORDER BY 1 DESC
 LIMIT 20;
