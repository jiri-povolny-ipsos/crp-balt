/*==================================================================================================
|| 01 - Kontrola předpokladů před nasazením ids.f_spotplan_cache_update na BALT (imsbalt_prod)
|| - všechny dotazy jsou jen SELECT (nic nemění)
|| - spusť PŘED skriptem 02, výsledky vyhodnoť podle poznámek u každé sekce
|| - IDS rvloadid na BALT = 100 (IMS = 101); cache se plní jen pro 100
||
|| Vytvořil: Jiří Povolný
|| Vytvořeno dne: 2026-10-13
||================================================================================================*/


/*--------------------------------------------------------------------------------------------------
|| 1. Existence použitých objektů (musí vyjít 10 řádků, u všech exists = true)
||------------------------------------------------------------------------------------------------*/
SELECT o.objekt,
       EXISTS (SELECT 1
                 FROM pg_class c
                 JOIN pg_namespace n ON (n.oid = c.relnamespace)
                WHERE n.nspname = o.schema_name
                  AND c.relname = o.name
                  AND c.relkind IN ('r', 'v', 'm')) AS "exists"
  FROM (SELECT 'ids.ids_vac_calc_campaign_cache2' AS objekt, 'ids' AS schema_name, 'ids_vac_calc_campaign_cache2' AS name
        UNION ALL SELECT 'vai_subl.vai_subl_route_plan_dynva', 'vai_subl', 'vai_subl_route_plan_dynva'
        UNION ALL SELECT 'vai.vai_vac_res', 'vai', 'vai_vac_res'
        UNION ALL SELECT 'vai.vai_release_vac_static_load', 'vai', 'vai_release_vac_static_load'
        UNION ALL SELECT 'pnl.pnl_facemid', 'pnl', 'pnl_facemid'
        UNION ALL SELECT 'pnl.pnl_main', 'pnl', 'pnl_main'
        UNION ALL SELECT 'res.res_release_publish', 'res', 'res_release_publish'
        UNION ALL SELECT 'res.res_online_ea_output', 'res', 'res_online_ea_output'
        UNION ALL SELECT 'res.res_period', 'res', 'res_period') o
UNION ALL
SELECT 'vac8s._f_vac_calc_ids_dynvac_fepe',
       EXISTS (SELECT 1
                 FROM pg_proc p
                 JOIN pg_namespace n ON (n.oid = p.pronamespace)
                WHERE n.nspname = 'vac8s'
                  AND p.proname = '_f_vac_calc_ids_dynvac_fepe')
UNION ALL
SELECT 'vac8s.f_params',
       EXISTS (SELECT 1
                 FROM pg_proc p
                 JOIN pg_namespace n ON (n.oid = p.pronamespace)
                WHERE n.nspname = 'vac8s'
                  AND p.proname = 'f_params');


/*--------------------------------------------------------------------------------------------------
|| 2. Sloupce cache tabulky (funkce vkládá 12 sloupců, všechny musí existovat)
|| Očekávané: rvloadid, faceid, spot_length, spot_pause, vacid, facepid, vac, rots, vac_orig,
||            rots_orig, group_def, period_hours
||------------------------------------------------------------------------------------------------*/
SELECT c.ordinal_position,
       c.column_name,
       c.data_type
  FROM information_schema.columns c
 WHERE c.table_schema = 'ids'
   AND c.table_name = 'ids_vac_calc_campaign_cache2'
 ORDER BY 1;

-- Triggery a indexy cache (unikátní index by chránil proti duplicitám; SE jej nemá)
SELECT tgname AS trigger_name FROM pg_trigger WHERE tgrelid = 'ids.ids_vac_calc_campaign_cache2'::regclass AND NOT tgisinternal;
SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'ids' AND tablename = 'ids_vac_calc_campaign_cache2';


/*--------------------------------------------------------------------------------------------------
|| 3. Prostředí rvloadid a metoda výpočtu
|| Očekávání: rvloadid 100 má pnlvaienv = 'EA', dynva_method = 'FEPE'.
|| Jiná hodnota = funkce skončí výjimkou.
||------------------------------------------------------------------------------------------------*/
SELECT rvloadid, pnlvaienv
  FROM vai.vai_release_vac_static_load
 WHERE rvloadid IN (100, 101)
 ORDER BY 1;

SELECT dynva_method FROM vac8s.f_params();


/*--------------------------------------------------------------------------------------------------
|| 4. Hodnota pnlmotion digitálních panelů (KLÍČOVÉ)
|| BALT kód (f_fill_crp_cache_temp, campaign funkce) používá 'D'; SE používá 'G'.
|| Z druhého dotazu musí být vidět, že cache dnes pokrývá právě ten typ, který je ve funkci
|| v proměnné sPnlmotion. Jiný typ -> uprav sPnlmotion ve skriptu 02.
||------------------------------------------------------------------------------------------------*/
SELECT * FROM code.code_pnlmotion;

SELECT m.pnlmotion,
       COUNT(DISTINCT p.vacid) AS vacidu_v_publish,
       COUNT(DISTINCT c.vacid) AS z_toho_v_cache
  FROM res.res_release_publish p
  JOIN pnl.pnl_main m ON (m.mid = p.mid)
  LEFT JOIN ids.ids_vac_calc_campaign_cache2 c ON (c.rvloadid = p.rvloadid AND c.vacid = p.vacid)
 WHERE p.rvloadid = 100
   AND p.transptype = 'ALL'
 GROUP BY 1
 ORDER BY 1;

-- Typy pnlmotion, které mají trasy v route_plan_dynva
SELECT pnlmotion, COUNT(*) AS radku FROM vai_subl.vai_subl_route_plan_dynva GROUP BY 1 ORDER BY 1;


/*--------------------------------------------------------------------------------------------------
|| 5. Stav cache pro rvloadid 100
||------------------------------------------------------------------------------------------------*/
SELECT rvloadid,
       COUNT(*) AS radku,
       COUNT(DISTINCT vacid) AS vacidu,
       COUNT(DISTINCT (spot_length, spot_pause)) AS spotplanu
  FROM ids.ids_vac_calc_campaign_cache2
 GROUP BY 1
 ORDER BY 1;

-- Pokud je cache pro rvloadid 100 PRÁZDNÁ, funkce nemá odkud vzít spotplány a nic nevloží.
-- Duplicity v cache (musí vrátit 0 řádků)
SELECT rvloadid, spot_length, spot_pause, vacid, COUNT(*) AS pocet
  FROM ids.ids_vac_calc_campaign_cache2
 WHERE rvloadid = 100
 GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1;

-- Vacidy digitálních panelů v publish, které v cache chybí (ZMĚŇ 'D' dle sekce 4)
SELECT COUNT(DISTINCT p.vacid) AS chybi_v_cache
  FROM res.res_release_publish p
  JOIN pnl.pnl_main m ON (m.mid = p.mid)
 WHERE p.rvloadid = 100
   AND m.pnlmotion = 'D'
   AND NOT EXISTS (SELECT 1 FROM ids.ids_vac_calc_campaign_cache2 c WHERE c.rvloadid = p.rvloadid AND c.vacid = p.vacid);

-- Detail: rsid = 0 znamená panel bez trasy -> nelze spočítat, řádek v cache nikdy nevznikne
SELECT DISTINCT p.vacid, p.facepid, p.faceid, b.rsid,
       (SELECT COUNT(*) FROM vai_subl.vai_subl_route_plan_dynva a WHERE a.rsid = b.rsid) AS radku_route_plan
  FROM res.res_release_publish p
  JOIN pnl.pnl_main m ON (m.mid = p.mid)
  LEFT JOIN vai.vai_vac_res b ON (b.vacid = p.vacid)
 WHERE p.rvloadid = 100
   AND m.pnlmotion = 'D'
   AND NOT EXISTS (SELECT 1 FROM ids.ids_vac_calc_campaign_cache2 c WHERE c.rvloadid = p.rvloadid AND c.vacid = p.vacid)
 ORDER BY 1
 LIMIT 100;

-- Zastaralé vacidy v cache (v publish už nejsou) - funkce je v automatickém režimu smaže
SELECT c.vacid, c.facepid, COUNT(*) AS radku
  FROM ids.ids_vac_calc_campaign_cache2 c
 WHERE c.rvloadid = 100
   AND NOT EXISTS (SELECT 1 FROM res.res_release_publish p WHERE p.rvloadid = c.rvloadid AND p.vacid = c.vacid)
 GROUP BY 1, 2
 ORDER BY 1;


/*--------------------------------------------------------------------------------------------------
|| 6. Předpoklady výpočtu (všechny čtyři čísla musí být 0)
|| Funkce agreguje po vacidu, campaign funkce po rsid; shodné jsou jen při mapování vacid <-> rsid 1:1
|| a jedné periodě na vacid.
||------------------------------------------------------------------------------------------------*/
SELECT 'vacid s vice rsid' AS kontrola, COUNT(*) AS pocet
  FROM (SELECT b.vacid
          FROM vai.vai_vac_res b
         WHERE b.vacid IN (SELECT vacid FROM res.res_release_publish WHERE rvloadid = 100)
         GROUP BY 1
        HAVING COUNT(DISTINCT b.rsid) > 1) x
UNION ALL
SELECT 'nenulove rsid s vice vacidy', COUNT(*)
  FROM (SELECT b.rsid
          FROM vai.vai_vac_res b
         WHERE b.rsid <> 0
           AND b.vacid IN (SELECT vacid FROM res.res_release_publish WHERE rvloadid = 100)
         GROUP BY 1
        HAVING COUNT(DISTINCT b.vacid) > 1) x
UNION ALL
SELECT 'vacid s vice periodami (EA output)', COUNT(*)
  FROM (SELECT vacid
          FROM res.res_online_ea_output
         WHERE vacid IN (SELECT vacid FROM res.res_release_publish WHERE rvloadid = 100)
         GROUP BY 1
        HAVING COUNT(DISTINCT periodid) > 1) x
UNION ALL
SELECT 'vacid s vice faceid', COUNT(*)
  FROM (SELECT b.vacid
          FROM vai.vai_vac_res b
         WHERE b.vacid IN (SELECT vacid FROM res.res_release_publish WHERE rvloadid = 100)
         GROUP BY 1
        HAVING COUNT(DISTINCT b.faceid) > 1) x;
