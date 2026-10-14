--
-- Full CCF výpočet pro release rvloadid = 13 s publish do nového CRP (rvloadid 102, 103)
-- a kontrola výsledku - spouštění ručně, krok po kroku
--
-- Vytvořil: Jiří Povolný
-- Vytvořeno dne: 13.10.2026
-- Změnil: Jiří Povolný
-- Změněno dne: 13.10.2026
-- Popis změny:
--   1) úprava pro ruční spouštění po krocích v GUI klientovi (pgAdmin, DBeaver...) místo psql -f.
--      Odstraněny příkazy psql (\set, \timing), stav před během se ukládá do trvalé tabulky
--      x_jpovolny.crp_full13_pred (dočasná tabulka by se při změně spojení klienta ztratila),
--      kontroly se vážou na load vzniklý až po uložení stavu.
--   2) základem jsou panely releasu 13 (dřív 14, panely i vacidy jsou shodné), periodid = 1,
--      a běh publikuje do res.res_release_publish, rvloadid 102 (IDS) a 103 (IMS).
--      Soubor přejmenován z 2026-10-13_03_run_full_ccf_rvloadid14.sql.
--
-- Předpoklady (hotovo 13.10.2026): inkrementální proces zastavený, rel_pub.crp_panel_full založená,
-- vlna 3 v rel_pub.crp_routing a rel_pub.crp_resp_weight_input, nasazené funkce
-- rel_pub.auto_rel_pub_ccf(p_rvloadid, p_periodid, p_publish) a rel_pub.create_ims_input_ccf(p_rvloadid, p_periodid).
--
-- Jak spouštět:
--   - každý KROK zvlášť (označit příkaz a spustit), v uvedeném pořadí, s výsledkem kroku pokračovat dál
--   - klient v režimu autocommit; bez autocommitu po KROKU 1 a KROKU 3 dát COMMIT
--   - KROK 3 (výpočet) trvá dlouho, klient nesmí mít nastavený timeout dotazu
--
-- Výsledek:
--   - nový load v rel_pub.etl_ccf_rel_pub (processed = 0, IDS si full tabulku sám nebere)
--   - res.res_release_publish: celý obsah rvloadid 102 (IDS, periodid 1) a 103 (IMS, periodid 200)
--     nahrazený všemi facemi releasu 13 (9 340 fací vč. digitálních s pozicí > 1, po 5 transptype)
--
-- Pozor: auto_rel_pub_ccf má EXCEPTION blok, chyba se jen zaloguje do crp_log_table a funkce skončí
-- "úspěšně" s rollbackem celého výpočtu i publish. Výsledek je proto potřeba vždy ověřit v KROKU 4.
--


-- ====================================================================================================
-- KROK 0a: Předpoklady. Očekáváno: proběhne bez chyby. Při chybě dál nepokračovat.
-- ====================================================================================================
DO $$
DECLARE
  v bigint;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                 WHERE table_schema = 'rel_pub' AND table_name = 'crp_panel_full') THEN
    RAISE EXCEPTION 'Chybí tabulka rel_pub.crp_panel_full';
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
                 WHERE n.nspname = 'rel_pub' AND p.proname = 'auto_rel_pub_ccf'
                   AND pg_get_function_identity_arguments(p.oid) = 'p_rvloadid integer, p_periodid integer, p_publish boolean') THEN
    RAISE EXCEPTION 'Chybí funkce rel_pub.auto_rel_pub_ccf(p_rvloadid integer, p_periodid integer, p_publish boolean)';
  END IF;

  SELECT COUNT(*) INTO v FROM res.res_release_publish WHERE rvloadid = 13;
  IF v = 0 THEN
    RAISE EXCEPTION 'V res.res_release_publish není rvloadid = 13';
  END IF;

  SELECT COUNT(*) INTO v FROM vai.vai_release_vac_static_load WHERE rvloadid IN (102, 103);
  IF v <> 2 THEN
    RAISE EXCEPTION 'Ve vai.vai_release_vac_static_load chybí rvloadid 102 nebo 103';
  END IF;

  SELECT COUNT(*) INTO v FROM rel_pub.crp_routing WHERE wave = 'Spring2026';
  IF v <> 2878 THEN
    RAISE EXCEPTION 'V rel_pub.crp_routing je % tras vlny Spring2026, očekáváno 2878', v;
  END IF;

  SELECT COUNT(*) INTO v
  FROM rel_pub.crp_routing r
  WHERE NOT EXISTS (SELECT 1 FROM rel_pub.crp_resp_weight_input w WHERE w.respondent_id = r.respondent_id);
  IF v > 0 THEN
    RAISE EXCEPTION '% tras nemá váhu respondenta v rel_pub.crp_resp_weight_input', v;
  END IF;
END
$$;


-- ====================================================================================================
-- KROK 0b: Kontrola, že neběží jiný výpočet nad rel_pub. Očekáváno: žádný řádek.
-- ====================================================================================================
SELECT pid, usename, state, query_start, LEFT(query, 100) AS query
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND (query ILIKE '%auto_rel_pub_inc%' OR query ILIKE '%auto_rel_pub_ccf%')
  AND state <> 'idle';


-- ====================================================================================================
-- KROK 1: Uložení stavu před během. Očekáváno: tabulka s 1 řádkem.
-- Pokud tabulka už existuje (opakovaný běh), nejdřív: DROP TABLE x_jpovolny.crp_full13_pred;
-- ====================================================================================================
CREATE TABLE x_jpovolny.crp_full13_pred AS
SELECT
  now() AS ulozeno,
  (SELECT COALESCE(MAX(row_id), 0) FROM rel_pub.crp_log_table) AS max_log_row,
  (SELECT COALESCE(MAX(load_id), 0) FROM rel_pub.etl_ccf_rel_pub_loads) AS max_full_load,
  (SELECT COALESCE(MAX(load_id), 0) FROM rel_pub.etl_ccf_rel_pub_loads_inc) AS max_inc_load,
  (SELECT COUNT(*) FROM rel_pub.crp_all_panel) AS all_panel,
  (SELECT COUNT(*) FROM rel_pub.crp_missing) AS missing,
  (SELECT COUNT(*) FROM rel_pub.crp_routing) AS routing,
  (SELECT COUNT(*) FROM rel_pub.crp_resp_weight_input) AS resp_weight,
  (SELECT COUNT(*) FROM res.res_release_publish WHERE rvloadid = 102) AS publish_102,
  (SELECT COUNT(*) FROM res.res_release_publish WHERE rvloadid = 103) AS publish_103,
  (SELECT COUNT(*) FROM res.res_release_publish WHERE rvloadid NOT IN (102, 103)) AS publish_ostatni;


-- ====================================================================================================
-- KROK 2: Kontrola uloženého stavu.
-- Očekáváno: max_full_load = 6, routing = 22436, resp_weight = 7763, publish_102 = 0, publish_103 = 0.
-- ====================================================================================================
SELECT * FROM x_jpovolny.crp_full13_pred;


-- ====================================================================================================
-- KROK 3: Výpočet a publish do 102/103. Trvá dlouho. Průběh je vidět v hláškách (NOTICE) klienta.
-- ====================================================================================================
SELECT rel_pub.auto_rel_pub_ccf(13, 1, true);


-- ====================================================================================================
-- KROK 4: Kontroly výsledku
-- ====================================================================================================

-- 4a: Log běhu. Očekáváno: INFO 'The function completed successfully' (create_ims_input_ccf)
--     a INFO 'Load ID: N (rvloadid 13, periodid 1) - successfuly done. Num of recs: ... Published 102: 46700, 103: 46700.',
--     žádný ERROR.
SELECT l.row_id, l."timestamp", l.type, l.source, l.desc_text
FROM rel_pub.crp_log_table l, x_jpovolny.crp_full13_pred p
WHERE l.row_id > p.max_log_row
ORDER BY l.row_id;

-- 4b: Nový load. Očekáván právě jeden řádek, processed = 0. Pokud žádný, výpočet spadl (viz 4a).
SELECT ld.*
FROM rel_pub.etl_ccf_rel_pub_loads ld, x_jpovolny.crp_full13_pred p
WHERE ld.load_id > p.max_full_load;

-- 4c: Pokrytí releasu: panely release -> zpracované -> ve výstupu.
--     Očekáváno: pid_release 7102, pid_nespocitano 0, pid_vystup blízko pid_zpracovano.
SELECT
  (SELECT COUNT(DISTINCT pid) FROM res.res_release_publish WHERE rvloadid = 13) AS pid_release,
  (SELECT COUNT(*) FROM rel_pub.crp_panel_full) AS pid_zpracovano,
  (SELECT COUNT(*) FROM rel_pub.crp_no_calc) AS pid_nespocitano,
  COUNT(DISTINCT e.pid) AS pid_vystup,
  COUNT(DISTINCT e.faceid) AS face_vystup,
  COUNT(*) AS radku_vystup
FROM rel_pub.etl_ccf_rel_pub e, x_jpovolny.crp_full13_pred p
WHERE e.load_id > p.max_full_load;

-- 4d: Typ průsečíku zpracovaných panelů (ORIGINAL = vlastní trasy, SUBSTITUTE = trasy náhradního panelu)
SELECT intersection_type, COUNT(*) AS panelu
FROM rel_pub.crp_panel_full
GROUP BY intersection_type
ORDER BY intersection_type;

-- 4e: Zpracované panely, které nejsou ve výstupu (např. SUBSTITUTE bez náhradního panelu do 5 km)
SELECT f.intersection_type, COUNT(*) AS panelu_bez_vystupu
FROM rel_pub.crp_panel_full f
WHERE NOT EXISTS (SELECT 1 FROM rel_pub.etl_ccf_rel_pub e, x_jpovolny.crp_full13_pred p
                  WHERE e.load_id > p.max_full_load AND e.pid = f.pid)
GROUP BY f.intersection_type;

-- 4f: Nespočítané panely tohoto běhu. Očekáváno: žádný řádek (release 13 nemá nulové VAC/ROTS ani chyby).
SELECT * FROM rel_pub.crp_no_calc ORDER BY pid;

-- 4g: Výstup podle zemí, podíl respondentů vlny 3 (LAT9xxxxx, LIT8xxxxx). Očekáváno: vlna 3 jen u LAT a LIT.
SELECT
  e.country,
  COUNT(*) AS radku,
  COUNT(DISTINCT e.respondent_id) AS respondentu,
  COUNT(DISTINCT CASE WHEN e.respondent_id ~ '^(LAT9|LIT8)[0-9]{5}$' THEN e.respondent_id END) AS respondentu_vlna3,
  ROUND(SUM(e.ts_rots)::numeric) AS ts_rots,
  ROUND(SUM(e.vac)::numeric) AS vac
FROM rel_pub.etl_ccf_rel_pub e, x_jpovolny.crp_full13_pred p
WHERE e.load_id > p.max_full_load
GROUP BY e.country
ORDER BY e.country;

-- 4h: NULL, záporné nebo VA > 1. Očekáváno: 0, 0.
SELECT COUNT(*) AS radku, COUNT(DISTINCT e.pid) AS panelu
FROM rel_pub.etl_ccf_rel_pub e, x_jpovolny.crp_full13_pred p
WHERE e.load_id > p.max_full_load
  AND (e.va IS NULL OR e.vac IS NULL OR e.va < 0 OR e.vac < 0 OR e.va > 1);

-- 4i: vacid ve výstupu musí být z releasu. Očekáváno: 0.
SELECT COUNT(DISTINCT e.vacid) AS vacid_mimo_release
FROM rel_pub.etl_ccf_rel_pub e, x_jpovolny.crp_full13_pred p
WHERE e.load_id > p.max_full_load
  AND e.vacid NOT IN (SELECT vacid FROM res.res_release_publish WHERE rvloadid = 13);

-- 4j: VAC: výstup vs. IMS (crp_ts_rots_temp) vs. kontrolní součet vs. release 13 (ALL).
--     Očekáváno: všechny čtyři hodnoty téměř shodné, rozdíl výstup / IMS je jen zaokrouhlení round(vac) po řádcích.
SELECT
  (SELECT ROUND(SUM(e.vac)::numeric) FROM rel_pub.etl_ccf_rel_pub e, x_jpovolny.crp_full13_pred p
    WHERE e.load_id > p.max_full_load) AS vac_vystup,
  (SELECT ROUND(SUM(ims_vac)::numeric) FROM rel_pub.crp_ts_rots_temp) AS vac_ims,
  (SELECT ROUND(SUM(c.sum_of_ts_vac)::numeric) FROM rel_pub.crp_control_sum c, x_jpovolny.crp_full13_pred p
    WHERE c.load_id > p.max_full_load AND c.load_type = 'INIT') AS vac_control_sum,
  (SELECT ROUND(SUM(r.vac)::numeric) FROM res.res_release_publish r
    WHERE r.rvloadid = 13 AND r.transptype = 'ALL'
      AND r.faceid IN (SELECT faceid FROM rel_pub.crp_ts_rots_temp)) AS vac_release13;

-- 4k: Srovnání s předchozím full loadem 6 (1.4.2026, stará data bez vlny 3)
SELECT e.load_id, e.country, COUNT(DISTINCT e.pid) AS panelu, COUNT(*) AS radku,
       ROUND(SUM(e.ts_rots)::numeric) AS ts_rots, ROUND(SUM(e.vac)::numeric) AS vac
FROM rel_pub.etl_ccf_rel_pub e
WHERE e.load_id = 6
   OR e.load_id > (SELECT max_full_load FROM x_jpovolny.crp_full13_pred)
GROUP BY e.load_id, e.country
ORDER BY e.country, e.load_id;

-- 4l: Publish 102/103 - počty. Očekáváno pro oba rvloadid: 46700 řádků, 9340 fací, 7102 pidů, 5 transptype,
--     102 jen periodid 1, 103 jen periodid 200.
SELECT rvloadid, periodid, COUNT(*) AS radku, COUNT(DISTINCT facepid) AS faci, COUNT(DISTINCT pid) AS pidu,
       COUNT(DISTINCT transptype) AS transptype, ROUND(SUM(vac)::numeric) AS vac, ROUND(SUM(rots)::numeric) AS rots
FROM res.res_release_publish
WHERE rvloadid IN (13, 14, 102, 103)
GROUP BY rvloadid, periodid
ORDER BY rvloadid;

-- 4m: Publish 102/103 - obsah musí být řádek po řádku shodný s release 13 (102) a 14 (103).
--     Očekáváno: všechny čtyři hodnoty 0.
SELECT
  (SELECT COUNT(*) FROM (
     SELECT facepid, vacid, selsetid, dayicid, hourcid, fdurid, periodid, modeltype, transptype, rminvacver,
            vac, rots, va, pop_total, grp_vac_total, grp_rots_total, pid, mid, faceid
     FROM res.res_release_publish WHERE rvloadid = 102
     EXCEPT
     SELECT facepid, vacid, selsetid, dayicid, hourcid, fdurid, periodid, modeltype, transptype, rminvacver,
            vac, rots, va, pop_total, grp_vac_total, grp_rots_total, pid, mid, faceid
     FROM res.res_release_publish WHERE rvloadid = 13) x) AS jen_v_102,
  (SELECT COUNT(*) FROM (
     SELECT facepid, vacid, selsetid, dayicid, hourcid, fdurid, periodid, modeltype, transptype, rminvacver,
            vac, rots, va, pop_total, grp_vac_total, grp_rots_total, pid, mid, faceid
     FROM res.res_release_publish WHERE rvloadid = 13
     EXCEPT
     SELECT facepid, vacid, selsetid, dayicid, hourcid, fdurid, periodid, modeltype, transptype, rminvacver,
            vac, rots, va, pop_total, grp_vac_total, grp_rots_total, pid, mid, faceid
     FROM res.res_release_publish WHERE rvloadid = 102) x) AS jen_v_13,
  (SELECT COUNT(*) FROM (
     SELECT facepid, vacid, selsetid, dayicid, hourcid, fdurid, periodid, modeltype, transptype, rminvacver,
            vac, rots, va, pop_total, grp_vac_total, grp_rots_total, pid, mid, faceid
     FROM res.res_release_publish WHERE rvloadid = 103
     EXCEPT
     SELECT facepid, vacid, selsetid, dayicid, hourcid, fdurid, periodid, modeltype, transptype, rminvacver,
            vac, rots, va, pop_total, grp_vac_total, grp_rots_total, pid, mid, faceid
     FROM res.res_release_publish WHERE rvloadid = 14) x) AS jen_v_103,
  (SELECT COUNT(*) FROM (
     SELECT facepid, vacid, selsetid, dayicid, hourcid, fdurid, periodid, modeltype, transptype, rminvacver,
            vac, rots, va, pop_total, grp_vac_total, grp_rots_total, pid, mid, faceid
     FROM res.res_release_publish WHERE rvloadid = 14
     EXCEPT
     SELECT facepid, vacid, selsetid, dayicid, hourcid, fdurid, periodid, modeltype, transptype, rminvacver,
            vac, rots, va, pop_total, grp_vac_total, grp_rots_total, pid, mid, faceid
     FROM res.res_release_publish WHERE rvloadid = 103) x) AS jen_v_14;

-- 4n: Tabulky inkrementálního běhu, vstupy a ostatní rvloadid v publish se nesměly změnit.
--     Očekáváno: všechny sloupce true.
SELECT
  p.all_panel = (SELECT COUNT(*) FROM rel_pub.crp_all_panel) AS all_panel_ok,
  p.missing = (SELECT COUNT(*) FROM rel_pub.crp_missing) AS missing_ok,
  p.max_inc_load = (SELECT COALESCE(MAX(load_id), 0) FROM rel_pub.etl_ccf_rel_pub_loads_inc) AS inc_load_ok,
  p.routing = (SELECT COUNT(*) FROM rel_pub.crp_routing) AS routing_ok,
  p.resp_weight = (SELECT COUNT(*) FROM rel_pub.crp_resp_weight_input) AS resp_weight_ok,
  p.publish_ostatni = (SELECT COUNT(*) FROM res.res_release_publish WHERE rvloadid NOT IN (102, 103)) AS publish_ostatni_ok
FROM x_jpovolny.crp_full13_pred p;


-- ====================================================================================================
-- KROK 5: Úklid po odsouhlasení výsledku (nepovinné)
-- ====================================================================================================
-- DROP TABLE x_jpovolny.crp_full13_pred;
