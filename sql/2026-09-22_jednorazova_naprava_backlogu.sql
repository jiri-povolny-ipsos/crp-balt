--
-- Jednorázová náprava backlogu CRP BALT
--
-- Vytvořil: Jiří Povolný
-- Vytvořeno dne: 22.9.2026
--
-- POZOR: tenhle skript MĚNÍ DATA v produkční imsbalt_prod. Pouštět vědomě, ne automaticky.
--        Je zabalený do transakce - do COMMITu si můžeš výsledek zkontrolovat.
--
-- Řeší dvě skupiny panelů, které chybí v IDS (podklady viz analysis/, soubory
-- A_zaseknute_ve_fronte.csv a B_ztracene_pri_padu.csv):
--
--   A) 68 pidů zaseknutých v crp_no_calc_inc (in_queue = TRUE, calculated = FALSE).
--      Uvízly tam kvůli rollbacku po "division by zero" - příznak calculated se nikdy
--      nezapsal a 4denní okno (stamp_in_queue > CURRENT_DATE - 4) mezitím vypršelo.
--      Jejich VAC výsledky jsou od hromadného přepočtu 11.9.2026 už v pořádku
--      (všech 72 ploch má rots > 0), takže je stačí nechat znovu zpracovat.
--
--   B) 14 pidů, které se ztratily úplně. Byly modifikované v den, kdy běh spadl,
--      rollback je zahodil a výběr "stamp > CURRENT_DATE - 1" je už znovu nenabral.
--      Také mají zdravé VAC výsledky.
--
--   C) 4 pidy, které v IDS jsou, ale se starou modifikací (1663, 1673, 2909, 6609).
--      Jejich nová modifikace se nikdy nezpracovala, takže v IDS i v publishi visí
--      data z mid, který má ještě navteqver 25q1 (aktuální je 26q1). Vlastní kontrola
--      navteq verze v auto_rel_pub_inc() je nechytí - ta testuje jen panely, které
--      zrovna jdou do zpracování, ne to, co už je publikované. Přepočtem se zároveň
--      uklidí 3 osiřelé facepidy (9466, 9470, 9617), které v publishi jsou, ale
--      v IDS ne - jde o druhé plochy digitálních panelů, publikované ještě z doby,
--      kdy panel nebyl 'D'.
--
-- Všechny tři skupiny se vloží do rel_pub.crp_missing, odkud si je vezme nejbližší běh
-- auto_rel_pub_inc(). U skupiny A je nutné zároveň smazat řádky z crp_no_calc_inc,
-- protože běh jinak panely z crp_geom_panel zase vyhodí (DELETE ... WHERE pid IN
-- (SELECT pid FROM crp_no_calc_inc)).
--
-- Doporučené pořadí: nejdřív nasadit 2026-09-22_fix_auto_rel_pub_inc.sql, teprve
-- potom tuhle nápravu - kdyby některý panel přece jen vrátil nulové rots, oprava ho
-- odchytí do crp_pnl_to_revise místo aby shodila běh.
--

BEGIN;

-- ---------------------------------------------------------------------------
-- Kontrola PŘED zásahem
-- ---------------------------------------------------------------------------
SELECT 'pred: crp_missing' AS co, COUNT(*) AS radku FROM rel_pub.crp_missing
UNION ALL SELECT 'pred: crp_no_calc_inc', COUNT(*) FROM rel_pub.crp_no_calc_inc
UNION ALL SELECT 'pred: crp_pnl_to_revise', COUNT(*) FROM rel_pub.crp_pnl_to_revise;

-- ---------------------------------------------------------------------------
-- A) Zaseknuté panely z crp_no_calc_inc -> k novému zpracování
-- ---------------------------------------------------------------------------

-- Pojistka: vezmeme jen ty, které opravdu čekají ve frontě a nemají chybu výpočtu.
INSERT INTO rel_pub.crp_missing (pid)
SELECT DISTINCT n.pid
FROM rel_pub.crp_no_calc_inc n
WHERE n.in_queue = TRUE
  AND n.calculated = FALSE
  AND n.ignore = FALSE
  AND n.err_vac_res IS NULL
  AND n.pid NOT IN (SELECT pid FROM rel_pub.crp_missing);

-- Bez tohohle DELETE by je běh z crp_geom_panel zase vyhodil.
DELETE FROM rel_pub.crp_no_calc_inc
WHERE in_queue = TRUE
  AND calculated = FALSE
  AND ignore = FALSE
  AND err_vac_res IS NULL;

-- ---------------------------------------------------------------------------
-- B) Panely ztracené při spadlém běhu -> k novému zpracování
-- ---------------------------------------------------------------------------
-- Seznam je z B_ztracene_pri_padu.csv (stav k 22.9.2026). Uvedený výčet je záměrně
-- explicitní, aby bylo na první pohled vidět, čeho se zásah týká.
INSERT INTO rel_pub.crp_missing (pid)
SELECT v.pid
FROM (
  VALUES (8087::bigint), (8280), (8288), (8289), (8290), (8291), (8292),
         (8293), (8294), (8295), (8296), (8297), (8298), (8299)
) AS v(pid)
WHERE v.pid NOT IN (SELECT pid FROM rel_pub.crp_missing)
  -- Jen pokud je panel pořád živý a opravdu chybí v IDS.
  AND EXISTS (SELECT 1 FROM pnl.mv_pnl_main_maxdate m
              WHERE m.pid = v.pid AND m.status = 3 AND m.timeattid = 0
                AND m.ownid = m.subownid)
  AND NOT EXISTS (SELECT 1 FROM ids.v_panel p WHERE p.pid = v.pid);

-- ---------------------------------------------------------------------------
-- C) Panely, u kterých má IDS starší modifikaci než IMS -> k novému zpracování
-- ---------------------------------------------------------------------------
-- Kritérium je psané obecně (IDS má jiný mid než aktuální v IMS), protože zastaralý
-- navteqver je jen důsledek. K 22.9.2026 vybere pidy 1663, 1673, 2909 a 6609,
-- z toho 2909 už pokryla skupina A, takže reálně přibudou 3.
-- Po přepočtu publish smaže staré řádky (DELETE ... WHERE pid IN crp_ims_input_inc)
-- a vloží nové s aktuálním mid.
INSERT INTO rel_pub.crp_missing (pid)
SELECT DISTINCT v.pid
FROM ids.v_panel v
JOIN pnl.mv_pnl_main_maxdate m ON m.pid = v.pid
WHERE m.status = 3
  AND m.timeattid = 0
  AND v.mid <> m.mid
  AND v.pid NOT IN (SELECT pid FROM rel_pub.crp_missing);

-- ---------------------------------------------------------------------------
-- Kontrola PO zásahu (před COMMITem)
-- ---------------------------------------------------------------------------
SELECT 'po: crp_missing' AS co, COUNT(*) AS radku FROM rel_pub.crp_missing
UNION ALL SELECT 'po: crp_no_calc_inc', COUNT(*) FROM rel_pub.crp_no_calc_inc
UNION ALL SELECT 'po: crp_pnl_to_revise', COUNT(*) FROM rel_pub.crp_pnl_to_revise;

-- Očekávaný stav k 22.9.2026:
--   crp_missing       0  -> 85   (68 ze skupiny A + 14 ze skupiny B + 3 ze skupiny C)
--   crp_no_calc_inc  84  ->  0
--   crp_pnl_to_revise         beze změny (117)

COMMIT;
-- V případě pochybností místo COMMIT použij ROLLBACK.

-- ---------------------------------------------------------------------------
-- Po nejbližším běhu auto_rel_pub_inc() zkontrolovat:
--   - kolik panelů load zpracoval (mělo by být ~85 + běžné denní změny)
--   - jestli crp_missing zůstala prázdná (běh ji na konci TRUNCATE)
--   - jestli panely dorazily do ids.v_panel
--   - jestli v ids.v_panel ani v res_release_publish (rvloadid 100, 101) nezůstal
--     žádný mid s navteqver <> 26q1 - viz analysis/kontrola_po_naprave.sql
-- ---------------------------------------------------------------------------
