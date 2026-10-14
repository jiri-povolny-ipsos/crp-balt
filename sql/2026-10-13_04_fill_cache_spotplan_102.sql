--
-- Jednorázové naplnění cache spotplánů ids.ids_vac_calc_campaign_cache2 pro rvloadid 102 (nové CRP)
-- po full runu 13.10.2026 (load 7, publish 102/103 z releasu 13) - spouštění ručně, krok po kroku
--
-- Vytvořil: Jiří Povolný
-- Vytvořeno dne: 13.10.2026
--
-- Cache 102 je po full runu prázdná. IDS by si ji po přepnutí na 102 dopočítalo samo při prvním
-- dotazu na každý spotplán (vac8s._f_vac_calc_ids_campaign, ~5 s na spotplán). Tento skript ji předvyplní
-- pro spotplány, které jsou v cache 100 (34 spotplánů, ~3 min). Počítá se stejnou oficiální funkcí,
-- výsledek je tedy totožný s tím, co by spočítalo IDS.
--
-- Další full runy s p_publish = true si cache 102 obnovují samy (auto_rel_pub_ccf, bod 10).
--
-- Jak spouštět: každý KROK zvlášť, klient v režimu autocommit.
--


-- ====================================================================================================
-- KROK 0: Předpoklady. Očekáváno: cache_102 = 0, spotplanu_100 = 34, necelociselnych = 0,
--         digit_vacidu_102 = 684, pnlvaienv_102 = 'EA'.
-- ====================================================================================================
SELECT
  (SELECT COUNT(*) FROM ids.ids_vac_calc_campaign_cache2 WHERE rvloadid = 102) AS cache_102,
  (SELECT COUNT(*) FROM (SELECT DISTINCT spot_length, spot_pause
                         FROM ids.ids_vac_calc_campaign_cache2 WHERE rvloadid = 100) s) AS spotplanu_100,
  (SELECT COUNT(*) FROM (SELECT DISTINCT spot_length, spot_pause
                         FROM ids.ids_vac_calc_campaign_cache2 WHERE rvloadid = 100) s
    WHERE spot_length <> trunc(spot_length) OR spot_pause <> trunc(spot_pause)) AS necelociselnych,
  (SELECT COUNT(DISTINCT p.vacid) FROM res.res_release_publish p JOIN pnl.pnl_main m ON m.mid = p.mid
    WHERE p.rvloadid = 102 AND m.pnlmotion = 'D') AS digit_vacidu_102,
  (SELECT pnlvaienv FROM vai.vai_release_vac_static_load WHERE rvloadid = 102) AS pnlvaienv_102;


-- ====================================================================================================
-- KROK 1: Naplnění. Pro každý spotplán z cache 100 zavolá campaign funkci pro rvloadid 102.
--         Očekáváno: 34 řádků, u každého radku = 684, chyb = 0.
--         Spotplán s chybou (chyb > 0) má cache prázdnou, IDS si ho dopočítá samo.
-- ====================================================================================================
SELECT s.spot_length, s.spot_pause, x.radku, x.chyb
FROM (SELECT DISTINCT spot_length, spot_pause
      FROM ids.ids_vac_calc_campaign_cache2 WHERE rvloadid = 100) s,
     LATERAL (SELECT COUNT(*) AS radku, SUM(CASE WHEN c.vacid = -999 THEN 1 ELSE 0 END) AS chyb
              FROM vac8s._f_vac_calc_ids_campaign(102, s.spot_length::integer, s.spot_pause::integer) c) x
ORDER BY s.spot_length, s.spot_pause;


-- ====================================================================================================
-- KROK 2: Kontroly
-- ====================================================================================================

-- 2a: Souhrn. Očekáváno: spotplanu 34, radku 23256 (34 x 684), vacidu 684, facepidu 684.
SELECT COUNT(DISTINCT (spot_length, spot_pause)) AS spotplanu, COUNT(*) AS radku,
       COUNT(DISTINCT vacid) AS vacidu, COUNT(DISTINCT facepid) AS facepidu
FROM ids.ids_vac_calc_campaign_cache2
WHERE rvloadid = 102;

-- 2b: Každý vacid má všechny spotplány a žádná duplicita. Očekáváno: 0, 0.
SELECT
  (SELECT COUNT(*) FROM (SELECT vacid FROM ids.ids_vac_calc_campaign_cache2 WHERE rvloadid = 102
                         GROUP BY vacid
                         HAVING COUNT(DISTINCT (spot_length, spot_pause)) <>
                                (SELECT COUNT(DISTINCT (spot_length, spot_pause))
                                 FROM ids.ids_vac_calc_campaign_cache2 WHERE rvloadid = 102)) x) AS neuplnych_vacidu,
  (SELECT COUNT(*) FROM (SELECT vacid, spot_length, spot_pause FROM ids.ids_vac_calc_campaign_cache2
                         WHERE rvloadid = 102 GROUP BY 1, 2, 3 HAVING COUNT(*) > 1) x) AS duplicit;

-- 2c: Vacidy, které jsou i v cache 100, musí mít stejné hodnoty. Očekáváno: shodnych = spolecnych.
SELECT COUNT(*) AS spolecnych,
       SUM(CASE WHEN abs(a.vac - b.vac) < 1e-6 AND a.rots = b.rots AND a.facepid = b.facepid THEN 1 ELSE 0 END) AS shodnych
FROM ids.ids_vac_calc_campaign_cache2 a
JOIN ids.ids_vac_calc_campaign_cache2 b
  ON b.rvloadid = 100 AND b.vacid = a.vacid AND b.spot_length = a.spot_length AND b.spot_pause = a.spot_pause
WHERE a.rvloadid = 102;

-- 2d: Vacidy v cache 102 musí být v publish 102. Očekáváno: 0.
SELECT COUNT(DISTINCT c.vacid) AS vacid_mimo_publish
FROM ids.ids_vac_calc_campaign_cache2 c
WHERE c.rvloadid = 102
  AND NOT EXISTS (SELECT 1 FROM res.res_release_publish p WHERE p.rvloadid = 102 AND p.vacid = c.vacid);
