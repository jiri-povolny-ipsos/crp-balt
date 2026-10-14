--
-- Dorovnání nového CRP (rvloadid 102/103) se změnami od releasu 13 - panely do rel_pub.crp_missing
-- Spouštět ručně, krok po kroku, těsně před prvním spuštěním nové verze auto_rel_pub_inc()
--
-- Vytvořil: Jiří Povolný
-- Vytvořeno dne: 13.10.2026
--
-- Publish 102/103 vznikl full runem z releasu 13 (stav panelů k jeho cutoffu, 11.9.2026). Inkrementální běh
-- vybírá jen panely změněné za poslední den, změny mezi cutoffem releasu a prvním během nové verze by proto
-- v 102/103 chyběly. Skript vloží do crp_missing živé panely (status 3, timeattid 0, ownid = subownid),
-- jejichž stav v 102 neodpovídá aktuálnímu:
--   - panel v 102 chybí (nový nebo znovu oživený panel),
--   - v 102 je jiný mid (modifikace panelu),
--   - některá face aktuálního midu v 102 chybí, nebo má ve vai.vai_vac_res novější vacid než v 102.
-- Odpublikování (zdemolované, změněný status, ownid <> subownid) udělá auto_rel_pub_inc() sám.
--
-- Stav 13.10.2026: 13 + 8 + 19 pidů (část se může překrývat).
--
-- Jak spouštět: každý KROK zvlášť, klient v režimu autocommit.
--


-- ====================================================================================================
-- KROK 0: Náhled - kolik panelů a z jakého důvodu se vloží. Očekáváno: desítky pidů.
-- ====================================================================================================
WITH live AS (
  SELECT pid, mid FROM pnl.mv_pnl_main_maxdate
  WHERE status = 3 AND timeattid = 0 AND ownid = subownid
),
pub AS (
  SELECT DISTINCT pid, mid, faceid, vacid FROM res.res_release_publish WHERE rvloadid = 102
),
lastv AS (
  SELECT DISTINCT ON (faceid) faceid, vacid FROM vai.vai_vac_res ORDER BY faceid, vacid DESC
),
duvod AS (
  SELECT l.pid, 'panel v 102 chybí' AS duvod
  FROM live l WHERE NOT EXISTS (SELECT 1 FROM pub p WHERE p.pid = l.pid)
  UNION ALL
  SELECT DISTINCT l.pid, 'v 102 je jiný mid'
  FROM live l JOIN pub p ON p.pid = l.pid WHERE p.mid <> l.mid
  UNION ALL
  SELECT DISTINCT l.pid, 'face aktuálního midu v 102 chybí'
  FROM live l JOIN pnl.pnl_facemid f ON f.mid = l.mid
  WHERE EXISTS (SELECT 1 FROM pub p WHERE p.pid = l.pid AND p.mid = l.mid)
    AND NOT EXISTS (SELECT 1 FROM pub p WHERE p.faceid = f.faceid)
  UNION ALL
  SELECT DISTINCT l.pid, 'novější vacid než v 102'
  FROM live l JOIN pub p ON p.pid = l.pid AND p.mid = l.mid JOIN lastv v ON v.faceid = p.faceid
  WHERE v.vacid > p.vacid
)
SELECT duvod, COUNT(DISTINCT pid) AS pidu FROM duvod GROUP BY duvod
UNION ALL
SELECT 'CELKEM (unikátní pid)', COUNT(DISTINCT pid) FROM duvod
ORDER BY 1;


-- ====================================================================================================
-- KROK 1: Vložení do crp_missing (jen pidy, které tam ještě nejsou). Očekáváno: INSERT = CELKEM z KROKU 0.
-- ====================================================================================================
INSERT INTO rel_pub.crp_missing (pid)
SELECT DISTINCT x.pid
FROM (
  SELECT l.pid
  FROM pnl.mv_pnl_main_maxdate l
  WHERE l.status = 3 AND l.timeattid = 0 AND l.ownid = l.subownid
    AND (
      -- panel v 102 chybí nebo má jiný mid
      NOT EXISTS (SELECT 1 FROM res.res_release_publish p WHERE p.rvloadid = 102 AND p.pid = l.pid AND p.mid = l.mid)
      -- face aktuálního midu v 102 chybí
      OR EXISTS (SELECT 1 FROM pnl.pnl_facemid f
                 WHERE f.mid = l.mid
                   AND NOT EXISTS (SELECT 1 FROM res.res_release_publish p WHERE p.rvloadid = 102 AND p.faceid = f.faceid))
      -- novější vacid než v 102
      OR EXISTS (SELECT 1 FROM res.res_release_publish p
                 JOIN vai.vai_vac_res v ON v.faceid = p.faceid AND v.vacid > p.vacid
                 WHERE p.rvloadid = 102 AND p.pid = l.pid AND p.mid = l.mid)
    )
) x
WHERE NOT EXISTS (SELECT 1 FROM rel_pub.crp_missing m WHERE m.pid = x.pid);


-- ====================================================================================================
-- KROK 2: Kontrola obsahu crp_missing
-- ====================================================================================================
SELECT m.pid, l.mid, l.stamp, l.status, l.timeattid
FROM rel_pub.crp_missing m
LEFT JOIN pnl.mv_pnl_main_maxdate l ON l.pid = m.pid
ORDER BY m.pid;
