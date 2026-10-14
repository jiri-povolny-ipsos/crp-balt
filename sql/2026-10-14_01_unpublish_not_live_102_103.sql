--
-- Jednorázové odpublikování neživých panelů z nového CRP (rvloadid 102, 103) a úklid cache spotplánů 102
--
-- Vytvořil: Jiří Povolný
-- Vytvořeno dne: 14.10.2026
--
-- Dělá totéž, co by udělal první běh nové auto_rel_pub_inc() (2026-10-13_05), jen hned a bez výpočtu:
--   1) z res.res_release_publish (102, 103) odstraní panely, které nesplňují kritérium procesu
--      (status 3, timeattid 0, ownid = subownid) - stav 14.10.2026: 882 pidů (835 ownid <> subownid,
--      47 zdemolovaných), záloha smazaných řádků do x_jpovolny.res_release_publish_unpub_20261014,
--      evidence do rel_pub.crp_unpublish_list a rel_pub.crp_geom_panel_log,
--   2) z ids.ids_vac_calc_campaign_cache2 (102) smaže vacidy, které nejsou v publish 102
--      (log do rel_pub.crp_cache_del_log).
-- Vše v jedné transakci. Kontrola před COMMIT: pokud počet odpublikovaných pidů není 882, skript skončí
-- chybou a nic se nezmění.
--
-- Návrat zpět:
--   INSERT INTO res.res_release_publish SELECT * FROM x_jpovolny.res_release_publish_unpub_20261014;
--   INSERT INTO ids.ids_vac_calc_campaign_cache2 (rvloadid, faceid, spot_length, spot_pause, vacid, facepid,
--     vac, rots, vac_orig, rots_orig, group_def, period_hours)
--   SELECT rvloadid, faceid, spot_length, spot_pause, vacid, facepid, vac, rots, vac_orig, rots_orig, group_def, period_hours
--   FROM rel_pub.crp_cache_del_log WHERE rvloadid = 102 AND deleted_at >= '2026-10-14' (a deleted_at z tohoto běhu);
--

BEGIN;

-- 1. Seznam k odpublikování (stejné pravidlo jako "past cases" v auto_rel_pub_inc)
TRUNCATE rel_pub.crp_unpublish_list;

INSERT INTO rel_pub.crp_unpublish_list (mid, pid, desc_text)
SELECT
  m.mid,
  x.pid,
  'Unpublished - not live (status, timeattid, ownid <> subownid), one-off 14.10.2026'
FROM
  (SELECT DISTINCT pid FROM res.res_release_publish WHERE rvloadid IN (102, 103)) x
  LEFT JOIN pnl.mv_pnl_main_maxdate m ON m.pid = x.pid
WHERE
  NOT EXISTS (SELECT 1 FROM pnl.mv_pnl_main_maxdate l
              WHERE l.pid = x.pid AND l.status = 3 AND l.timeattid = 0 AND l.ownid = l.subownid)
ORDER BY
  x.pid;

-- 2. Záloha a odpublikování
CREATE TABLE x_jpovolny.res_release_publish_unpub_20261014 AS
SELECT p.*
FROM res.res_release_publish p
WHERE p.rvloadid IN (102, 103)
  AND p.pid IN (SELECT pid FROM rel_pub.crp_unpublish_list);

DELETE FROM res.res_release_publish p
WHERE p.rvloadid IN (102, 103)
  AND p.pid IN (SELECT pid FROM rel_pub.crp_unpublish_list);

INSERT INTO rel_pub.crp_geom_panel_log (mid, pid, desc_text)
SELECT mid, pid, desc_text FROM rel_pub.crp_unpublish_list;

-- 3. Úklid cache spotplánů 102: vacidy, které nejsou v publish 102
INSERT INTO rel_pub.crp_cache_del_log (
    rvloadid, faceid, spot_length, spot_pause, vacid, facepid,
    vac, rots, vac_orig, rots_orig, group_def, period_hours
)
SELECT
    rvloadid, faceid, spot_length, spot_pause, vacid, facepid,
    vac, rots, vac_orig, rots_orig, group_def, period_hours
FROM ids.ids_vac_calc_campaign_cache2 c
WHERE c.rvloadid = 102
  AND NOT EXISTS (SELECT 1 FROM res.res_release_publish p WHERE p.rvloadid = 102 AND p.vacid = c.vacid);

DELETE FROM ids.ids_vac_calc_campaign_cache2 c
WHERE c.rvloadid = 102
  AND NOT EXISTS (SELECT 1 FROM res.res_release_publish p WHERE p.rvloadid = 102 AND p.vacid = c.vacid);

-- 4. Kontroly (při nesouladu výjimka = rollback celé transakce)
DO $$
DECLARE
  v_pid bigint;
  v_nezive bigint;
  v_rozdil bigint;
BEGIN
  SELECT COUNT(DISTINCT pid) INTO v_pid FROM rel_pub.crp_unpublish_list;
  IF v_pid <> 882 THEN
    RAISE EXCEPTION 'Odpublikováno % pidů, očekáváno 882 - nic se nemění', v_pid;
  END IF;

  SELECT COUNT(DISTINCT x.pid) INTO v_nezive
  FROM res.res_release_publish x
  WHERE x.rvloadid IN (102, 103)
    AND NOT EXISTS (SELECT 1 FROM pnl.mv_pnl_main_maxdate m
                    WHERE m.pid = x.pid AND m.status = 3 AND m.timeattid = 0 AND m.ownid = m.subownid);
  IF v_nezive <> 0 THEN
    RAISE EXCEPTION 'V publish 102/103 zůstalo % neživých pidů', v_nezive;
  END IF;

  SELECT COUNT(*) INTO v_rozdil FROM (
    SELECT facepid, vacid, transptype FROM res.res_release_publish WHERE rvloadid = 102
    EXCEPT
    SELECT facepid, vacid, transptype FROM res.res_release_publish WHERE rvloadid = 103) x;
  IF v_rozdil <> 0 THEN
    RAISE EXCEPTION 'Publish 102 a 103 se liší (% řádků)', v_rozdil;
  END IF;
END
$$;

-- Souhrn
SELECT
  (SELECT COUNT(DISTINCT pid) FROM rel_pub.crp_unpublish_list) AS odpublikovano_pidu,
  (SELECT COUNT(*) FROM x_jpovolny.res_release_publish_unpub_20261014) AS smazano_radku_publish,
  (SELECT COUNT(DISTINCT pid) FROM res.res_release_publish WHERE rvloadid = 102) AS pidu_102,
  (SELECT COUNT(*) FROM res.res_release_publish WHERE rvloadid = 102) AS radku_102,
  (SELECT COUNT(*) FROM res.res_release_publish WHERE rvloadid = 103) AS radku_103,
  (SELECT COUNT(*) FROM ids.ids_vac_calc_campaign_cache2 WHERE rvloadid = 102) AS cache_102,
  (SELECT COUNT(DISTINCT vacid) FROM ids.ids_vac_calc_campaign_cache2 WHERE rvloadid = 102) AS cache_102_vacidu;

COMMIT;
