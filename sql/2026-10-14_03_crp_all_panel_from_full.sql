--
-- Naplnění haldy panelů rel_pub.crp_all_panel výsledkem full runu (rel_pub.crp_panel_full)
--
-- Vytvořil: Jiří Povolný
-- Vytvořeno dne: 14.10.2026
--
-- crp_all_panel používá auto_rel_pub_inc() jako haldu všech panelů (upsert zpracovaných panelů a hledání
-- náhradního panelu pro panely bez průsečíku s trasami - crp_nearest20). Před touto změnou obsahovala 14 003
-- midů včetně starých (modifikace, demolice). Nahrazuje se celým obsahem crp_panel_full z full runu
-- 13.10.2026 (load 7, release 13, 7 102 panelů). Panely změněné po releasu doplní první běh inc
-- (dorovnání přes crp_missing, 2026-10-13_06), protože zpracované panely do haldy upsertuje.
--
-- Předpoklad: inkrementální proces zastavený.
-- Návrat zpět: TRUNCATE rel_pub.crp_all_panel; INSERT INTO rel_pub.crp_all_panel SELECT * FROM x_jpovolny.crp_all_panel_20261014;
--

BEGIN;

-- Záloha (pokud záloha už existuje, skript skončí chybou a nic se nezmění)
CREATE TABLE x_jpovolny.crp_all_panel_20261014 AS
SELECT * FROM rel_pub.crp_all_panel;

TRUNCATE rel_pub.crp_all_panel;

INSERT INTO rel_pub.crp_all_panel
  (mid, pid, the_geom, buffer_geom, vac_all_1, maxdist, intersection_type, last_vacid, err_vac_res)
SELECT
  mid, pid, the_geom, buffer_geom, vac_all_1, maxdist, intersection_type, last_vacid, err_vac_res
FROM rel_pub.crp_panel_full;

-- Kontrola (při nesouladu výjimka = rollback celé transakce)
DO $$
DECLARE
  v_full bigint;
  v_all bigint;
  v_rozdil bigint;
BEGIN
  SELECT COUNT(*) INTO v_full FROM rel_pub.crp_panel_full;
  SELECT COUNT(*) INTO v_all FROM rel_pub.crp_all_panel;
  IF v_full <> 7102 OR v_all <> v_full THEN
    RAISE EXCEPTION 'crp_panel_full má % řádků, crp_all_panel % (očekáváno 7102) - nic se nemění', v_full, v_all;
  END IF;

  SELECT COUNT(*) INTO v_rozdil FROM (
    SELECT mid, pid, vac_all_1, maxdist, intersection_type, last_vacid FROM rel_pub.crp_panel_full
    EXCEPT
    SELECT mid, pid, vac_all_1, maxdist, intersection_type, last_vacid FROM rel_pub.crp_all_panel) x;
  IF v_rozdil <> 0 THEN
    RAISE EXCEPTION 'crp_all_panel se liší od crp_panel_full (% řádků)', v_rozdil;
  END IF;
END
$$;

-- Souhrn
SELECT
  (SELECT COUNT(*) FROM x_jpovolny.crp_all_panel_20261014) AS zaloha,
  COUNT(*) AS radku,
  COUNT(DISTINCT pid) AS pidu,
  SUM(CASE WHEN intersection_type = 'ORIGINAL' THEN 1 ELSE 0 END) AS original,
  SUM(CASE WHEN intersection_type = 'SUBSTITUTE' THEN 1 ELSE 0 END) AS substitute
FROM rel_pub.crp_all_panel;

COMMIT;

ANALYZE rel_pub.crp_all_panel;
