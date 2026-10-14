--
-- Tabulka rel_pub.crp_panel_full - seznam panelů full běhu (rel_pub.auto_rel_pub_ccf)
--
-- Vytvořil: Jiří Povolný
-- Vytvořeno dne: 13.10.2026
--
-- Struktura, index a práva jsou shodné s rel_pub.crp_all_panel. Důvod oddělení: crp_all_panel je halda
-- panelů inkrementálního běhu (auto_rel_pub_inc ji upsertuje a hledá v ní náhradní panely pro
-- crp_nearest20), full běh si svůj seznam panelů na začátku vždy maže (TRUNCATE).
--
-- Nasazení: spustit celý soubor (psql -f), před 2026-10-13_02_auto_rel_pub_ccf_rvloadid.sql.
-- Rollback: DROP TABLE rel_pub.crp_panel_full;
--

BEGIN;

CREATE TABLE rel_pub.crp_panel_full (
    mid bigint,
    pid bigint,
    the_geom topo.geometry,
    buffer_geom topo.geometry,
    vac_all_1 double precision,
    maxdist double precision,
    intersection_type character varying,
    last_vacid bigint,
    err_vac_res character varying
);

CREATE INDEX z_panel_full_indx_gist_buffer ON rel_pub.crp_panel_full USING gist (buffer_geom);

ALTER TABLE rel_pub.crp_panel_full OWNER TO vaibalt_jpovolny_login;
GRANT SELECT ON rel_pub.crp_panel_full TO vaibalt_ids_login;

COMMIT;
