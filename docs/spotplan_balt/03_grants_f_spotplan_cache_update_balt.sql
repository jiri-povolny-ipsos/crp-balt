/*==================================================================================================
|| 03 - GRANTy pro ids.f_spotplan_cache_update (BALT)
|| Funkce běží jako SECURITY INVOKER, takže volající role potřebuje práva na všechny použité objekty.
|| Denní CRP na BALT se spouští přes psql pod uživatelem postgres (files/crp_inc.bat) - pak GRANTy
|| NEJSOU potřeba. Skript spusť jen tehdy, když funkci volá jiná (ne-superuser) role.
||
|| Použití: do proměnné sRole doplň roli/login, který funkci volá, a skript spusť.
|| Výsledek kontroly se vypíše přes RAISE NOTICE (všude musí být true).
||
|| Vytvořil: Jiří Povolný
|| Vytvořeno dne: 2026-10-13
||================================================================================================*/
DO $do$
DECLARE
    sRole varchar := 'CHANGE_ME';   -- <== role/login volající funkci (např. login CRP scheduleru)
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = sRole) THEN
        RAISE EXCEPTION 'Role % neexistuje - uprav sRole', sRole;
    END IF;

    -- 1. Schémata (USAGE)
    EXECUTE 'GRANT USAGE ON SCHEMA ids TO ' || quote_ident(sRole);
    EXECUTE 'GRANT USAGE ON SCHEMA vac8s TO ' || quote_ident(sRole);
    EXECUTE 'GRANT USAGE ON SCHEMA vai_subl TO ' || quote_ident(sRole);

    -- 2. Tabulky
    EXECUTE 'GRANT SELECT, INSERT, DELETE ON TABLE ids.ids_vac_calc_campaign_cache2 TO ' || quote_ident(sRole);
    EXECUTE 'GRANT SELECT ON TABLE vai_subl.vai_subl_route_plan_dynva TO ' || quote_ident(sRole);

    -- 3. Funkce
    EXECUTE 'GRANT EXECUTE ON FUNCTION ids.f_spotplan_cache_update(integer, bigint[]) TO ' || quote_ident(sRole);

    -- Kontrola
    RAISE NOTICE 'schema ids USAGE: %', has_schema_privilege(sRole, 'ids', 'USAGE');
    RAISE NOTICE 'schema vac8s USAGE: %', has_schema_privilege(sRole, 'vac8s', 'USAGE');
    RAISE NOTICE 'schema vai_subl USAGE: %', has_schema_privilege(sRole, 'vai_subl', 'USAGE');
    RAISE NOTICE 'cache SELECT: %', has_table_privilege(sRole, 'ids.ids_vac_calc_campaign_cache2', 'SELECT');
    RAISE NOTICE 'cache INSERT: %', has_table_privilege(sRole, 'ids.ids_vac_calc_campaign_cache2', 'INSERT');
    RAISE NOTICE 'cache DELETE: %', has_table_privilege(sRole, 'ids.ids_vac_calc_campaign_cache2', 'DELETE');
    RAISE NOTICE 'route_plan_dynva SELECT: %', has_table_privilege(sRole, 'vai_subl.vai_subl_route_plan_dynva', 'SELECT');
    RAISE NOTICE 'f_spotplan_cache_update EXECUTE: %', has_function_privilege(sRole, 'ids.f_spotplan_cache_update(integer, bigint[])', 'EXECUTE');
    RAISE NOTICE 'vac8s.f_params EXECUTE: %', has_function_privilege(sRole, 'vac8s.f_params()', 'EXECUTE');
    RAISE NOTICE 'vac8s._f_vac_calc_ids_dynvac_fepe: nutno ověřit ručně (výchozí EXECUTE pro PUBLIC)';
    -- Práva na res, vai, pnl (SELECT) role typicky už má z běžného provozu CRP; ověř skriptem 01, sekce 1.
END
$do$;
