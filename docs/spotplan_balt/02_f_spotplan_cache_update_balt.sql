/*==================================================================================================
|| Funkce pro inkrementální aktualizaci cache ids.ids_vac_calc_campaign_cache2 (spotplánové VAC/ROTS)
|| - verze pro BALT (imsbalt_prod): jen outdoor (EA), IDS rvloadid 100
||
|| - pro daný rvloadid smaže z cache vacidy, které už nejsou v res.res_release_publish (výměna vacidu),
||   a spočítá (a vloží) vacidy digitálních panelů, které v cache chybí (nový panel / nový vacid)
|| - spotplány (spot_length, spot_pause) se berou z cache daného rvloadid
|| - výpočet je shodný s vac8s._f_vac_calc_ids_campaign (FEPE)
|| - pokud je ve vac8s.f_params() jiná dynva_method než FEPE, skončí výjimkou (jako campaign funkce)
|| - in_vacids = NULL  -> automaticky (vacidy publish bez záznamu v cache + úklid zastaralých)
||   in_vacids = pole -> pouze zadané vacidy (staré záznamy se smažou a spočítají znovu)
|| Vrací počet vložených řádků.
||
|| Rozdíly proti verzi pro SE (ids.f_spotplan_cache_update v imsse_prod):
|| - jen EA (BALT nemá indoor, schéma vacia8s se nepoužívá); jiné prostředí = výjimka
|| - digitální panel = pnlmotion 'D' (BALT; v SE je to 'G') - viz proměnnou sPnlmotion
|| - advisory lock proti souběžnému běhu (ruční spuštění + denní CRP), cache nemá unikátní index
||
|| Vytvořil: Jiří Povolný
|| Vytvořeno dne: 2026-10-13
||================================================================================================*/
CREATE OR REPLACE FUNCTION ids.f_spotplan_cache_update(in_rvloadid integer, in_vacids bigint[] DEFAULT NULL)
  RETURNS integer AS
$BODY$
DECLARE
    -- Hodnota pnlmotion digitálních panelů. BALT: 'D' (ověř v code.code_pnlmotion, skript 01 sekce 4).
    sPnlmotion varchar := 'D';
    sEnv varchar;
    sDynVaMethod varchar;
    arrVacids bigint[];
    recSpot record;
    iInserted integer := 0;
    iRows integer;
BEGIN
    -- souběžné volání pro stejný rvloadid počká (lock se uvolní s koncem transakce)
    PERFORM pg_advisory_xact_lock(hashtext('ids.f_spotplan_cache_update'), in_rvloadid);

    -- prostředí rvloadid (BALT: jen EA)
    SELECT upper(a.pnlvaienv) INTO sEnv FROM vai.vai_release_vac_static_load a WHERE a.rvloadid = in_rvloadid;
    IF sEnv IS DISTINCT FROM 'EA' THEN
        RAISE EXCEPTION 'Neznamy nebo nepodporovany rvloadid % (pnlvaienv=%, podporovano jen EA)', in_rvloadid, coalesce(sEnv, 'null');
    END IF;

    -- funkce počítá jen metodou FEPE, stejně jako campaign funkce; jiná metoda by dala nekonzistentní cache
    SELECT upper(p.dynva_method) INTO sDynVaMethod FROM vac8s.f_params() p;
    IF sDynVaMethod IS DISTINCT FROM 'FEPE' THEN
        RAISE EXCEPTION 'method = % not implemented', coalesce(sDynVaMethod, 'null');
    END IF;

    IF in_vacids IS NULL THEN
        -- úklid zastaralých vacidů (vacid byl v publish nahrazen novým)
        DELETE FROM ids.ids_vac_calc_campaign_cache2 c
        WHERE c.rvloadid = in_rvloadid
          AND NOT EXISTS (SELECT 1 FROM res.res_release_publish p WHERE p.rvloadid = c.rvloadid AND p.vacid = c.vacid);

        -- vacidy digitálních panelů z publish, které v cache chybí
        SELECT array_agg(x.vacid) INTO arrVacids
        FROM (SELECT DISTINCT p.vacid
              FROM res.res_release_publish p JOIN pnl.pnl_main m ON (m.mid = p.mid)
              WHERE p.rvloadid = in_rvloadid AND m.pnlmotion = sPnlmotion
                AND NOT EXISTS (SELECT 1 FROM ids.ids_vac_calc_campaign_cache2 c
                                WHERE c.rvloadid = p.rvloadid AND c.vacid = p.vacid)) x;
    ELSE
        arrVacids := in_vacids;
        DELETE FROM ids.ids_vac_calc_campaign_cache2 c
        WHERE c.rvloadid = in_rvloadid AND c.vacid = ANY (arrVacids);
    END IF;

    IF arrVacids IS NULL THEN
        RETURN 0;
    END IF;

    FOR recSpot IN
        SELECT a.spot_length, a.spot_pause
        FROM ids.ids_vac_calc_campaign_cache2 a
        WHERE a.rvloadid = in_rvloadid
        GROUP BY a.spot_length, a.spot_pause
        ORDER BY 1, 2
    LOOP
        INSERT INTO ids.ids_vac_calc_campaign_cache2
            (rvloadid, faceid, spot_length, spot_pause, vacid, facepid, vac, rots, vac_orig, rots_orig, group_def, period_hours)
        SELECT in_rvloadid, max(t.faceid), recSpot.spot_length::double precision, recSpot.spot_pause::double precision,
               t.vacid, max(t.facepid),
               sum(t.dynva * t.fa_calc), round(CAST(sum(t.fa_calc) AS numeric), 0),
               sum(t.dynva * t.fa_calc), round(CAST(sum(t.fa_calc) AS numeric), 0),
               null::varchar, max(t.period_hours)
        FROM (SELECT c.facepid, b.faceid, b.vacid, a.fa_calc,
                     vac8s._f_vac_calc_ids_dynvac_fepe(a.transptype, a.pnlmotion, a.route_kcd, a.route_va,
                         recSpot.spot_length, recSpot.spot_length + recSpot.spot_pause, a.daf_angle) AS dynva,
                     f.period_hours
              FROM vai_subl.vai_subl_route_plan_dynva a
                   JOIN vai.vai_vac_res b ON (a.rsid = b.rsid)
                   JOIN pnl.pnl_facemid c ON (b.faceid = c.faceid)
                   JOIN (SELECT DISTINCT o.vacid, o.periodid
                         FROM res.res_online_ea_output o
                         WHERE o.vacid = ANY (arrVacids)) e ON (e.vacid = b.vacid)
                   JOIN res.res_period f ON (f.periodid = e.periodid)
              WHERE a.pnlmotion = sPnlmotion AND a.route_va > 0 AND a.fa_calc > 0
                AND b.vacid = ANY (arrVacids)) t
        GROUP BY t.vacid;

        GET DIAGNOSTICS iRows = ROW_COUNT;
        iInserted := iInserted + iRows;
    END LOOP;

    RETURN iInserted;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
