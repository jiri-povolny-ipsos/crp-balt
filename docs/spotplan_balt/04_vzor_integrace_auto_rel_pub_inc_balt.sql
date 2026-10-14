/*==================================================================================================
|| 04 - VZOR integrace do rel_pub.auto_rel_pub_inc() (BALT)
|| !!! NESPOUŠTĚT jako skript !!! Jde o kus kódu, který se ručně vloží do funkce auto_rel_pub_inc()
|| (aktuální produkční verze, ne ze starého exportu ddl/schema_rel_pub.sql).
||
|| Co se mění
|| ----------
|| Dnes (komentář v kódu: "Nové, cache - test režim") se na konci běhu, uvnitř IF i_cnt > 0:
||   TRUNCATE rel_pub.crp_spotplans; INSERT INTO rel_pub.crp_spotplans ...;
||   PERFORM rel_pub.f_fill_crp_cache_temp();
||   INSERT INTO ids.ids_vac_calc_campaign_cache2 SELECT * FROM rel_pub.crp_cache_temp;
||
|| Nově se tyto 3 kroky (včetně RAISE NOTICE mezi nimi) nahradí jediným voláním níže.
|| Blok "Odlogujeme mazané záznamy" + DELETE z cache pro demolované/změněné panely (pnlmotion = 'D',
|| rvloadid = 100) ZŮSTÁVÁ beze změny - nová funkce navazuje: smazané vacidy v publish najde a dopočte.
||
|| Podmínky
|| --------
|| 1. Volání musí být PO všech DELETE/INSERT do res.res_release_publish (publish musí být hotový,
||    podle něj funkce maže zastaralé vacidy a doplňuje chybějící). Funkce běží ve stejné transakci,
||    takže vidí i nepotvrzené změny publish.
|| 2. Vlastní vnořený BEGIN ... EXCEPTION blok: chyba cache nesmí vrátit zpět celou publikaci
||    (vnější EXCEPTION blok auto_rel_pub_inc() by vrátil i všechny dosavadní změny publish).
|| 3. Do DECLARE funkce doplnit:  i_cache_cnt integer;   -- Počet řádků vložených do cache spotplánů
|| 4. Do hlavičky funkce doplnit Změnil / Změněno dne / Popis změny.
|| 5. Pozor: f_fill_crp_cache_temp() kromě cache plní i auditní tabulky rel_pub.crp_cache_log a
||    rel_pub.crp_ims_input_inc_log (+ cache_ok). Po nahrazení se tyto tabulky přestanou plnit.
||    Pokud je audit potřeba, nech volání f_fill_crp_cache_temp() jen pro logování, nebo audit
||    doplň zvlášť (otevřený bod v dokumentaci, kapitola 9).
||
|| Vytvořil: Jiří Povolný
|| Vytvořeno dne: 2026-10-13
||================================================================================================*/

/* ---------------------------- ZAČÁTEK VKLÁDANÉHO BLOKU ------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------
    -- Inkrementální aktualizace cache spotplánových VAC/ROTS (ids.ids_vac_calc_campaign_cache2, rvloadid 100)
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Spotplan cache update start.';

    BEGIN
      SELECT ids.f_spotplan_cache_update(100) INTO i_cache_cnt;
      RAISE NOTICE '% | auto_rel_pub_inc | spotplan cache 100: % radku', CLOCK_TIMESTAMP(), i_cache_cnt;
      PERFORM rel_pub.log_msg('INFO', 'fce:auto_rel_pub_inc', 'Spotplan cache 100 updated, inserted rows: ' || i_cache_cnt);
    EXCEPTION
      WHEN others THEN
        -- záměrně bez textu "exception:", ať chyba cache nevypadá jako pád celého běhu
        RAISE NOTICE 'WARNING: spotplan cache 100 failed: %', SQLERRM;
        PERFORM rel_pub.log_msg('ERROR', 'auto_rel_pub_inc', 'Spotplan cache 100 failed: ' || SQLERRM);
    END;

--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Spotplan cache update end.';
---------------------------------------------------------------------------------------------------------------------------------

   ----------------------------- KONEC VKLÁDANÉHO BLOKU ---------------------------------------- */
