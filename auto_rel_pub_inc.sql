/**********************************************************************************************
*
*	Vyrobil: Jirka Povolný, 8.3.2026
*
*	Používá se k automatickému výpočtu CCF na denní bázi.
*	A už i k dennímu, kontinuálnímu publikování.dbg_ims_input_inc
*
*	
*
**********************************************************************************************/
DECLARE
  exe_str varchar;			-- String pro poskládání SQL příkazů
  last_load_id integer;	-- Id posledního loadu do etl tabulky pro IDS
  i_cnt integer;				-- Počet záznamů ke zpracování
  navteqver_str varchar;-- Poslední navteqver z params()
  
BEGIN
-------------------------------------------------------------------------------------------------------------------
--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Function auto_rel_pub_inc start.';

	-- Načtení aktuální navteqver pro kontrolu správného navteqver u panelů.
	SELECT navteq_version INTO navteqver_str FROM fnc.f_params();

--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Navteq version: ' || navteqver_str;


  -- INSERT panelů, kde proběhla za poslední den změna v timestamp.
  -- Rovněž se přidají panely z tabulky crp_missing, kam se dají přidávat panely "z venku", ručně.
  -- Do tabulky crp_missing se také budou ukládat panely, 
  -- které se budou přepočítávat online strojkem, aby se zprocesovaly v dalším běhu (viz níže).
  -- Tabulka crp_geom_panel obsahuje seznam panelů s jejich geometrií (tabulka s panely a jejich kolaci, vzdalenost maxdist + 200m).

  TRUNCATE rel_pub.crp_geom_panel;

--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Create panels list (crp_geom_panel).';
  -- Insert panelů, den starých, se statusem 3.
  -- Insert panelů z tabulky crp_missing. Tam se dávají panely ručně, ty, které musím z nějakého důvodu zpracovat. 
  -- Insert panelů pro owner=51 ve stavu 1. Nějaká výjimka.
  INSERT INTO 
    rel_pub.crp_geom_panel
  (
    mid,
    pid,
    the_geom,
    buffer_geom,
    maxdist
  )
  SELECT
    a.mid,
    a.pid,
    b.the_geom,
    geometry(st_buffer(geography(b.the_geom),a.maxdist + 200)) AS buffer_geom,
    a.maxdist
  FROM
    pnl.pnl_main a
    INNER JOIN pnl.pnl_geom b ON a.mid = b.mid
    INNER JOIN pnl.mv_pnl_main_maxdate c ON a.mid = c.mid
  WHERE 
    (c.stamp > CURRENT_DATE - 1 AND c.status = 3 AND c.timeattid = 0)
    OR a.pid IN(SELECT pid FROM rel_pub.crp_missing)
  ;

  -- INSERT panelů, které byly poslední den "oddemolovány".
  INSERT INTO 
    rel_pub.crp_geom_panel
  (
    mid,
    pid,
    the_geom,
    buffer_geom,
    maxdist
  )
  SELECT
    a.mid,
    a.pid,
    b.the_geom,
    geometry(st_buffer(geography(b.the_geom),a.maxdist + 200)) AS buffer_geom,
    a.maxdist
  FROM
    pnl.pnl_main a
    INNER JOIN pnl.pnl_geom b ON a.mid = b.mid
    INNER JOIN pnl.mv_pnl_main_maxdate c ON a.mid = c.mid
  WHERE 
    (c.status = 3 AND a.pid IN (SELECT pid FROM pnl.pnl_timeatts t WHERE t.stamp > CURRENT_DATE - 1 AND t.timeattid = 0))
  ;

--** Tady je NOTICE **--	
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Updating last_vacid, err_vac_res.';

  --Doupdate posledního vacid k panelům a případné chyby, pokud při výpočtu panelu vznikla
  UPDATE
    rel_pub.crp_geom_panel x
  SET
    last_vacid = sqry.vacid,
    err_vac_res = sqry.err_vac_res
  FROM
  (
    SELECT DISTINCT ON (v.mid)
      v.vacid,
      v.mid,
      v.err_vac_res
    FROM 
      vai.vai_vac_res v JOIN rel_pub.crp_geom_panel p ON v.mid = p.mid
    WHERE
      1 = 1
      --AND p.mid = 53837
    ORDER BY
      v.mid,
      v.vacid DESC
      ) sqry
    WHERE x.mid = sqry.mid
  ;

--** Tady je NOTICE **--	
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Updating VACs.';

  --Doupdate VAC k panelům, bere se transptype = 'ALL' a periodid = 1, proto se pole jmenuje "vac_all_1"
  UPDATE
    rel_pub.crp_geom_panel x
  SET
    vac_all_1 = sqry.vac
  FROM
  (
    SELECT DISTINCT ON (v.mid)
      v.mid,
      v.vacid,
      r.vac
    FROM
      vai.vai_vac_res v
      JOIN res.res_online_ea_output_round r ON v.vacid = r.vacid
    WHERE
      r.periodid = 1
      AND r.transptype = 'ALL'
    ORDER BY
      v.mid,
      v.vacid DESC
  ) sqry
  WHERE
    x.last_vacid = sqry.vacid
  ;

--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Non calculating panels management.';

  -- Zaznamenám panely, které nemají napočítáno nebo skončily s chybou, ty se, mimo jiné, nedostanou do výstupu
  -- V tabulce rel_pub.crp_no_calc_inc se tyto panely budou kumulovat. 
  -- Postupně se pak musí spočítat a následně zpracovat.
  -- Vymažu již zpracované, ty co se mají ignorovat tam nechávám na věčnou památku a taky abych si je pamatoval na příště.
--** Tady je NOTICE **--	
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Deleting processed panels from crp_no_calc_inc.';
  DELETE FROM rel_pub.crp_no_calc_inc WHERE processed = TRUE AND ignore = FALSE;

--** Tady je NOTICE **--	
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Inserting panels to from crp_no_calc_inc. (vac_all_2 = 0 OR vac_all_2 IS NULL OR err_vac_res IS NOT NULL)';
  INSERT INTO rel_pub.crp_no_calc_inc (pid, mid, err_vac_res)
  SELECT 
    pid,
    mid,
    err_vac_res
  FROM 
    rel_pub.crp_geom_panel 
  WHERE
    (vac_all_1 = 0 OR vac_all_1 IS NULL OR err_vac_res IS NOT NULL)
    AND pid NOT IN (SELECT pid FROM rel_pub.crp_no_calc_inc) -- Vkládám jen ty, co v tabulce ještě nejsou.
  ;
  -- A vyřadím je z dalších akcí.
  DELETE FROM rel_pub.crp_geom_panel WHERE pid IN (SELECT pid FROM rel_pub.crp_no_calc_inc/* WHERE calculated = FALSE*/);

  -- Doupdatnu errory, protože se mohly změnit i u těch záznamů, které už tam (crp_no_calc_inc) jsou.
  UPDATE rel_pub.crp_no_calc_inc n SET err_vac_res = p.err_vac_res FROM rel_pub.crp_geom_panel p WHERE n.pid = p.pid;

  ---------------------------------------------------------------------------------------------------------------------
  -- Update ocenění (náročnosti) výpočtu v crp_no_calc_inc
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Calculation of panels calc cost.';

  WITH TargetMids AS (
    -- Základní sada panelů
    SELECT n.pid, n.mid 
    FROM rel_pub.crp_no_calc_inc n
    WHERE 
      1 = 1
      AND n.in_queue = FALSE
  ),
  CleanedSegments AS (
    -- Výběr unikátních segmentů s vyřazením linetype ('70','71','72')
    SELECT DISTINCT ON (s.mid, s.navteqid, s.orient)
        tm.pid,
        s.mid,
        s.navteqid,
        s.orient,
        ROUND(s.x1::numeric, 6) as x1, ROUND(s.y1::numeric, 6) as y1,
        ROUND(s.x2::numeric, 6) as x2, ROUND(s.y2::numeric, 6) as y2
    FROM vai.vai_seg s
    INNER JOIN TargetMids tm ON s.mid = tm.mid
    -- Přidání JOINu pro filtraci linetype (Comment, BALT zatím nemá)
    /*INNER JOIN topo.topo_streets_pli b ON s.navteqid = b.link_id AND s.navteqver = b.navteqver
    WHERE 
        b.linetype NOT IN ('70', '71', '72')*/
  ),
  UniqueNodes AS (
    SELECT mid, x1 AS x, y1 AS y FROM CleanedSegments
    UNION
    SELECT mid, x2 AS x, y2 AS y FROM CleanedSegments
  ),
  Stats AS (
    SELECT 
        c.pid,
        c.mid,
        COUNT(*)::float AS total_segments,
        (SELECT COUNT(*)::float FROM UniqueNodes u WHERE u.mid = c.mid) AS total_nodes
    FROM CleanedSegments c
    GROUP BY c.pid, c.mid
  ),
  CalculatedResults AS (
    SELECT 
        pid,
        mid,
        total_segments,
        total_nodes,
        ROUND((total_segments / NULLIF(total_nodes, 0))::numeric, 3) AS complexity_index
    FROM Stats
  )
  UPDATE
  	rel_pub.crp_no_calc_inc n
  SET 
  	complexity_index = sqry.complexity_index,
  	routing_difficulty = sqry.routing_difficulty
  FROM
  (
    SELECT 
        pid,
        mid,
        total_segments,
        total_nodes,
        complexity_index,
        CASE 
            WHEN complexity_index < 1.5 THEN 'LOW (Linear/Simple)'
            WHEN complexity_index BETWEEN 1.5 AND 2.2 THEN 'MEDIUM (Standard)'
            WHEN complexity_index > 2.2 THEN 'HIGH (Complex Mesh - slow)'
            ELSE 'UNDEFINED'
        END AS routing_difficulty
    FROM CalculatedResults
  ) sqry
  WHERE
  n.pid = sqry.pid;

  --------------------------------------
  -- Přesun problematických do tabulky problematických (rel_pub.crp_pnl_to_revise) = insert -> delete
  -- INSERT
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' INSERT of costly panels to table crp_pnl_to_revise and DELETE from crp_no_calc_inc.';

  INSERT INTO 
    rel_pub.crp_pnl_to_revise
  (
    pid,
    mid,
    err_vac_res,
    complexity_index,
    routing_difficulty
  )
  SELECT
    pid,
    mid,
    err_vac_res,
    complexity_index,
    routing_difficulty
  FROM
    rel_pub.crp_no_calc_inc n
  WHERE
    n.in_queue = FALSE
    AND n.ignore = FALSE
    --AND (n.complexity_index > 2.2 OR n.err_vac_res IS NOT NULL)
    AND n.err_vac_res IS NOT NULL
    AND pid NOT IN (SELECT pid FROM rel_pub.crp_pnl_to_revise) -- Jen ty, co tam ještě nejsou.
  ;

  -- DELETE
  DELETE FROM
    rel_pub.crp_no_calc_inc n
  WHERE
    n.in_queue = FALSE
    --AND (n.complexity_index > 2.2 OR n.err_vac_res IS NOT NULL)
    AND n.err_vac_res IS NOT NULL
  ;

  ---------------------------------------------------------------------------------------------------------------------

  -- Postup pro nespočítané panely
  -- V tabulce crp_no_calc_inc jsou 4 sloupce
  -- - in_queue = zařazeno v oper_vac_queue k výpočtu
  -- - calculated = panel je spočítám, může se zpracovat. Kromě těch, co mají v počítání chybu v err_vac_res
  -- - processed = panel zprocesován
  -- - ignore = panel se zcela ignoruje, např. pid = 534, který má ve výpčtu vždy = 0
  -- Princip je, že nespočítané panely ze crp_no_calc_inc vložím do strojku (oper_vac_queue, table = 50020) a zaškrtnu in_queue
  -- Před tímto skriptem se pustí jiný skript, který zkontroluje panely ve frontě a zaškrtne jim calculated
  -- To způsobí, že se panely dostanou do zpracování a to tak, že se do tabulky crp_missing zapíší všechny panely
  -- ze crp_no_calc_inc, které mají zaškrtnuto in_queue a calculated, ale processed = false (krom chyby v err_vac_res)
  -- Panelům se zaškrtne processed, tím jsou hotový, tímhle skriptem zpravovaný

  -- Začátek propočítávání panelů -------------------------
  -- Vložím do queue

  INSERT INTO oper.oper_vac_queue (vacid,faceid,mid,usrn,stamp_in_queue,priority,rmloadid,temp_table)
  SELECT 0, sqry.faceid, sqry.mid, 'init_continous_release', CURRENT_TIMESTAMP, 1, 0, 50820 FROM
    ( 
      SELECT DISTINCT ON (pid)
        p.pid,
        p.mid,
        f.faceid
      FROM
        rel_pub.crp_no_calc_inc z
        JOIN pnl.pnl_main p ON z.pid = p.pid
        JOIN pnl.pnl_facemid f ON p.mid = f.mid
      WHERE
        z.pid IN(SELECT pid FROM rel_pub.crp_no_calc_inc WHERE in_queue = FALSE AND ignore = FALSE AND err_vac_res IS NULL ORDER BY pid)
      ORDER BY p.pid, p.mid DESC
    )sqry
  ;

  -- Označím vložené do queue
  UPDATE
    rel_pub.crp_no_calc_inc
  SET
    in_queue = TRUE
  WHERE 
    in_queue = FALSE  
  ;

  -- Když jsou panely spočítané, nahodí se jim příznak "calculated" ....
  UPDATE rel_pub.crp_no_calc_inc nc
    SET calculated = TRUE
  WHERE
    nc.pid IN(
    SELECT DISTINCT 
      p.pid 
    FROM 
      oper.oper_vac_queue q
      JOIN pnl.pnl_main p ON q.mid = p.mid
      JOIN rel_pub.crp_no_calc_inc n ON p.pid = n.pid
    WHERE
      q.usrn = 'init_continous_release'
      AND q.temp_table = 50820
      AND q.stamp_end IS NOT NULL
      AND n.err_vac_res IS NULL
      AND n.ignore = FALSE
      AND q.stamp_in_queue > CURRENT_DATE - 4
      AND n.processed = FALSE
    )
  ;

  --  .... a zařadí se do zpracování.
  INSERT INTO 
    rel_pub.crp_geom_panel
  (
    mid,
    pid,
    the_geom,
    buffer_geom,
    maxdist
  )
  SELECT
    a.mid,
    a.pid,
    b.the_geom,
    geometry(st_buffer(geography(b.the_geom),a.maxdist + 200)) AS buffer_geom,
    a.maxdist
  FROM
    pnl.pnl_main a
    INNER JOIN pnl.pnl_geom b ON a.mid = b.mid
    INNER JOIN pnl.mv_pnl_main_maxdate c ON a.mid = c.mid
    LEFT JOIN rel_pub.crp_geom_panel d ON a.pid = d.pid
  WHERE 
    a.pid IN(SELECT pid FROM rel_pub.crp_no_calc_inc WHERE in_queue = TRUE AND calculated = TRUE AND processed = FALSE AND ignore = FALSE)
    AND d.pid IS NULL
  ;

  -- Teď jsem je zařadil ke zpracování a nahodím jim "processed".
  -- Označím vložené do crp_geom_panel
  UPDATE
    rel_pub.crp_no_calc_inc
  SET
    processed = TRUE
  WHERE 
    calculated = TRUE 
    AND processed = FALSE
    AND ignore = FALSE 
  ;

  -- Konec propočítávání panelů ---------------------------

  -- Odtranění duplicit v seznamu panelů.
  TRUNCATE TABLE rel_pub.crp_geom_panel_temp;
  INSERT INTO rel_pub.crp_geom_panel_temp SELECT DISTINCT * FROM rel_pub.crp_geom_panel;
  TRUNCATE TABLE rel_pub.crp_geom_panel;
  INSERT INTO rel_pub.crp_geom_panel SELECT * FROM rel_pub.crp_geom_panel_temp;

  -- Počet panelů ke zpracování, použije se v IF níže pro test, jestli je co zpracovat, jinak se to skočí.
  SELECT COUNT(*) INTO i_cnt FROM rel_pub.crp_geom_panel;

--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Panels to process:' || i_cnt;

--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Creating unpublish list.';
  -------------------------------------------------------------------------------------------------------------------------
  -- Unpublish Start ------------------------------------------------------------------------------------------------------
  -- Všechny panely k odpublikování se nejdříve načtou do tabulky crp_unpublish_list
  -- Z publish smažu, je když nemám debug mód.
  -- Přesunuto z hlavního IFu fce, musí se spustit i když se nenajdou žádné panely CURRENT_TIME -1

  -- Nejdříve promáznu seznam od posledně
  TRUNCATE rel_pub.crp_unpublish_list;

  -- Z publishe smáznu panely se změněným statusem a zdemolované, abych si pak 
  -- nesmáznul ty, který jsem zpracoval.
  -- Odpublikování Status 1, 2, 4
  INSERT INTO 
    rel_pub.crp_unpublish_list
  (
    mid,
    pid,
    desc_text
  )
  SELECT
    m.pid,
    m.pid,
    'Unpublished - Status changed to 1, 2 or 4'
  FROM
    pnl.mv_pnl_main_maxdate m
  WHERE 
    m.stamp > CURRENT_DATE - 1
    AND m.status IN(1, 2, 4)
  ;

  -- Přidám ty, co nám mohly v minulosti proklouznout
  INSERT INTO 
    rel_pub.crp_unpublish_list
  (
    pid,
    desc_text
  )
  SELECT mx.pid, 'Unpublished - demolished, past cases' FROM pnl.mv_pnl_main_maxdate mx
  WHERE 
    mx.pid IN (SELECT DISTINCT x.pid
               FROM
                (SELECT pid, stamp, status, timeattid 
                  FROM pnl.mv_pnl_main_maxdate 
                  WHERE 
                  (status = 3 AND timeattid = 0)
                ) m
                RIGHT JOIN 
                (SELECT DISTINCT pid 
                  FROM res.res_release_publish 
                  WHERE rvloadid IN(100, 101)) x ON m.pid = x.pid
              WHERE
                m.pid IS NULL
      						
  )
  AND mx.timeattid <> 0
  AND mx.stamp <= CURRENT_DATE - 1
  ORDER BY
    mx.pid
  ;

  -- Přidám zdemolované na seznam
  INSERT INTO 
    rel_pub.crp_unpublish_list
  (
    pid,
    desc_text
  )
  SELECT 
    pid,
    'Unpublish - demolished' 
  FROM 
    pnl.pnl_timeatts t 
  WHERE 
    t.stamp > CURRENT_DATE - 1
    AND t.timeattid <> 0
  ;

  -- Smažu pidy které jdou do zpracování
  DELETE FROM rel_pub.crp_unpublish_list u
  USING
    (
      SELECT 
        pid
      FROM 
        rel_pub.crp_geom_panel
    ) sqry
  WHERE u.pid = sqry.pid
  ;

  -- Odpublikování podle seznamu v crp_unpublish_list
--** Tady je NOTICE **--
  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Unpublish by the list in crp_unpublish_list table.';

  DELETE FROM res.res_release_publish p
  USING
    (
      SELECT 
        pid 
      FROM 
        rel_pub.crp_unpublish_list
    ) sqry
  WHERE p.pid = sqry.pid AND p.rvloadid IN (100, 101)
  ;

--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Logging unpublished panel list.';
  -- A ještě evidence / log
  INSERT INTO 
    rel_pub.crp_geom_panel_log
    (
      mid,
      pid,
      desc_text
    )
    SELECT 
      mid,
      pid,
      desc_text
    FROM 
      rel_pub.crp_unpublish_list
  ;
  -- Unpublish Konec -----------------------------------------------------------------------------------------------------


  -- Panely ze crp_missing jsou vloženy pro výpočet a použity pro unpublish, možno smazat.
  --TRUNCATE rel_pub.crp_missing;


  -- Hlavní zpracování, výpočty ------------------------------------------------------------------------------------------
  IF i_cnt > 0 THEN
    -- Založí se nový load a sosne se jeho id do proměnné load_id.
    INSERT INTO rel_pub.etl_ccf_rel_pub_loads_inc ("current_user")
    VALUES (CURRENT_USER);

    SELECT load_id INTO last_load_id FROM rel_pub.etl_ccf_rel_pub_loads_inc ORDER BY load_id DESC;
        
--** Tady je NOTICE **--
		RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' New load_id:' || last_load_id;

--** Tady je NOTICE **--
		RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Recs found:' || i_cnt;

  
--** Tady je NOTICE **--
  	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Bad navteq management.';

    -- Test na správný navteq version.
    -- Pokud má panel špatný navteqver, uloží se záznam do tabulky crp_bad_navteq a vyhodí z počítání.
    -- Nejdříve smažeme panely zprocesované posledně
    DELETE FROM rel_pub.crp_bad_navteq WHERE processed = 1;

    -- Zkontroluji a uložim špatný navteqver na seznam
    INSERT INTO rel_pub.crp_bad_navteq
    (
      mid,
      pid,
      navteqver
    )
    SELECT
      a.mid,
      a.pid,
      a.navteqver
    FROM
      pnl.pnl_main a
      INNER JOIN rel_pub.crp_geom_panel b ON a.mid = b.mid
    WHERE
      a.navteqver <> navteqver_str
    ;
      
    -- Špatný navteq se nebude počítat
    DELETE FROM rel_pub.crp_geom_panel a
    WHERE a.mid IN (SELECT mid FROM rel_pub.crp_bad_navteq);
  	
    -- Potom se označí opravené pidy
    UPDATE 
      rel_pub.crp_bad_navteq 
    SET 
      processed = 1
    WHERE 
      pid IN
      (SELECT z.pid 
        FROM rel_pub.crp_bad_navteq z 
        JOIN pnl.pnl_main p ON z.pid = p.pid 
        WHERE p.navteqver = navteqver_str
      )
      AND processed = 0;
    
    -- A opravené se vrátí/nově vloží na seznam
    INSERT INTO 
      rel_pub.crp_geom_panel
    (
      mid,
      pid,
      the_geom,
      buffer_geom,
      maxdist
    )
    SELECT
      a.mid,
      a.pid,
      b.the_geom,
      geometry(st_buffer(geography(b.the_geom),a.maxdist + 200)) AS buffer_geom,
      a.maxdist
    FROM
      pnl.pnl_main a
      INNER JOIN pnl.pnl_geom b ON a.mid = b.mid
      INNER JOIN pnl.mv_pnl_main_maxdate c ON a.mid = c.mid
    WHERE 
      a.pid IN(SELECT pid FROM rel_pub.crp_bad_navteq WHERE processed = 1)
    ;	
      	
    -- Zde se vytvoří crp_ims_input_inc (původně ims_input) pro nalezené změněné panely
    PERFORM rel_pub.create_ims_input_inc();

    ---------------------------------------------------------
    -- Prusecik kolacu bufferu a tripu. obe tabulky by mely byt naindexovane. panely o dotaz vyse, tripy od RM
    --TRUNCATE rel_pub.crp_intersection;

--** Tady je NOTICE **--	
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Calculating intersection.';

    -- Výpočet všech intersekcí, prusecik kolacu bufferu a tripu.
    TRUNCATE rel_pub.crp_intersection;
    INSERT INTO rel_pub.crp_intersection
    SELECT
      a.mid, b.sid--, st_length(geography(st_intersection(a.buffer_geom,b.geom))) as intrs_len
    FROM
      rel_pub.crp_geom_panel a
      INNER JOIN rel_pub.crp_routing b 
        ON a.buffer_geom && b.geom       					-- Indexový test obálek
        AND ST_Intersects(a.buffer_geom, b.geom)	-- Přesný prostorový test
    ;
    	
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Updating intersection type in panel list (crp_geom_panel).';
      
    --Doupdate 'ORIGINAL' a 'SUBSTITUTE' podle spočítaných intersekcí.
    --Panely, které nemají v crp_intersection_inc záznam se musí dohledat (níže).
    --Doupdate 'ORIGINAL'
    UPDATE rel_pub.crp_geom_panel
    SET intersection_type = 'ORIGINAL'
    WHERE mid IN(SELECT DISTINCT mid FROM rel_pub.crp_intersection);

    --Doupdate 'SUBSTITUTE'
    UPDATE rel_pub.crp_geom_panel
    SET intersection_type = 'SUBSTITUTE'
    WHERE intersection_type IS NULL;
    	
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Filling crp_evidence from crp_intersection (ORIGINAL).';

    --Celkový seznam, naplnění tabulky, nejprve ty, co mají průsečík s trasou, pak se sem pak přidají panely bez průsečíku
    TRUNCATE rel_pub.crp_evidence;
    INSERT INTO rel_pub.crp_evidence
    SELECT mid, sid
    FROM rel_pub.crp_intersection;

--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Listing panels wo intersection to crp_panel_wo_its.';

    --Panely bez intersekce
    TRUNCATE rel_pub.crp_panel_wo_its;
    INSERT INTO rel_pub.crp_panel_wo_its
    SELECT 
      mid,
      pid,
      the_geom,
      buffer_geom,
      vac_all_2
    FROM
      rel_pub.crp_geom_panel 
    WHERE intersection_type = 'SUBSTITUTE';
	
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Finding nearest panels for panels wo intersection (best of 20).';

    --Nalezeni nejblizsiho MIDu (pro vsechny verze tripu) a doplneni sidu z tech tripu do evidence delanych midu
    TRUNCATE TABLE rel_pub.crp_nearest20;
    WITH nearest20 AS (
        -- 1. Pro každý bod T2 najdeme 20 geometricky nejbližších bodů T1
        SELECT
            t2.mid AS mid_woi,
            t2.vac_all_2 AS vac_woi,
            t1.mid AS mid_near,
            t1.vac_all_2 AS vac_near,
            ST_Distance(t2.the_geom, t1.the_geom) AS distance,
            ROW_NUMBER() OVER (
                PARTITION BY t2.mid
                ORDER BY t2.the_geom <-> t1.the_geom -- Řazení podle vzdálenosti s GiST indexem
            ) as rn_geom,  -- Pořadí podle geometrické vzdálenosti
            t1.pid AS pid_near
        FROM
            rel_pub.crp_panel_wo_its t2,
            rel_pub.crp_all_panel t1 -- Používáme implicitní JOIN pro KNN dotaz
        -- WHERE ST_DWithin(t2.geom, t1.geom, 5000) -- Volitelné omezení maximální vzdálenosti (např. 5 km)
        WHERE
          t1.intersection_type = 'ORIGINAL'
    )
    , best_match AS (
        -- 2. Z 20 nejbližších vybereme ten s minimálním rozdílem hodnot
        SELECT
            n20.*,
            ABS(n20.vac_woi - n20.vac_near) AS value_difference,
            ROW_NUMBER() OVER (
                PARTITION BY n20.mid_woi
                ORDER BY
                    ABS(n20.vac_woi - n20.vac_near) ASC, -- Primární řazení: nejmenší rozdíl hodnot
                    n20.distance ASC                     -- Sekundární řazení (tie-breaker): nejmenší geometrická vzdálenost
            ) as rn_value -- Pořadí podle rozdílu hodnot
        FROM
            nearest20 n20
        WHERE
            n20.rn_geom <= 20 -- Omezíme na 20 nejbližších z hlediska geometrie
    )
    INSERT INTO rel_pub.crp_nearest20
    SELECT
        bm.mid_woi,
        bm.vac_woi,
        bm.mid_near AS best_mid_near,
        bm.vac_near AS best_vac_near,
        bm.distance AS geometric_distance,
        bm.value_difference,
        bm.pid_near AS best_pid_near
    FROM
        best_match bm
    WHERE
        bm.rn_value = 1;

		--**--!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

  	
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Filling substituted panels';
    
    --Tady je to trochu zašmodrchaný, nejdříve update the_geom a buffer_geom podle near midů (best_mid_near)
    UPDATE rel_pub.crp_nearest20 n
    SET 
      the_geom = sqry.the_geom,
      buffer_geom = sqry.buffer_geom
    FROM (
      SELECT
        a.mid,
        a.pid,
        b.the_geom,
        geometry(st_buffer(geography(b.the_geom),a.maxdist + 200)) AS buffer_geom,
        a.maxdist
      FROM
        pnl.pnl_main a
        INNER JOIN pnl.pnl_geom b ON a.mid = b.mid
        INNER JOIN rel_pub.crp_nearest20 c ON a.mid = c.best_mid_near
      ) sqry
    ;
    
    -- Doplním do crp_intersection, geom mám z best nearest, tak se najdou průsečíky,
    -- ale mid je z toho neprotnutýho panelu (mid_woi - mid without intersection)
    INSERT INTO rel_pub.crp_intersection
    SELECT
      a.mid_woi, b.sid
    FROM
      rel_pub.crp_nearest20 a
      INNER JOIN rel_pub.crp_routing_ps b ON st_intersects(a.buffer_geom, b.geom) AND a.buffer_geom && b.geom
    ;
    
    INSERT INTO rel_pub.crp_evidence  
    SELECT 
      mid_woi,
      sid
    FROM 
      rel_pub.crp_nearest20 a
      JOIN rel_pub.crp_intersection b ON a.mid_woi = b.mid
    ;
    -- Teď mám v crp_intersection vložený i panely bez kontaktů, nefejkovaný podle panelů, který kontakt mají, 
    -- jsou blízko a mají podobný hodnoty
    
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Filling panels to heap of all panels (crp_all_panel)';
  		
    -- UPSERT: Aktualizujte existující řádky v crp_all_panel daty z crp_geom_panel,
    -- a přidá ty, které chybí, vč. vac, vacid, intersection_type..
    UPDATE rel_pub.crp_all_panel AS a
    SET
        mid = b.mid,
        the_geom = b.the_geom,
        buffer_geom = b.buffer_geom,
        vac_all_2 = b.vac_all_2,
        maxdist = b.maxdist,
        intersection_type = b.intersection_type,
        last_vacid = b.last_vacid
    FROM
        rel_pub.crp_geom_panel AS b
    WHERE
        a.mid = b.mid;

    INSERT INTO rel_pub.crp_all_panel
    SELECT
        b.mid,
        b.pid,
        b.the_geom,
        b.buffer_geom,
        b.vac_all_2,
        b.maxdist,
        b.intersection_type,
        b.last_vacid
    FROM
        rel_pub.crp_geom_panel AS b
    LEFT JOIN
        rel_pub.crp_all_panel AS a ON b.mid = a.mid
    WHERE
        a.mid IS NULL; -- Klíč z TABLE_A je NULL, pokud řádek nebyl nalezen

--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' End of panel preparation.';

--** Tady je NOTICE **--
	  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Creating ccf_input.';

    TRUNCATE rel_pub.crp_ccf_input;
    INSERT INTO rel_pub.crp_ccf_input
    (
      mid,
      sid,
      desc_text,
      sbjnum,
      routingmode,
      pid,
      tsyear
    )
    SELECT 
      e.mid,
      e.sid,
      e.desc_text,
      o.sbjnum,
      o.routingmode,
      m.pid,
      o.tsyear
    FROM 
      rel_pub.crp_evidence e
      JOIN rel_pub.crp_routing o ON e.sid = o.sid
      JOIN pnl.pnl_main m ON e.mid = m.mid
    ;
      
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Creating ccf_output.';

    TRUNCATE TABLE rel_pub.crp_ts_rots_temp;
    TRUNCATE TABLE rel_pub.crp_ccf_output;
    TRUNCATE TABLE rel_pub.crp_ims_temp;

    -- Optimalizováno, místo původních 4 dotazů na jeden, všechny tripy vloženy do crp_routing
    INSERT INTO rel_pub.crp_ccf_output (pid, sid, respweight, modality, dayofweek, sbjnum)
    SELECT 
      ci.pid, 
      ci.sid, 
      rwi.benchwgt_pp, 
      ci.routingmode, 
      rou.dayofweek,
      rwi.sbjnum
    FROM 
      rel_pub.crp_routing rou
      JOIN rel_pub.crp_ccf_input ci ON rou.sid = ci.sid
      JOIN rel_pub.crp_resp_weight_input rwi ON rou.sbjnum = rwi.sbjnum
    WHERE  
      ci.sbjnum = rwi.sbjnum
    ;

    -- Updatne weekDay
    UPDATE rel_pub.crp_ccf_output SET dayofweek = LEFT(dayofweek,2);
    	
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Trips filled to crp_ccf_output.';
  	
    -- Naplní crp_ims_temp
    INSERT INTO rel_pub.crp_ims_temp ( 
      pid, mid, pnlkey, pnldesc2, 
      facepid, ownid_desc, pnlillum_desc, 
      pnlmotion_desc, frmwidth, frmheight, pnlvaienv, 
      pnldesc, district, vac_brutto_weekly_all, vac_brutto_weekly_veh, 
      vac_brutto_weekly_ped, rots_brutto_weekly_all, rots_brutto_weekly_veh, rots_brutto_weekly_ped)
    SELECT 
      pid, mid, pnlkey, pnldesc2, 
      facepid, ownid_desc, pnlillum_desc, 
      pnlmotion_desc, frmwidth, frmheight, pnlvaienv, 
      pnldesc, district, vac_brutto_daily_all*7, vac_brutto_daily_veh*7, 
      vac_brutto_daily_ped*7, rots_brutto_daily_all*7, rots_brutto_daily_veh*7, rots_brutto_daily_ped*7
    FROM rel_pub.crp_ims_input_inc;

--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Table crp_ims_temp filled.';

    -- Naplní crp_ts_rots_temp
    INSERT INTO rel_pub.crp_ts_rots_temp ( pid, ts_rots )
    SELECT pid, SUM(respweight)
    FROM rel_pub.crp_ccf_output
    GROUP BY pid;

--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Table crp_ts_rots_temp filled.';

    -- Updatne TIM_performance
    UPDATE rel_pub.crp_ts_rots_temp trt
    SET tim_rots = it.rots_brutto_weekly_all, tim_vac = it.vac_brutto_weekly_all
    FROM rel_pub.crp_ims_temp AS it 
    WHERE trt.pid = it.pid;

--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Column tim_rots filled.';

    -- Updatne TS_VA
    UPDATE rel_pub.crp_ts_rots_temp SET ts_va = tim_vac/ts_rots;

    -- Updatne TS_Freq
    UPDATE rel_pub.crp_ts_rots_temp SET ts_freq = CASE WHEN ts_va > 1 THEN ts_va + 1 ELSE 1 END;

    -- Updatne VA_Adj
    UPDATE rel_pub.crp_ts_rots_temp SET ts_va_adj = ts_va/ts_freq;
    
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Table crp_ts_rots_temp updated.';

    -- Updatne VA a VAC
    UPDATE rel_pub.crp_ccf_output c
    SET 
      va = 	CASE WHEN trt.ts_freq < 20 THEN 1-(1-trt.ts_va_adj)^trt.ts_freq 
              ELSE 1
              END, 
      vac = trt.ts_va_adj*trt.ts_freq*respweight
    FROM rel_pub.crp_ts_rots_temp AS trt
    WHERE c.pid = trt.pid;

--** Tady je NOTICE **--
      RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Table crp_ccf_output updated.';

    -- Final CCF load
--** Tady je NOTICE **--
	  RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Final CCF load.';

    exe_str = 'INSERT INTO 
                  rel_pub.etl_ccf_rel_pub_inc
                 (
                  pid,
                  mid,
                  facepid,
                  faceid,
                  respondent_id,
                  weekdayname,
                  resp_weight,
                  va,
                  vac,
                  load_id
                ) 
                  SELECT 
                    crp_ccf_output.pid,
                    crp_ims_temp.mid, 
                    crp_ims_temp.facepid, 
                    0 AS facepid, 
                    crp_ccf_output.sbjnum, 
                    LOWER(crp_ccf_output.dayofweek), 
                    crp_ccf_output.respweight, 
                    crp_ccf_output.va, 
                    round(crp_ccf_output.vac), ' || last_load_id || '
                  FROM rel_pub.crp_ccf_output 
                    INNER JOIN rel_pub.crp_ims_temp ON crp_ccf_output.pid = crp_ims_temp.pid; ';
    EXECUTE exe_str;
    
    -- Ošetření NULL hodnot ve výsledku
    -- Nejdřív uklidim panely, co mají NULL do tabulky k revizi
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Treating NULL values ​​in the result.';

    INSERT INTO 
      rel_pub.crp_pnl_to_revise
    (
      pid,
      mid,
      load_id,
      va,
      vac,
      null_zero_result
    )
    SELECT DISTINCT
      pid,
      mid,
      load_id,
      va,
      vac,
      TRUE
    FROM 
      rel_pub.etl_ccf_rel_pub_inc 
    WHERE
      (va IS NULL OR vac IS NULL OR va < 0 OR vac < 0)
      AND pid NOT IN (SELECT pid FROM rel_pub.crp_pnl_to_revise WHERE load_id IS NOT NULL) -- Jen ty, co tam ještě nejsou.
    ;

    GET DIAGNOSTICS i_cnt = ROW_COUNT;
    
    -- Pak je smažu z outputu
    -- DELETE
    DELETE FROM
      rel_pub.etl_ccf_rel_pub_inc
    WHERE
      (va IS NULL OR vac IS NULL OR va < 0 OR vac < 0)
    ;
  
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Treating NULL values done. Affected rows: ' || i_cnt;

--** Tady je NOTICE **--
		RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Publishing.';
    -- Automatický publish --------------------------------------------------------------------------------------------------
    -- Delete publishe, pokud panely už mají publikované výsledky
    DELETE FROM 
      res.res_release_publish
    WHERE
      pid IN(SELECT pid FROM rel_pub.crp_ims_input_inc)
      AND rvloadid IN(104, 105)
    ;
      
    -- Continual Release IDS
    INSERT INTO res.res_release_publish 
    SELECT DISTINCT ON (sqry.facepid, sqry.transptype)
      104::integer as rvloadid, --!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      sqry.*
    FROM (
      SELECT DISTINCT
        fm.facepid,
        r.*,
        f.pid,
        fm.mid,
        fm.faceid
      FROM 
        res.res_online_ea_output_round r
        JOIN (SELECT DISTINCT ON (vvr.faceid)
                vvr.vacid,
                vvr.faceid,
                vvr.mid,
                vvr.err_vac_res
              FROM 
                rel_pub.crp_ims_input_inc nm
                JOIN pnl.pnl_facemid fm ON nm.mid = fm.mid
                JOIN vai.vai_vac_res vvr ON fm.faceid = vvr.faceid
              ORDER BY  
                vvr.faceid,
                vvr.vacid DESC
              ) v ON r.vacid = v.vacid
        JOIN pnl.pnl_facemid fm ON v.faceid = fm.faceid
        JOIN pnl.pnl_faces f ON fm.facepid = f.facepid
      WHERE 
        r.periodid = 2
    )sqry
    --WHERE sqry.facepid = 5490
    ORDER BY
      sqry.facepid, sqry.transptype, sqry.vacid DESC
    ;

    -- Continual Release IMS
    INSERT INTO res.res_release_publish 
    SELECT DISTINCT ON (sqry.facepid, sqry.transptype, sqry.periodid)
      105::integer as rvloadid, --!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      sqry.*
    FROM (
      SELECT DISTINCT
        fm.facepid,
        r.*,
        f.pid,
        fm.mid,
        fm.faceid
      FROM 
        res.res_online_ea_output_round r
        JOIN (SELECT DISTINCT ON (vvr.faceid)
                vvr.vacid,
                vvr.faceid,
                vvr.mid,
                vvr.err_vac_res
              FROM 
                rel_pub.crp_ims_input_inc nm
                JOIN pnl.pnl_facemid fm ON nm.mid = fm.mid
                JOIN vai.vai_vac_res vvr ON fm.faceid = vvr.faceid
              ORDER BY  
                vvr.faceid,
                vvr.vacid DESC
              ) v ON r.vacid = v.vacid
        JOIN pnl.pnl_facemid fm ON v.faceid = fm.faceid
        JOIN pnl.pnl_faces f ON fm.facepid = f.facepid
      WHERE 
        r.periodid IN (10,11)
    )sqry
    ORDER BY
      sqry.facepid, sqry.transptype, sqry.periodid, sqry.vacid DESC
    ;

    -- Evidence / log zpracovaných panelů v tomto loadu
    INSERT INTO 
      rel_pub.crp_geom_panel_log
    (
      mid,
      pid,
      the_geom,
      buffer_geom,
      load_id,
      desc_text
    )
    SELECT 
      mid,
      pid,
      the_geom,
      buffer_geom,
      last_load_id,
      'Processed by load'
    FROM 
      rel_pub.crp_geom_panel
    ;
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Publishing end.';
  	
	  -- Znovu se načte, některé panely mohly být vyřazeny
    SELECT COUNT(*) INTO i_cnt FROM rel_pub.crp_geom_panel;

--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Load ID: ' || last_load_id || ' - successfuly done. Num of recs: ' || i_cnt || '.';
    PERFORM rel_pub.log_msg('INFO', 'fce:auto_rel_pub_inc', 'Load ID: ' || last_load_id || ' - successfuly done. Num of recs: ' || i_cnt || '.');

	ELSE
--** Tady je NOTICE **--
    RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' Recs(panels) not found, exiting.';
    PERFORM rel_pub.log_msg('INFO', 'fce:auto_rel_pub_inc', 'Load did not run, no panel records.');
  END IF; -- IF i_cnt >

--** Tady je NOTICE **--
	RAISE NOTICE '%', CLOCK_TIMESTAMP() || ' The end.';

  EXCEPTION
  WHEN others THEN
    RAISE NOTICE 'exception: %', SQLERRM;
    PERFORM rel_pub.log_msg('ERROR', 'auto_rel_pub_inc', 'Error occured: ' || SQLERRM);

END;