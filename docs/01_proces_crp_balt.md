# CRP BALT – popis procesu

Zdroj: [`ddl/schema_rel_pub.sql`](../ddl/schema_rel_pub.sql), export schématu `rel_pub` z databáze `imsbalt_prod`
k 21.9.2026. Regionální instance procesu CRP (Continuous Release Publishing) pro Pobaltí (Litva, Lotyšsko,
Estonsko) – stejná architektura jako CRP ZA, ale samostatná databáze a (viz níže) v některých detailech odlišný
stav kódu.

## Účel

Proces denně přepočítává a publikuje výkony reklamních panelů (VAC – vizuálně adjustované kontakty, "vac") ze
systému IMS do systému IDS. Vstupem jsou naměřená/modelovaná data o trasách respondentů (`res`, `vai`, `pnl`
schémata), výstupem je publikovaná tabulka `res.res_release_publish` a ETL tabulka `rel_pub.etl_ccf_rel_pub_inc`,
které čte cílový systém IDS (mimo tento repozitář).

## Funkce – přehled

Schéma `rel_pub` obsahuje 7 plpgsql funkcí:

| Funkce | Vyrobil / datum | Volá se | Účel |
|---|---|---|---|
| `auto_rel_pub_ccf()` | Jirka Povolný, 2.2.2024 | ručně / vstupní bod dle zadání | Plný/inicializační přepočet CCF přes všechny relevantní panely. |
| `auto_rel_pub_inc()` | Jirka Povolný, 8.3.2026 | denně (scheduler mimo tento repozitář) | Přírůstkový denní přepočet CCF **a jediná funkce, která reálně publikuje** do `res.res_release_publish` a `rel_pub.etl_ccf_rel_pub_inc`. Na konci navíc plní cache pro dynamické VA (viz níže). |
| `create_ims_input_ccf()` | Jirka Povolný, 2.2.2024 | z `auto_rel_pub_ccf()` | Naplní `rel_pub.crp_ims_input` – snapshot vlastností a týdenních VAC/ROTS/VA panelů pro plný běh. |
| `create_ims_input_inc()` | Jirka Povolný, 2.2.2024 | z `auto_rel_pub_inc()` | Totéž jako výše, ale do `rel_pub.crp_ims_input_inc` pro přírůstkový běh. |
| `f_fill_crp_cache_temp()` | – | z `auto_rel_pub_inc()` | Dopočítá dynamickou viditelnost (`dynva`) pro digitální/pohyblivé panely (`pnlmotion = 'D'`) podle spot length/pause a uloží do cache `ids.ids_vac_calc_campaign_cache2`. |
| `log_msg()` | – | ze všech ostatních funkcí | Zapíše řádek do `rel_pub.crp_log_table` (perzistentní log); chyby při zápisu potichu polyká. |
| `re_publish_crp()` | Jirka Povolný, 10.3.2026 | ručně | Bez přepočtu znovu naplní `res.res_release_publish` (rvloadid 100, 101) přímo z `res.res_online_ea_output_round` – jednorázová "reset & republish" utilita. |

## Datový tok – hlavní (denní) běh `auto_rel_pub_inc()`

1. **Výběr panelů ke zpracování** do `crp_geom_panel` – panely změněné za poslední den (`pnl.mv_pnl_main_maxdate`,
   status 3), ručně přidané přes `crp_missing`, "oddemolované" panely a panely vrácené z fronty přepočtu (viz bod 3).
2. **Doplnění posledního VAC** (`vai.vai_vac_res`, `res.res_online_ea_output_round`, `periodid = 1`,
   `transptype = 'ALL'`) k panelu.
3. **Správa nespočítaných panelů** (`crp_no_calc_inc`) – panely bez VAC nebo s chybou se vyřadí, ohodnotí se
   složitost jejich routingu (`complexity_index`/`routing_difficulty` z `vai.vai_seg`) a zařadí se do fronty
   přepočtu `oper.oper_vac_queue` (`temp_table = 50820`). Panely, u kterých fronta vrátí výsledek, se v příštím
   běhu vrátí zpět do zpracování; panely s trvalou chybou skončí v `crp_pnl_to_revise`.
4. **Cache pro demolované panely** – z `ids.ids_vac_calc_campaign_cache2` se pro demolované panely (`pnlmotion =
   'D'`) odlogují a smažou záznamy `rvloadid = 100` (do `crp_cache_del_log`).
5. **Průsečíky s trasami** (`crp_intersection`) – prostorový průnik bufferu panelu (`buffer_geom`) s trasami
   respondentů (`crp_routing`, GiST index). Panely bez přímého průsečíku se dohledají přes 20 geometricky
   nejbližších panelů s podobnou hodnotou VAC (`crp_nearest20`) a jejich průsečík se panelu "nasubstituuje"
   (`intersection_type = 'SUBSTITUTE'`).
6. **CCF výpočet** (`crp_ccf_output`, `crp_ts_rots_temp`) – kombinace trasy respondenta, váhy respondenta
   (`crp_resp_weight_input`) a frekvenční distribuce (`crp_freq_trips_vector`) dá `ts_rots` (technická ROTS), z
   poměru k IMS vstupu (`crp_ims_input_inc`) se dopočítá `ts_va`, `ts_freq` a finální VA/VAC vzorcem
   `1 - (1 - ts_va_adj)^ts_freq`.
7. **Zápis do `etl_ccf_rel_pub_inc`** (dynamické `EXECUTE`, `load_id` z `etl_ccf_rel_pub_loads_inc`). Záznamy s
   `NULL`/zápornou VA nebo VAC se přesunou do `crp_pnl_to_revise` a z výstupu se smažou.
8. **Publish do `res.res_release_publish`** – nejdřív se smažou stará data pro dotčené panely (`rvloadid IN (100,
   101)`), pak se vloží nová: `rvloadid = 100` (IDS, `periodid = 1`), `rvloadid = 101` (IMS, `periodid = 200`).
   Souběžně s tím `auto_rel_pub_inc()` na začátku běhu z `crp_unpublish_list` maže i panely, které se přestaly
   kvalifikovat (změna statusu, demolice) – viz krok "Unpublish" v kódu funkce.
9. **Cache dynamické viditelnosti** – na závěr běhu se naplní `crp_spotplans` (spot length/pause z cache) a zavolá
   `f_fill_crp_cache_temp()`, výsledek se zkopíruje zpět do `ids.ids_vac_calc_campaign_cache2`. Komentář v kódu
   označuje tuto část jako "Nové, cache - test režim" – vypadá to na rozpracovanou/testovací funkcionalitu.

`auto_rel_pub_ccf()` dělá analogický, ale plný (ne přírůstkový) běh nad všemi panely a publikuje přímo do
`rel_pub.etl_ccf_rel_pub` (ne do `res.res_release_publish` – ten plní jen `auto_rel_pub_inc()`).

## Cizí schémata a objekty (mimo tento repozitář)

| Schéma | Objekty použité v `rel_pub` | Jak se používá |
|---|---|---|
| `pnl` | `pnl_main`, `pnl_geom`, `pnl_facemid`, `pnl_faces`, `pnl_timeatts`, `mv_pnl_main_maxdate` | Zdroj dat o panelech (geometrie, faces, stavy, demolice). Jen čtení. |
| `vai` | `vai_vac_res`, `vai_seg` | Poslední VAC výpočet k panelu; segmenty trasy pro odhad složitosti routingu. Jen čtení. |
| `res` | `res_online_ea_output_round`, `res_online_ea_output`, `res_period`, `res_release_publish` | Zdroj VAC/ROTS výsledků; `res_release_publish` je hlavní **výstup** (mazání i insert). |
| `topo` | `geometry` (typ), `topo_streets_pli` | PostGIS typ sloupců; `topo_streets_pli` je v `auto_rel_pub_inc()` zakomentovaný (viz níže – BALT ho zatím nepoužívá). |
| `fnc` | `f_params()`, `set_updated_by()`, `set_updated_timestamp()` | `f_params()` dává aktuální navteq verzi; zbytek jsou triggerové procedury na `crp_no_calc_inc`/`crp_pnl_to_revise`. |
| `oper` | `oper_vac_queue` | Fronta pro dopočet VAC nespočítaných panelů (`temp_table = 50820`). Čtení i zápis. |
| `ids` | `ids_vac_calc_campaign_cache2` | Cache dynamické viditelnosti pro digitální panely – čtení, mazání i zápis v `auto_rel_pub_inc()` a `f_fill_crp_cache_temp()`. |
| `code` | `code_pnlsubtype` | Číselník podtypů panelu, JOIN při plnění `crp_ims_input`/`crp_ims_input_inc`. Jen čtení. |
| `vac8s` | `_f_vac_calc_ids_dynvac_fepe()` | Výpočetní funkce dynamické VA pro pohyblivé/digitální panely, volaná z `f_fill_crp_cache_temp()`. |
| `vai_subl` | `vai_subl_route_plan_dynva` | Zdroj dat pro dynamický výpočet VA (`f_fill_crp_cache_temp()`). Jen čtení. |

Schémata `ids`, `code`, `vac8s` a `vai_subl` **nejsou** v seznamu cizích schémat u CRP ZA – buď jde o funkcionalitu
specifickou pro BALT (cache dynamické VA pro digitální panely), nebo o novější rozšíření, které se do CRP ZA teprve
propsalo. Neověřeno, jen zjištěno z tohoto DDL.

## Zjištěné rozdíly oproti CRP ZA

Toto DDL (export z `imsbalt_prod`) se v několika bodech liší od stavu zdokumentovaného pro CRP ZA. Nejde o
vyčerpávající srovnání (k tomu by bylo potřeba mít po ruce i aktuální DDL CRP ZA), jen o to, co bylo ověřeno při
psaní této dokumentace:

- **Žádný `debug_mode`.** V `auto_rel_pub_ccf()` u BALT není žádná zmínka o `debug_mode` – funkce zapisuje rovnou
  do produkčních tabulek `rel_pub.etl_ccf_rel_pub` a `rel_pub.crp_geom_panel_log`. Tabulky `dbg_etl_ccf_rel_pub` /
  `dbg_geom_panel_log` z CRP ZA zde vůbec neexistují.
- **`rvloadid` sedí.** `re_publish_crp()` má komentář i tělo konzistentně na `rvloadid 100, 101` (u CRP ZA byl
  komentář zastaralý a odkazoval na 100/101 vs. tělo na 104/105).
- **`crp_routing_ps` neexistuje** v tomto schématu vůbec (u CRP ZA šlo o nepoužívanou tabulku).
- **Jiná sada nepoužívaných tabulek.** V BALT DDL existují, ale žádná funkce je nečte ani neplní:
  `crp_ims_input2`, `crp_new_mids`, `crp_ims_temp`, `crp_ccf_input`, `crp_ccf_output_inc`, `crp_freq_trips`
  (podrobnosti [`02_datovy_slovnik.md`](02_datovy_slovnik.md)). U CRP ZA byly nepoužívané jen `crp_ims_input2`,
  `crp_new_mids`, `crp_routing_ps`.
- **`code.code_pnlsubtype` join přibyl** v `create_ims_input_ccf()`/`create_ims_input_inc()` (číselník podtypu
  panelu) – u CRP ZA nezdokumentováno.
- **Cache dynamické viditelnosti pro digitální panely** (`ids.ids_vac_calc_campaign_cache2`, `crp_spotplans`,
  `crp_cache_temp`, `crp_cache_log`, `crp_cache_del_log`, funkce `f_fill_crp_cache_temp()`, schémata `vac8s` a
  `vai_subl`) je funkcionalita, která v popisu CRP ZA vůbec není. Autor kódu ji v komentáři sám označuje jako
  "Nové, cache - test režim" – vypadá na rozpracovanou/testovací funkcionalitu, ne definitivní stav.
- **BALT zatím nemá filtraci `linetype`.** V `auto_rel_pub_inc()` (výpočet `complexity_index` pro
  `crp_no_calc_inc`) je zakomentovaný JOIN na `topo.topo_streets_pli` s poznámkou přímo v kódu: *"Přidání JOINu
  pro filtraci linetype (Comment, BALT zatím nemá)"* – tzn. jde o vědomě neúplnou funkcionalitu, ne o
  přehlédnutou chybu.
- **Drobná nekonzistence v `re_publish_crp()`:** `EXCEPTION WHEN others` blok volá `log_msg('ERROR',
  'auto_rel_pub_inc', ...)` místo `'re_publish_crp'` – zjevně zkopírováno z `auto_rel_pub_inc()` a nedopsáno.
  Ovlivňuje jen popisek zdroje v logu, ne funkčnost.

Neopravuj tyto věci automaticky (je to čtecí export) – při jakékoli úpravě DDL/dokumentace na ně jen upozorni a
ověř s uživatelem, jestli jde o záměr nebo chybu (viz [`../CLAUDE.md`](../CLAUDE.md)).
