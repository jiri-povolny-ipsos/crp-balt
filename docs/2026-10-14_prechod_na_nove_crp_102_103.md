# CRP BALT – přechod na nové CRP (rvloadid 102/103), 12.–14. 10. 2026

Vytvořil: Jiří Povolný, vytvořeno dne: 14.10.2026
Tickety: REL-3629 (nová vlna respondentů), REL-3092 (spotplánová cache)

Přehled všech změn dat a funkcí v `imsbalt_prod`, které vedly k novému CRP nad releasem CR 26.1. Nové CRP
publikuje do `res.res_release_publish` pod `rvloadid` **102** (IDS, periodid 1) a **103** (IMS, periodid 200),
ve `vai.vai_release_vac_static_load` jde o release 9 „Continual Release - CR 26.1“. Původní CRP (100/101) se už
neaktualizuje.

Pozor: [`01_proces_crp_balt.md`](01_proces_crp_balt.md) a [`02_datovy_slovnik.md`](02_datovy_slovnik.md) zatím
popisují stav před těmito změnami (DDL export z 21. 9. 2026).

## 1. Trasy vlny 3 (Spring2026)

- Zdroj: `rel3274_balt_trips_wave3.gpkg` (MQ2), načteno do `x_jpovolny.crp_balt_routes`. Celkem 2 878 segmentů
  tras a 1 007 respondentů (Litva 459, mqid 8xxxxx; Lotyšsko 548, mqid 9xxxxx). Časy jsou uložené v zóně
  session `Europe/Belgrade`. Podrobnosti viz [`shrnuti_2026-10-12.md`](shrnuti_2026-10-12.md).
- Frekvence tripu: `answerfreq` je převzatá z MQ2 odpovědi na otázku 30 (CSV `routes_answers`, vazba na segment).
  `trip_freq` se odvozuje stejně jako ve starých vlnách (podle typu dne):

  | answerfreq | wd | we |
  |---|---|---|
  | Everyday | 5 | 2 |
  | 4 to 5 days a week | 4,5 | 2 |
  | 2 to 3 days a week | 2,5 | 2 |
  | Once a week / Less often | 1 | 1 |
  | Every other week | 0,1 | 0,25 |
  | Once a month | 0,045 | 0,125 |
  | bez odpovědi (2 segmenty) → N/A | 0,2 | 0,1 |

- 13. 10. vloženo do `rel_pub.crp_routing` skriptem
  `files/REL-3629/REL-3629_crp_routing_insert_v2.sql`:
  - `wave = 'Spring2026'`, `respondent_id = LAT/LIT || mqid`,
  - `sid` a `pkid` navazují na předchozí maximum (nové `max(sid)` = 28 307),
  - celkem je teď 22 436 tras.
- Záloha: `x_jpovolny.crp_routing_20261012`. Trasy jsou 1:1 shodné se sandboxem `rel_pub_full`.

## 2. Váhy respondentů vlny 3

- Zdroj: Excely NielsenIQ (`files/REL-3629`), řádky `Wave = 3`, sloupce `MQID 2026` a `weight_populationNEW`.
  Načteno do `x_jpovolny.crp_balt_resp_weight`: 1 112 respondentů (LAT 581, LIT 531), včetně 105 bez tras.
- 13. 10. vloženo do `rel_pub.crp_resp_weight_input` skriptem
  `files/REL-3629/REL-3629_crp_resp_weight_insert.sql`. Celkem je teď 7 763 vah, každá trasa má váhu.
  Záloha: `x_jpovolny.crp_resp_weight_input_20261012`.
- Váhy starých vln (1–2) zůstaly beze změny. Excel obsahuje i jejich přepočítané váhy (LV −40 až +8,7 %,
  LT do ±1 %). Rozhodnutí, zda je převzít, je otevřené (kap. 9).

## 3. Full běh CCF ve schématu `rel_pub`

Full postup se přesunul ze sandboxu `rel_pub_full` do `rel_pub`. Skripty jsou v [`../sql/`](../sql):

| Skript | Obsah |
|---|---|
| `2026-10-13_01_create_crp_panel_full.sql` | nová tabulka `rel_pub.crp_panel_full` (kopie struktury `crp_all_panel`) – seznam panelů full běhu, aby full nemazal haldu inc běhu |
| `2026-10-13_02_auto_rel_pub_ccf_rvloadid.sql` | nové verze `auto_rel_pub_ccf` a `create_ims_input_ccf` (staré bez parametrů odstraněny) |
| `2026-10-13_03_run_full_ccf_rvloadid13.sql` | ruční spuštění po krocích + kontroly |

`rel_pub.auto_rel_pub_ccf(p_rvloadid integer, p_periodid integer DEFAULT NULL, p_publish boolean DEFAULT false)`:

- Panely (pid, mid) a vacidy se berou z `res.res_release_publish` pro `p_rvloadid`. Dřív se panely vybíraly
  podle statusu v `pnl` a vacid se bral poslední z `vai_vac_res`.
- `p_periodid` určuje IMS hodnoty pro kalibraci. Když je NULL, vezme se periodid releasu.
- Seznam panelů je v `crp_panel_full`. Práce s `crp_missing` je pryč (patří inc). `crp_no_calc` se na začátku
  vyprázdní.
- Dělení nulou se ošetřuje pomocí `NULLIF` a panely s nulovým rots se vyřadí do `crp_no_calc` (stejně jako v inc).
- `crp_geom_panel_log.desc_text = 'Full load, rvloadid N, periodid M'`.
- Při `p_publish = true` na konci proběhne:
  1. nahrazení celého obsahu 102/103 všemi facemi releasu (102 = periodid 1, 103 = periodid 200),
  2. smazání cache spotplánů 102 (s logem) a její nový výpočet funkcí `vac8s._f_vac_calc_ids_campaign`
     pro spotplány, které v ní byly.

**Běh 13. 10.:** `SELECT rel_pub.auto_rel_pub_ccf(13, 1, true);`

- Release 13 a 14 mají shodné panely i vacidy, liší se jen periodid.
- Výsledek je load 7 v `rel_pub.etl_ccf_rel_pub` (`processed = 0`): 8 444 291 řádků, 7 102 pidů, 8 998 faců
  (digitální panely jen pozice 1), VAC 364,7 mil.
- Panely: 7 094 ORIGINAL, 8 SUBSTITUTE, 0 nespočítaných.
- Publish 102 a 103: 46 700 řádků každý (9 340 faců, 7 102 pidů), řádek po řádku shodný s release 13 a 14.

## 4. Spotplánová cache (`ids.ids_vac_calc_campaign_cache2`)

- Původ: REL-3092 (duben 2026). `vac8s._f_vac_calc_ids_campaign` počítá cache pro trojici
  (`rvloadid`, `spot_length`, `spot_pause`) jen jednou, pokud je prázdná. CRP proto musí cache průběžně udržovat.
  Původní implementace v inc (`f_fill_crp_cache_temp`) počítala správně, ale jen pro 100 a jen face na pozici 1.
- 13. 10. jednorázové naplnění cache 102 skriptem `2026-10-13_04_fill_cache_spotplan_102.sql`:
  34 spotplánů (podle cache 100) × 684 vacidů = 23 256 řádků. Spočítáno oficiální campaign funkcí.
- Cache se udržuje pro **všechny digitální face** (pozice 1 i 2), stejně jako ji počítá IDS.

## 5. Inkrementální běh `auto_rel_pub_inc()` a `f_fill_crp_cache_temp(p_rvloadid)`

Skript `2026-10-13_05_auto_rel_pub_inc_rvloadid102.sql` (vychází z nasazené verze `2026-09-22_fix_...`):

1. Publikace, odpublikování a cache používají 102/103 místo 100/101.
2. Publikují se všechny face, u digitálních panelů i pozice 2. CCF výstup (`etl_ccf_rel_pub_inc`) má dál jen
   pozici 1.
3. Odpublikování minulých případů zahrnuje vše v 102/103, co nesplňuje kritérium procesu (status 3,
   timeattid 0, `ownid = subownid`), dřív jen zdemolované panely. Podmínka `ownid = subownid` je nově i ve
   výběru „oddemolovaných“ panelů.
4. Oprava: u odpublikování panelů se změněným statusem se do `mid` zapisoval pid.
5. Cache spotplánů:
   - celá je na konci za publikací ve vlastním bloku s `EXCEPTION`, takže chyba se zapíše jako `WARNING`
     a nevrátí zpět výpočet ani publikaci,
   - přepočítávají se všechny face zpracovaných digitálních panelů,
   - na konci každého běhu se z cache 102 smažou vacidy, které nejsou v publish 102,
   - zbytečné prázdné řádky a komentáře „test režim“ jsou odstraněné.
6. `f_fill_crp_cache_temp(p_rvloadid)` (dříve bez parametru): rvloadid je parametr a vacidy se berou z publikace
   pro zpracované digitální panely. Vzorec výpočtu je beze změny.

Ověřeno suchým během celé funkce v transakci s rollbackem. Číslo loadu 86 se při tom spotřebovalo, první
skutečný load bude 87.

## 6. Úpravy dat 14. 10.

| Skript | Změna |
|---|---|
| `2026-10-14_01_unpublish_not_live_102_103.sql` | Z 102/103 odpublikováno 882 neživých pidů (835 `ownid <> subownid`, 47 zdemolovaných), 9 870 řádků. Z cache 102 smazáno 7 140 řádků jejich vacidů. Výsledek: 102/103 mají 6 220 pidů a 41 765 řádků, cache 102 má 16 116 řádků (474 vacidů × 34 spotplánů). |
| `2026-10-14_03_crp_all_panel_from_full.sql` | Halda `crp_all_panel` (14 003 midů včetně starých) nahrazena obsahem `crp_panel_full` (7 102 panelů z full běhu). |

## 7. Další funkce a nástroje

- `re_publish_crp()` (`2026-10-14_02_re_publish_crp_rvloadid102.sql`):
  - přepláchne 102/103 místo 100/101,
  - publikuje všechny face a jen živé panely včetně `ownid = subownid`,
  - po publikaci obnoví cache 102 campaign funkcí (asi 3 minuty),
  - je opravený zdroj chyby v logu.

  Ověřeno suchým během.
- Noční report (`tools/night_report/crp_night_report.py`):
  - výchozí `rvloadid` 102/103, nastavitelné v `[NIGHT_REPORT] rvloadid_ids / rvloadid_ims`,
  - kontroly proti `ids.v_panel` jen pro `rvloadid_ids`; dokud ho IDS nemá, vynechají se,
  - nová varování: `WARNING` z `crp_log_table` a nesoulad cache spotplánů s publikací.

## 8. Stav k 14. 10. a další kroky

- Inc je **zastavený**. 102/103 odpovídají release 13 (cutoff 11. 9.) bez neživých panelů.
- Pro IDS je připraveno:
  - CCF load 7 v `etl_ccf_rel_pub` – obsahuje i 882 odpublikovaných pidů, převzít jen pidy z publish 102,
  - publish 102/103,
  - cache spotplánů 102 (pro 103 se cache nevede).

Postup:

1. IDS ručně nalije nový release.
2. Těsně před restartem inc spustit `2026-10-13_06_catchup_crp_missing.sql` (dnes 40 panelů: 13 nových,
   8 s novým midem, 19 s novějším vacidem). Lze spustit opakovaně.
3. Zapnout plánovač inc. Od té chvíle se plní jen 102/103 a `etl_ccf_rel_pub_inc` (load 87+).
4. Ráno zkontrolovat noční report: zhruba 40 zpracovaných panelů a 6 233 pidů v 102.
5. Zrušit schéma `rel_pub_full`, smazat `x_jpovolny.crp_full13_pred`, aktualizovat `01`/`02` dokumentaci a DDL export.

## 9. Otevřené body

- **Váhy starých vln:** převzít přepočítané `weight_populationNEW` pro vlny 1–2 (LV, LT)? Estonsko nové váhy nemá.
- **Původní CRP 100/101** zůstává ve stavu ze 13. 10. Rozhodnout, kdy ho zrušit (i s cache 100, kde je 211
  vacidů navíc).
- **Den v týdnu v CCF výstupu:** trip se rozkopíruje 5× (wd) / 2× (we) se stejným `weekdayname`, kdežto původní
  Access postup přiřazoval dny z `crp_freq_trips_vector`. Podle dohody ponecháno, stejně jako v inc.

## 10. Zálohy

| Tabulka | Obsah |
|---|---|
| `x_jpovolny.crp_routing_20261012` | `crp_routing` před vlnou 3 |
| `x_jpovolny.crp_resp_weight_input_20261012` | `crp_resp_weight_input` před vlnou 3 |
| `x_jpovolny.res_release_publish_unpub_20261014` | odpublikované řádky 102/103 |
| `x_jpovolny.crp_all_panel_20261014` | `crp_all_panel` před nahrazením |
| `rel_pub.crp_cache_del_log` | všechny smazané řádky cache (`deleted_at`) |

Návrat ke každé změně je popsaný v hlavičce příslušného skriptu.
