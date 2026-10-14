# Spotplánová funkce `ids.f_spotplan_cache_update` – popis implementace a přenos na BALT

Vytvořil: Jiří Povolný, vytvořeno dne: 13.10.2026
Zdroj: REL-3552 (CRP SE, soubory v `D:\_WI\_WI_2026\03\REL-3552-RELSE - CRP - Spotplánová Funkce`)
Cíl: REL-3092 (CRP BALT, databáze `imsbalt_prod`)
Databáze: PostgreSQL 9.3.10

## 1. K čemu funkce slouží

Funkce průběžně aktualizuje cache spotplánových hodnot VAC a ROTS v tabulce
`ids.ids_vac_calc_campaign_cache2`. Do cache doplní nové a vyměněné vacidy digitálních panelů
a odstraní vacidy, které už nejsou publikované. Cache pro celé `rvloadid` se nemaže ani nepočítá znovu.

Cache čtou IDS procesy (`vac8s._f_vac_calc_ids_campaign`). Ty fungují systémem **všechno nebo nic**
pro trojici (`rvloadid`, `spot_length`, `spot_pause`): pokud v cache existuje aspoň jeden řádek,
vrátí cache a nic nedopočítají. Panel, který přibude do publikace později, proto v již spočítaném
spotplánu chybí. Tento nedostatek funkce řeší.

## 2. Rozhraní

```sql
ids.f_spotplan_cache_update(in_rvloadid integer, in_vacids bigint[] DEFAULT NULL) RETURNS integer
```

| Parametr | Význam |
|---|---|
| `in_rvloadid` | IDS `rvloadid`, pro které se cache aktualizuje. SE: 1000 (EA), 1002 (IA). **BALT: 100** (jen EA). |
| `in_vacids` | `NULL` = automatický režim. Pole vacidů = ruční režim jen pro zadané vacidy. |

Vrací počet řádků vložených do cache. Funkce je `SECURITY INVOKER`, `VOLATILE`.

## 3. Algoritmus

1. **Lock:** (jen BALT verze) `pg_advisory_xact_lock` na `rvloadid`. Souběžné volání počká.
2. **Prostředí:** podle `vai.vai_release_vac_static_load.pnlvaienv` se zvolí EA (`vac8s`,
   `res.res_online_ea_output`). SE navíc umí IA (`vacia8s`, `res.res_online_ia_output`).
   BALT IA nemá, jiné prostředí než EA skončí výjimkou.
3. **Metoda výpočtu:** `dynva_method` ve `vac8s.f_params()` musí být `FEPE`. Jinak výjimka
   `method = ... not implemented`, stejně jako v campaign funkci.
4. **Výběr vacidů**
   - Automatický režim: smaže z cache řádky, jejichž vacid už není v `res.res_release_publish`
     (výměna vacidu). Pak najde vacidy digitálních panelů z publish bez řádku v cache.
   - Ruční režim: smaže z cache zadané vacidy a spočítá je znovu.
   - Pokud není co počítat, vrátí 0.
5. **Spotplány:** všechny kombinace (`spot_length`, `spot_pause`), které jsou v cache pro daný `rvloadid`.
6. **Výpočet:** pro každý spotplán jeden `INSERT ... SELECT` pro všechny vybrané vacidy.
7. Vše běží v transakci volajícího. IDS procesy nikdy neuvidí cache v rozpracovaném stavu.

### Vzorec

Zdroj: `vai_subl.vai_subl_route_plan_dynva` (trasy) → `vai.vai_vac_res` (`rsid` → `vacid`, `faceid`) →
`pnl.pnl_facemid` (`facepid`) + perioda z `res.res_online_ea_output` → `res.res_period` (`period_hours`).

```
dynva = vac8s._f_vac_calc_ids_dynvac_fepe(transptype, pnlmotion, route_kcd, route_va,
                                          spot_length, spot_length + spot_pause, daf_angle)
vac   = sum(dynva * fa_calc)               rots = round(sum(fa_calc), 0)
```

Agregace po vacidu. Podmínky: `pnlmotion = <digitální>`, `route_va > 0`, `fa_calc > 0`.
Do cache se zapisují: `rvloadid`, `faceid`, `facepid`, `spot_length`, `spot_pause`, `vacid`,
`vac`, `rots`, `vac_orig` (= `vac`), `rots_orig` (= `rots`), `group_def` (`NULL`), `period_hours`.

### Proč funkce nevolá campaign funkci

Campaign funkce počítá celé `rvloadid`, nemá parametr pro výběr vacidů a při neprázdné cache nic nepočítá.
Funkce proto obsahuje vlastní kopii výpočtu omezenou na vybrané vacidy.

## 4. Chování a omezení

- **Jen digitální panely.** Panely ostatních typů se do cache nepočítají.
- **Jen existující spotplány.** Nový spotplán, který v cache ještě není, dopočte campaign funkce při prvním
  volání. Pokud je cache pro `rvloadid` prázdná, funkce nevloží nic.
- **Vacid s `rsid = 0`** nemá trasy, takže se nevloží nic. V automatickém režimu se hodnotí jako chybějící
  při každém volání.
- **Cache nemá unikátní index.** Duplicitám brání jen transakční logika (a na BALT advisory lock).
- **Údržba:** výpočet existuje na třech místech (campaign funkce `vac8s`, `vacia8s` a tato funkce).
  Změna vzorce se musí promítnout do všech a znovu ověřit (skript 05, sekce 1).

## 5. Stav na SE

| Soubor | Obsah |
|---|---|
| `f_spotplan_cache_update.sql` | funkce ve schématu `ids` |
| `f_spotplan_cache_update_ids_vrf.sql` | totožná funkce v `ids_vrf` (stejná cache) |
| `grants_f_spotplan_cache_update.sql` | práva pro `vaise_relpub_role` |
| `027_fn_auto_rel_pub_part5b.sql` | volání `ids.f_spotplan_cache_update(1000)` a `(1002)` na konci publikace, každé ve vlastním bloku `BEGIN ... EXCEPTION` |
| `kontrolni_skripty.sql` | kontrolní SELECTy |

Na SE se funkce volá z `rel_pub.fn_auto_rel_pub_part5b` až po všech INSERTech do `res.res_release_publish`.
Každé volání je ve vnořeném bloku, aby chyba cache nevrátila zpět publikaci. Chyba pro 1000 nezabrání
volání pro 1002.

## 6. Rozdíly BALT × SE

| Oblast | SE (`imsse_prod`) | BALT (`imsbalt_prod`) |
|---|---|---|
| IDS `rvloadid` | 1000 (EA), 1002 (IA); IMS 1001, 1003 | **100** (IDS), 101 (IMS) – cache se plní jen pro 100 |
| Prostředí | EA + IA (`vac8s`, `vacia8s`) | jen EA (`vac8s`) |
| Digitální panel `pnlmotion` | `'G'` | **`'D'`** (podle `f_fill_crp_cache_temp` a campaign funkce na BALT; **ověřit** skriptem 01, sekce 4) |
| Místo volání | `fn_auto_rel_pub_part5b` | `auto_rel_pub_inc()` (jediná funkce, která na BALT publikuje) |
| Spouštění | Python orchestrátor | `psql` pod uživatelem `postgres` (`crp_inc.bat`) – GRANTy nejsou nutné |
| Existující řešení | žádné, funkce je nová | `rel_pub.f_fill_crp_cache_temp()` + `crp_spotplans` + `crp_cache_temp` ("test režim") |
| Varianta `ids_vrf` | ano | nepřenáší se |
| Souběh | bez zámku | advisory lock |

### Co nahrazuje

BALT dnes na konci `auto_rel_pub_inc()` plní cache jinak: z `crp_ims_input_inc` bere vacidy změněných panelů,
pro každý vacid a každý spotplán vloží jeden řádek (kurzor × kurzor) a výsledek kopíruje z `crp_cache_temp`
do cache. Nová funkce dělá totéž jedním `INSERT ... SELECT` na spotplán a navíc:

- řídí se publikací (`res.res_release_publish`), ne seznamem změněných panelů,
- maže zastaralé vacidy,
- opraví i vacidy, které v cache chyběly z dřívějška.

Blok před výpočtem tras, který loguje do `crp_cache_del_log` a maže cache panelů `pnlmotion = 'D'`
s `rvloadid = 100`, **zůstává**. Nová funkce pak smazané vacidy v publish najde a dopočte.

## 7. Postup nasazení na BALT

Soubory v této složce:

| # | Soubor | Účel |
|---|---|---|
| 01 | `01_kontrola_predpokladu_balt.sql` | jen SELECT: existence objektů, `pnlmotion`, `rvloadid`, metoda, mapování vacid ↔ rsid |
| 02 | `02_f_spotplan_cache_update_balt.sql` | vytvoření funkce `ids.f_spotplan_cache_update` |
| 03 | `03_grants_f_spotplan_cache_update_balt.sql` | práva (jen pokud funkci volá jiná role než `postgres`) |
| 04 | `04_vzor_integrace_auto_rel_pub_inc_balt.sql` | blok kódu k vložení do `rel_pub.auto_rel_pub_inc()` (**není spustitelný**) |
| 05 | `05_kontrolni_skripty_po_nasazeni_balt.sql` | ověření výpočtu proti cache, zkušební běh s `ROLLBACK` |

Kroky:

1. Spusť **01** a vyhodnoť:
   - všechny objekty existují (sekce 1),
   - `rvloadid` 100 má `pnlvaienv = 'EA'` a `dynva_method = 'FEPE'` (sekce 3),
   - **`pnlmotion`** digitálních panelů, které cache pokrývá, je `'D'` (sekce 4). Jinak uprav proměnnou
     `sPnlmotion` v 02 (a `'D'` v dotazech skriptů 01 a 05),
   - kontroly mapování v sekci 6 vrátily 0,
   - cache pro `rvloadid` 100 není prázdná (sekce 5). Prázdnou cache nejdřív naplň campaign funkcí.
2. Spusť **02** (vytvoří funkci).
3. Pokud funkci volá jiná role než `postgres`, doplň roli do **03** a spusť.
4. Spusť **05**, sekci 1 (porovnání s cache, všude `ok = true`) a sekci 2 (zkušební běh v transakci s `ROLLBACK`).
5. Ostrý první běh: `SELECT ids.f_spotplan_cache_update(100);` a znovu sekce 2 (druhé volání vrátí 0).
6. Až potom napoj funkci do `auto_rel_pub_inc()` podle **04**. Vkládej do aktuální produkční verze funkce
   (v repozitáři `crp_balt` je to `sql/2026-09-22_fix_auto_rel_pub_inc.sql`, ověř, že je aktuální).
   Zdokumentuj změnu v hlavičce (Změnil / Změněno dne / Popis změny).
7. Po prvním denním běhu zkontroluj `rel_pub.crp_log_table` (05, sekce 4) a `crp_output.log`
   (řádek `spotplan cache 100: N radku`).

Vrácení změny: `DROP FUNCTION ids.f_spotplan_cache_update(integer, bigint[]);` a vrátit původní blok
v `auto_rel_pub_inc()`. Cache zůstává platná, funkce do ní jen přidává nebo z ní maže záznamy.

## 8. Poznámky k verzi PostgreSQL 9.3

Skripty nepoužívají `to_regclass`, `ADD COLUMN IF NOT EXISTS`, `FILTER` ani jinou novější syntaxi.
`pg_advisory_xact_lock(int, int)` a `DO` bloky jsou v 9.3 k dispozici.

## 9. Otevřené body / rizika

1. **`pnlmotion` na BALT.** Kód na BALT používá `'D'`, ale komentář v campaign funkci mluví o `G`
   a její "bezvacidový chod" používá `'G'`. Rozhodne skript 01, sekce 4.
2. **Audit.** `f_fill_crp_cache_temp()` plní také `rel_pub.crp_cache_log` a `rel_pub.crp_ims_input_inc_log`
   (příznak `cache_ok`). Po nahrazení se tyto tabulky přestanou plnit. Je třeba rozhodnout, zda audit zachovat.
3. **Prázdná cache.** Funkce nevytváří nové spotplány. Pro nový `rvloadid` nebo prázdnou cache musí
   nejdřív proběhnout plný výpočet campaign funkcí.
4. **Agregace.** BALT campaign funkce agreguje po `rsid`, nová funkce po vacidu. Výsledky jsou shodné
   při mapování 1:1 (kontrola ve skriptu 01, sekce 6).
5. **Neověřeno na BALT DB.** Skripty jsem připravil podle DDL exportu a zdrojů v `D:\_WI`. K BALT DB
   jsem se nepřipojoval a funkci jsem nespustil. Před nasazením proběhne kontrola 01 a ověření 05.
