# CRP BALT – kontext pro AI asistenty

## Co tento repozitář je

Repozitář obsahuje **DDL export databázového schématu `rel_pub`** (PostgreSQL/PostGIS) procesu **CRP BALT
(Continuous Release Publishing)** nad databází **`imsbalt_prod`** – regionální instance pro **Pobaltí (Litva,
Lotyšsko, Estonsko)**. Proces denně přenáší výkony reklamních panelů (vac) ze systému **IMS** do systému **IDS**.
Jde o stejný proces/schéma jako u sesterského repozitáře pro CRP ZA, jen nad jinou regionální databází – při
úpravách nepředpokládej, že drobnosti (viz níže) sedí 1:1 mezi oběma instancemi, ověřuj v tomto DDL. Není zde
aplikační kód, jen SQL – žádný build, žádné testy, žádný package manager.

- `ddl/schema_rel_pub.sql` – kompletní DDL dump schématu `rel_pub` z `imsbalt_prod`: 7 funkcí (plpgsql) + ~50
  tabulek/sekvencí/indexů.
- `ddl/information_schema_tables.csv` – prostý seznam názvů tabulek ve schématu.
- `docs/` – ručně psaná dokumentace procesu (viz [`docs/README.md`](docs/README.md)) – **začni tam**, než se
  pouštět do parsování syrového DDL.

## Jak číst `schema_rel_pub.sql`

Na rozdíl od CRP ZA je tento export z `imsbalt_prod` normálně formátovaný `pg_dump` výstup – **funkce i tabulky
mají tělo rozepsané přes více řádků** (žádný jeden obří řádek). Soubor má ~3600 řádků, běžný `Read`
s offset/limit funguje bez problémů. Pro vyhledání konkrétní funkce/tabulky použij `Grep` na
`^CREATE (FUNCTION|TABLE)` nebo `-- Name:` komentáře (dávají mapu řádků → objektů), pak si danou funkci/tabulku
přečti přímo přes `Read` od jejího řádku. Sed-extrakce do dočasného souboru (postup použitý u CRP ZA) tu není
potřeba.

Neinvestuj čas do opakovaného parsování `schema_rel_pub.sql` pro obecné otázky o procesu – **primárně čerpej z
`docs/01_proces_crp_balt.md` a `docs/02_datovy_slovnik.md`**, do syrového DDL sahej jen pro ověření konkrétního
detailu (přesné jméno sloupce, přesná WHERE podmínka).

## Klíčové objekty (viz `docs/` pro detaily)

- Vstupní funkce dle zadání: `rel_pub.auto_rel_pub_ccf()` (plný/inicializační přepočet).
- Denní inkrementální běh a **jediná funkce, která reálně publikuje** do `res.res_release_publish` a
  `rel_pub.etl_ccf_rel_pub_inc`: `rel_pub.auto_rel_pub_inc()`.
- Výstup, který si čte cílový systém IDS (mimo tento repozitář): `rel_pub.etl_ccf_rel_pub_inc`, `res.res_release_publish`.
- Cizí schémata čtená/zapisovaná jako vstup nebo cache (nejsou v tomto repozitáři): `pnl`, `vai`, `res`, `topo`,
  `fnc`, `oper`, `code`, `ids`, `vac8s`, `vai_subl` – poslední čtyři nejsou v CRP ZA CLAUDE.md zmíněné, viz
  `docs/01_proces_crp_balt.md` pro detail (mj. `ids.ids_vac_calc_campaign_cache2`, cache dynamické VA pro
  digitální panely).

## Rozdíly oproti CRP ZA (ověřeno v tomto DDL exportu)

Tento repozitář vznikl podle vzoru sesterského repozitáře pro CRP ZA, ale popisuje **výhradně CRP BALT**. Proces a
struktura schématu jsou stejné, konkrétní stav DDL se ale liší – neplatí tedy automaticky nic, co je zdokumentované
pro CRP ZA (např. jeho sekce o `debug_mode` nebo nesouladu `rvloadid`). Podrobná analýza je v `docs/`; zde jen
připomínka, ať se nepřenášejí závěry z CRP ZA bez ověření.

## Jazyk a konvence

- Komentáře v SQL i dokumentace v `docs/` jsou **česky** – drž se toho i v nové dokumentaci/komunikaci k tomuto
  repozitáři, pokud uživatel neřekne jinak.
- V plpgsql kódu je konzistentní vzor: `RAISE NOTICE` pro průběžný debug log, `PERFORM rel_pub.log_msg(...)` pro
  perzistentní log (INFO na konci úspěšného běhu, ERROR v `EXCEPTION WHEN others`), `TRUNCATE` + `INSERT` pro
  staging tabulky. Drž se tohoto stylu, pokud budeš navrhovat úpravy nebo nové funkce do stejného schématu.

## Aktualizace dokumentace

Pokud se `ddl/schema_rel_pub.sql` změní (nový export z produkční DB), dokumenty v `docs/` je potřeba ručně projít a
aktualizovat – nejsou generované automaticky ze schématu.

## Databáze
- Pracujeme s PostgreSQL 9.3.10.
- Nepoužívej novější SQL/PL-pgSQL příkazy, které tato verze nepodporuje 
  (např. INCLUDE v indexech, make_interval, ADD COLUMN IF NOT EXISTS).
- Do nových DDL přidávej do poznámky na úvod Vytvořil: Jiří Povolný a Vytvořeno dne: <aktuální datum>. Pokud chybí, přidej.
- Do updatovaných DDL přidej/aktualizuj Změnil: Jiří Povolný, Změněno dne:  <aktuální datum> a popis změny.

## Komunikace
- Pokud máš doplňující/upřesňující otázky, ptej se jednu po druhé, ne všechny najednou.
- Tykáme si.
