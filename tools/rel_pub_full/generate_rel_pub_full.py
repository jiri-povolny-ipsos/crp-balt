"""
Generátor skriptu pro vytvoření schématu rel_pub_full z DDL exportu rel_pub.

Vytvořil:      Jiří Povolný
Vytvořeno dne: 12.10.2026

Vstup:  ddl/schema_rel_pub.sql (pg_dump schématu rel_pub z imsbalt_prod)
Výstup: files/rel_pub_full/01_create_rel_pub_full.sql

Pravidla:
  - všechny tabulky, sekvence, indexy, defaulty a triggery se přenesou 1:1 (název schématu rel_pub -> rel_pub_full),
  - z funkcí se přenesou jen ty, které potřebuje full run (FUNCTIONS_FULL). Ostatní (auto_rel_pub_inc,
    create_ims_input_inc, f_fill_crp_cache_temp, re_publish_crp) zapisují do sdílených objektů mimo schéma
    (res.res_release_publish, oper.oper_vac_queue, ids.ids_vac_calc_campaign_cache2), proto se do rel_pub_full nedávají,
  - záměna schématu se dělá jen na celé slovo rel_pub (etl_ccf_rel_pub, auto_rel_pub_ccf apod. zůstávají).
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "ddl" / "schema_rel_pub.sql"
DST = ROOT / "files" / "rel_pub_full" / "01_create_rel_pub_full.sql"

TARGET = "rel_pub_full"
FUNCTIONS_FULL = {"auto_rel_pub_ccf", "create_ims_input_ccf", "log_msg"}

# Zbytky z rel_pub_dbg, které ve schématu rel_pub_full původně byly.
OLD_FUNCTIONS = [
    "auto_rel_pub_dbg_ccf()",
    "auto_rel_pub_dbg_inc()",
    "create_ims_input_ccf()",
    "create_ims_input_inc()",
    "dbg_upd_trouble_pnl()",
    "dbg_upd_za_tables()",
    "dbg_upd_za_tables_vrf()",
    "log_msg(character varying, character varying, character varying)",
    "re_publish_crp()",
]
OLD_SEQUENCES = ["load_id_seq", "load_id_inc_seq"]

HEADER = """/*
 * Vytvoření schématu rel_pub_full (full instance CRP BALT) z DDL schématu rel_pub
 *
 * Vytvořil:      Jiří Povolný
 * Vytvořeno dne: 12.10.2026
 *
 * Skript je generovaný (tools/rel_pub_full/generate_rel_pub_full.py), neupravuj ho ručně.
 *
 * Obsah:
 *   - odstranění původních zbytků ze schématu rel_pub_full (funkce *_dbg_* z rel_pub_dbg, dvě sekvence),
 *     jejich záloha je v files/rel_pub_full/backup_puvodni_funkce_rel_pub_full_20261012.sql
 *   - všechny tabulky, sekvence, indexy, defaulty a triggery jako v rel_pub (bez dat, názvy beze změny)
 *   - funkce auto_rel_pub_ccf, create_ims_input_ccf, log_msg (odkazy na schéma přepsané na rel_pub_full)
 *
 * Funkce auto_rel_pub_inc, create_ims_input_inc, f_fill_crp_cache_temp a re_publish_crp se záměrně nepřenášejí,
 * protože zapisují do sdílených objektů (res.res_release_publish, oper.oper_vac_queue, ids cache).
 */

BEGIN;

"""


def swap_schema(text: str) -> str:
    return re.sub(r"\brel_pub\b", TARGET, text)


def main() -> int:
    text = SRC.read_text(encoding="utf-8")
    parts = re.split(r"(?m)^--\n(?=-- Name: )", text)
    header, blocks = parts[0], parts[1:]

    out = [HEADER]
    for sig in OLD_FUNCTIONS:
        out.append(f"DROP FUNCTION IF EXISTS {TARGET}.{sig};\n")
    for seq in OLD_SEQUENCES:
        out.append(f"DROP SEQUENCE IF EXISTS {TARGET}.{seq};\n")
    out.append(f"\nSET LOCAL search_path = {TARGET}, pg_catalog;\n")
    out.append("SET LOCAL default_tablespace = '';\nSET LOCAL default_with_oids = false;\n\n")

    kept_functions = set()
    n_objects = 0
    for block in blocks:
        m = re.match(r"-- Name: (.+?); Type: ([A-Z ]+?);", block)
        if not m:
            raise SystemExit("Nerozpoznaná hlavička bloku: " + block[:80])
        name, typ = m.group(1), m.group(2)
        if typ == "SCHEMA":
            continue
        if typ == "FUNCTION":
            fname = name.split("(")[0]
            if fname not in FUNCTIONS_FULL:
                continue
            kept_functions.add(fname)
        # závěrečný komentář pg_dump nepřenášet; SET z konce bloku funkce se řeší výše
        block = re.sub(r"(?s)\n--\n-- PostgreSQL database dump complete\n--\n\s*$", "\n", block)
        block = re.sub(r"(?m)^SET default_(tablespace|with_oids) = .*\n\n?", "", block)
        out.append("--\n" + swap_schema(block).rstrip("\n") + "\n\n")
        n_objects += 1

    missing = FUNCTIONS_FULL - kept_functions
    if missing:
        raise SystemExit("Ve zdrojovém DDL chybí funkce: " + ", ".join(sorted(missing)))

    out.append("COMMIT;\n")
    DST.parent.mkdir(parents=True, exist_ok=True)
    DST.write_text("".join(out), encoding="utf-8", newline="\n")
    print(f"Zapsáno {DST} ({n_objects} objektů, funkce: {', '.join(sorted(kept_functions))})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
