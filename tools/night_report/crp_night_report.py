"""Přehled o nočním běhu CRP BALT a jeho odeslání do Teams.

Vytvořil: Jiří Povolný
Vytvořeno dne: 22.9.2026
Změnil: Jiří Povolný
Změněno dne: 14.10.2026
Popis změny: nové CRP - publish a kontroly IDS pro rvloadid 102 (IDS) a 103 (IMS) místo 100/101
    (nastavitelné v [NIGHT_REPORT] rvloadid_ids / rvloadid_ims). Kontroly proti ids.v_panel jen pro
    rvloadid_ids, dokud ho IDS nemá, vynechají se. Varování (WARNING) z crp_log_table a kontrola
    souladu cache spotplánů ids.ids_vac_calc_campaign_cache2 s publish.

Čte stav z databáze imsbalt_prod (výhradně read-only), sestaví přehled o posledním
běhu rel_pub.auto_rel_pub_inc() a pošle ho do Teams přes webhook z config.ini.

Určeno ke spuštění scheduled taskem po nočním běhu (ten jede v 01:45), typicky ve 4:00.

Použití:
    python crp_night_report.py                 # dnešní běh, odešle do Teams
    python crp_night_report.py --dry-run       # jen vypíše, nic neodesílá
    python crp_night_report.py --date 2026-09-22
    python crp_night_report.py --config jiny\\config.ini

Nástroj si drží všechno u sebe v tools/night_report/:
    config.ini          přihlašovací údaje a nastavení (v .gitignore)
    config.ini.example  zaslepený vzor, ten patří do repozitáře
    logs/               průběžný log
    exports/            CSV se seznamy panelů

Návratové kódy (pro scheduled task):
    0 = běh proběhl v pořádku
    1 = běh proběhl, ale jsou varování
    2 = běh selhal, neproběhl vůbec, nebo selhala sama kontrola
"""

import argparse
import configparser
import csv
import datetime as dt
import json
import os
import sys
import traceback
import urllib.request

import psycopg2

ADRESAR_NASTROJE = os.path.dirname(os.path.abspath(__file__))
VYCHOZI_CONFIG = os.path.join(ADRESAR_NASTROJE, 'config.ini')
VYCHOZI_LOG = os.path.join(ADRESAR_NASTROJE, 'logs', 'crp_night_report.log')
VYCHOZI_EXPORT = os.path.join(ADRESAR_NASTROJE, 'exports')

STAV_OK = 'OK'
STAV_BEZ_PANELU = 'BEZ PANELU'
STAV_CHYBA = 'CHYBA'
STAV_NEPROBEHL = 'NEPROBEHL'

# rvloadid nového CRP v res.res_release_publish (přepisuje [NIGHT_REPORT] v config.ini)
VYCHOZI_RV_IDS = 102
VYCHOZI_RV_IMS = 103


# ---------------------------------------------------------------------------
# Databáze
# ---------------------------------------------------------------------------

def clean_password(pwd):
    """Odstrani 5 nahodnych znaku za 3. znakem hesla.

    Prevzato z crp_se/crp_engine.py, aby oba projekty pouzivaly stejny format.
    """
    if len(pwd) < 8:
        raise ValueError('Obfuskovane heslo musi mit aspon 8 znaku '
                         '(3 znaky hesla + 5 vlozenych nahodnych znaku).')
    return pwd[:3] + pwd[8:]


class Db:
    """Read-only přístup do imsbalt_prod. Zapisovat se odsud nesmí."""

    def __init__(self, cfg):
        db = cfg['DATABASE']
        self.conn = psycopg2.connect(
            host=db['host'], port=db['port'], dbname=db['database'],
            user=db['user'], password=clean_password(db['password']),
        )
        self.conn.set_session(readonly=True, autocommit=True)
        self.cur = self.conn.cursor()
        self.cur.execute("SET statement_timeout = '300s';")

    def radky(self, sql, params=None):
        self.cur.execute(sql, params or ())
        return self.cur.fetchall()

    def tabulka(self, sql, params=None):
        """Vrátí (nazvy_sloupcu, radky) - pro export do CSV."""
        self.cur.execute(sql, params or ())
        return [s[0] for s in self.cur.description], self.cur.fetchall()

    def radek(self, sql, params=None):
        vysledek = self.radky(sql, params)
        return vysledek[0] if vysledek else None

    def hodnota(self, sql, params=None, vychozi=None):
        radek = self.radek(sql, params)
        return vychozi if radek is None or radek[0] is None else radek[0]

    def zavri(self):
        self.cur.close()
        self.conn.close()


# ---------------------------------------------------------------------------
# Sběr dat
# ---------------------------------------------------------------------------

def seber_data(db, den, rv_ids=VYCHOZI_RV_IDS, rv_ims=VYCHOZI_RV_IMS):
    """Vrátí slovník s přehledem o běhu za daný den."""
    zitra = den + dt.timedelta(days=1)
    d = {'den': den, 'rv_ids': rv_ids, 'rv_ims': rv_ims}

    # --- Záznamy z logu za daný den -----------------------------------------
    d['log'] = db.radky(
        '''SELECT "timestamp", type, source, desc_text
           FROM rel_pub.crp_log_table
           WHERE "timestamp" >= %s AND "timestamp" < %s
           ORDER BY row_id''',
        (den, zitra))

    chyby = [z for z in d['log'] if z[1] == 'ERROR']
    d['log_varovani'] = [z[3] for z in d['log'] if z[1] == 'WARNING']
    hlavni = [z for z in d['log'] if z[2] in ('auto_rel_pub_inc', 'fce:auto_rel_pub_inc')]

    if chyby:
        d['stav'] = STAV_CHYBA
        d['chyby'] = [z[3] for z in chyby]
    elif not hlavni:
        d['stav'] = STAV_NEPROBEHL
        d['chyby'] = []
    elif any('Load did not run' in (z[3] or '') for z in hlavni):
        d['stav'] = STAV_BEZ_PANELU
        d['chyby'] = []
    else:
        d['stav'] = STAV_OK
        d['chyby'] = []

    # --- Load ----------------------------------------------------------------
    d['load'] = db.radek(
        '''SELECT load_id, "timestamp", processed
           FROM rel_pub.etl_ccf_rel_pub_loads_inc
           WHERE "timestamp" >= %s AND "timestamp" < %s
           ORDER BY load_id DESC LIMIT 1''',
        (den, zitra))

    if d['load']:
        load_id = d['load'][0]
        d['vystup'] = db.radek(
            '''SELECT COUNT(*), COUNT(DISTINCT pid), COUNT(DISTINCT faceid),
                      ROUND(SUM(vac)::numeric, 0)
               FROM rel_pub.etl_ccf_rel_pub_inc WHERE load_id = %s''',
            (load_id,))
        d['kontrolni_soucet'] = db.radek(
            '''SELECT COUNT(*), ROUND(SUM(sum_of_ts_rots)::numeric, 0),
                      ROUND(SUM(sum_of_ts_vac)::numeric, 0)
               FROM rel_pub.crp_control_sum WHERE load_id = %s''',
            (load_id,))
    else:
        d['vystup'] = None
        d['kontrolni_soucet'] = None

    # --- Co dnes přibylo k revizi -------------------------------------------
    # load_id IS NULL = panel s nulovým rots (blok přidaný 22.9.2026),
    # load_id vyplněné = NULL/záporný výsledek na konci běhu.
    d['revize_dnes'] = db.radek(
        '''SELECT SUM(CASE WHEN load_id IS NULL THEN 1 ELSE 0 END),
                  SUM(CASE WHEN load_id IS NOT NULL THEN 1 ELSE 0 END),
                  COUNT(*)
           FROM rel_pub.crp_pnl_to_revise
           WHERE created >= %s AND created < %s''',
        (den, zitra))

    # --- Stav tabulek --------------------------------------------------------
    d['tabulky'] = db.radek(
        '''SELECT (SELECT COUNT(*) FROM rel_pub.crp_no_calc_inc),
                  (SELECT COUNT(*) FROM rel_pub.crp_pnl_to_revise),
                  (SELECT COUNT(*) FROM rel_pub.crp_missing),
                  (SELECT COUNT(*) FROM rel_pub.crp_bad_navteq),
                  (SELECT COUNT(*) FROM rel_pub.crp_cache_temp)''')

    # --- Publish -------------------------------------------------------------
    d['publish'] = db.radky(
        '''SELECT rvloadid, COUNT(*), COUNT(DISTINCT pid)
           FROM res.res_release_publish WHERE rvloadid IN (%s, %s)
           GROUP BY rvloadid ORDER BY rvloadid''', (rv_ids, rv_ims))

    # --- Cache spotplánů -----------------------------------------------------
    # Digitální vacidy publish bez cache / vacidy v cache, které už nejsou v publish.
    d['cache'] = db.radek(
        '''SELECT
             (SELECT COUNT(*) FROM ids.ids_vac_calc_campaign_cache2 WHERE rvloadid = %s),
             (SELECT COUNT(DISTINCT p.vacid) FROM res.res_release_publish p
                JOIN pnl.pnl_main m ON m.mid = p.mid
               WHERE p.rvloadid = %s AND m.pnlmotion = 'D'
                 AND NOT EXISTS (SELECT 1 FROM ids.ids_vac_calc_campaign_cache2 c
                                 WHERE c.rvloadid = p.rvloadid AND c.vacid = p.vacid)),
             (SELECT COUNT(DISTINCT c.vacid) FROM ids.ids_vac_calc_campaign_cache2 c
               WHERE c.rvloadid = %s
                 AND NOT EXISTS (SELECT 1 FROM res.res_release_publish p
                                 WHERE p.rvloadid = c.rvloadid AND p.vacid = c.vacid))''',
        (rv_ids, rv_ids, rv_ids))

    # --- Konzistence IDS vs IMS ---------------------------------------------
    d['navteqver'] = db.hodnota('SELECT navteq_version FROM fnc.f_params()')

    # Kontroly proti ids.v_panel jen pro rvloadid, které IDS opravdu má. Dokud IDS
    # nové CRP nečte, kontrola by hlásila falešně všechny panely jako chybějící.
    d['ids_rvloadidy'] = [r[0] for r in db.radky(
        'SELECT DISTINCT rvloadid FROM ids.v_panel ORDER BY 1')]
    d['ids_kontrola'] = rv_ids in d['ids_rvloadidy']

    if d['ids_kontrola']:
        d['chybi_v_ids'] = db.hodnota(
            '''SELECT COUNT(*) FROM (
                 SELECT DISTINCT m.pid FROM pnl.mv_pnl_main_maxdate m
                 WHERE m.status = 3 AND m.timeattid = 0 AND m.ownid = m.subownid
                   AND NOT EXISTS (SELECT 1 FROM ids.v_panel v
                                   WHERE v.rvloadid = %s AND v.pid = m.pid)
               ) x''', (rv_ids,), vychozi=0)

        d['stary_navteqver'] = db.hodnota(
            '''SELECT COUNT(DISTINCT v.pid)
               FROM ids.v_panel v JOIN pnl.pnl_main p ON p.mid = v.mid
               WHERE v.rvloadid = %s AND p.navteqver <> %s''',
            (rv_ids, d['navteqver']), vychozi=0)

        d['starsi_modifikace'] = db.hodnota(
            '''SELECT COUNT(DISTINCT v.pid) FROM ids.v_panel v
               JOIN pnl.mv_pnl_main_maxdate m
                 ON m.pid = v.pid AND m.status = 3 AND m.timeattid = 0
               WHERE v.rvloadid = %s AND v.mid <> m.mid''', (rv_ids,), vychozi=0)

        d['osirele_facepidy'] = db.hodnota(
            '''SELECT COUNT(DISTINCT facepid) FROM res.res_release_publish
               WHERE rvloadid = %s
                 AND facepid NOT IN (SELECT pnlkey::bigint FROM ids.v_panel
                                     WHERE rvloadid = %s)''', (rv_ids, rv_ids), vychozi=0)
    else:
        d['chybi_v_ids'] = d['stary_navteqver'] = None
        d['starsi_modifikace'] = d['osirele_facepidy'] = None

    # --- Statistiky počtu panelů --------------------------------------------
    # Zpracováno = panely, které běh opravdu prohnal výpočtem (z crp_geom_panel_log
    # daného loadu). Ostatní tři jsou aktuální obsah pracovních tabulek.
    zpracovano = 0
    if d['load']:
        zpracovano = db.hodnota(
            '''SELECT COUNT(DISTINCT pid) FROM rel_pub.crp_geom_panel_log
               WHERE load_id = %s''', (d['load'][0],), vychozi=0)

    d['statistiky'] = [
        ('Zpracováno v běhu', zpracovano),
        ('Špatná navteq verze (crp_bad_navteq)', d['tabulky'][3]),
        ('Nespočítané (crp_no_calc_inc)', d['tabulky'][0]),
        ('K revizi (crp_pnl_to_revise)', d['tabulky'][1]),
    ]

    # --- Varování ------------------------------------------------------------
    varovani = []
    if d['stav'] == STAV_NEPROBEHL:
        varovani.append('Za dnešek není v crp_log_table žádný záznam - běh se zřejmě nespustil.')
    if d['chyby']:
        for chyba in d['chyby']:
            varovani.append('Běh skončil chybou: ' + chyba)
    for v in d['log_varovani']:
        varovani.append('Varování v logu běhu: ' + (v or ''))
    if d['cache'] and (d['cache'][1] or d['cache'][2]):
        varovani.append('Cache spotplánů %d nesouhlasí s publish: %d digitálních vacidů bez cache, '
                        '%d vacidů v cache navíc.' % (d['rv_ids'], d['cache'][1], d['cache'][2]))
    if d['revize_dnes'] and d['revize_dnes'][0]:
        varovani.append(
            'Panelů s nulovým rots odloženo k revizi: %d.' % d['revize_dnes'][0])
    if d['chybi_v_ids']:
        varovani.append('Živých panelů chybí v IDS: %d.' % d['chybi_v_ids'])
    if d['stary_navteqver']:
        varovani.append(
            'Panelů v IDS se zastaralým navteqver (aktuální %s): %d.'
            % (d['navteqver'], d['stary_navteqver']))
    if d['starsi_modifikace']:
        varovani.append('Panelů, kde má IDS starší modifikaci: %d.' % d['starsi_modifikace'])
    if d['osirele_facepidy']:
        varovani.append('Publikovaných facepidů chybějících v IDS: %d.' % d['osirele_facepidy'])
    if d['tabulky'] and d['tabulky'][3]:
        varovani.append('Panelů se špatnou navteq verzí v crp_bad_navteq: %d.' % d['tabulky'][3])
    if d['load'] and d['load'][2] == 0:
        varovani.append('Load %d zatím nemá příznak processed - IDS si ho možná nepřevzal.'
                        % d['load'][0])
    d['varovani'] = varovani

    return d


# ---------------------------------------------------------------------------
# Export seznamů panelů do CSV
# ---------------------------------------------------------------------------

def seber_seznamy(db, d):
    """Vrátí {nazev: (sloupce, radky)} se seznamy panelů k exportu."""
    seznamy = {}

    if d['load']:
        seznamy['zpracovane'] = db.tabulka(
            '''SELECT g.pid, g.mid, g.load_id, g.stamp,
                      m.pnlkey, m.pnlmotion, m.geoareaname, m.districtid
               FROM rel_pub.crp_geom_panel_log g
               LEFT JOIN pnl.mv_pnl_main_maxdate m ON m.pid = g.pid
               WHERE g.load_id = %s
               ORDER BY g.pid''', (d['load'][0],))
    else:
        seznamy['zpracovane'] = (['pid'], [])

    seznamy['bad_navteq'] = db.tabulka(
        '''SELECT b.pid, b.mid, b.navteqver, b.processed, b.stamp,
                  m.pnlkey, m.pnlmotion, m.geoareaname
           FROM rel_pub.crp_bad_navteq b
           LEFT JOIN pnl.mv_pnl_main_maxdate m ON m.pid = b.pid
           ORDER BY b.pid''')

    seznamy['no_calc'] = db.tabulka(
        '''SELECT n.pid, n.mid, n.in_queue, n.calculated, n.processed, n.ignore,
                  n.err_vac_res, n.complexity_index, n.routing_difficulty, n.created,
                  m.pnlkey, m.pnlmotion, m.geoareaname
           FROM rel_pub.crp_no_calc_inc n
           LEFT JOIN pnl.mv_pnl_main_maxdate m ON m.pid = n.pid
           ORDER BY n.pid''')

    seznamy['to_revise'] = db.tabulka(
        '''SELECT r.pid, r.mid, r.resolved, r.processed, r.null_zero_result,
                  r.err_vac_res, r.complexity_index, r.routing_difficulty,
                  r.va, r.vac, r.load_id, r.created,
                  m.pnlkey, m.pnlmotion, m.geoareaname
           FROM rel_pub.crp_pnl_to_revise r
           LEFT JOIN pnl.mv_pnl_main_maxdate m ON m.pid = r.pid
           ORDER BY r.pid''')

    return seznamy


def uloz_csv(seznamy, adresar, razitko, log):
    """Uloží seznamy do CSV s časovým razítkem v názvu. Vrátí {nazev: cesta}."""
    os.makedirs(adresar, exist_ok=True)
    ulozene = {}

    for nazev, (sloupce, radky) in seznamy.items():
        cesta = os.path.join(adresar, 'crp_%s_%s.csv' % (nazev, razitko))
        # utf-8-sig kvůli Excelu, středník kvůli českému locale.
        with open(cesta, 'w', newline='', encoding='utf-8-sig') as f:
            zapis = csv.writer(f, delimiter=';')
            zapis.writerow(sloupce)
            zapis.writerows(radky)
        ulozene[nazev] = cesta
        log('CSV %s: %d řádků -> %s' % (nazev, len(radky), cesta))

    return ulozene


# ---------------------------------------------------------------------------
# Sestavení zprávy
# ---------------------------------------------------------------------------

def sestav_zpravu(d):
    """Vrátí (nadpis, text) pro Teams."""
    den = d['den'].strftime('%d.%m.%Y')
    znacka = {STAV_OK: 'OK', STAV_BEZ_PANELU: 'BEZ PANELU',
              STAV_CHYBA: 'CHYBA', STAV_NEPROBEHL: 'NEPROBEHL'}[d['stav']]
    nadpis = 'CRP BALT - noční běh %s - %s' % (den, znacka)

    r = []

    if d['load']:
        load_id, cas, processed = d['load']
        r.append('Load %d, spuštěn %s, processed = %s'
                 % (load_id, cas.strftime('%H:%M:%S'), processed))
        if d['vystup'] and d['vystup'][0]:
            radku, pidu, faceu, vac = d['vystup']
            r.append('Zpracováno %d panelů / %d ploch, %d řádků v etl_ccf_rel_pub_inc, VAC %s'
                     % (pidu, faceu, radku, vac))
        if d['kontrolni_soucet'] and d['kontrolni_soucet'][0]:
            radku, ts_rots, ts_vac = d['kontrolni_soucet']
            r.append('Kontrolní součet: %d řádků, ts_rots %s, ts_vac %s'
                     % (radku, ts_rots, ts_vac))
    elif d['stav'] == STAV_BEZ_PANELU:
        r.append('Běh proběhl, ale nenašel žádné panely ke zpracování.')
    elif d['stav'] == STAV_NEPROBEHL:
        r.append('Za dnešek není v logu žádný záznam o běhu.')

    if d['publish']:
        cast = ', '.join('rvloadid %d: %d řádků / %d panelů' % (rv, radku, pidu)
                         for rv, radku, pidu in d['publish'])
        r.append('Publikováno celkem - ' + cast)

    if d['revize_dnes'] and d['revize_dnes'][2]:
        nulove, null_vysledek, celkem = d['revize_dnes']
        r.append('Dnes přibylo k revizi: %d (z toho nulový rots %d, NULL výsledek %d)'
                 % (celkem, nulove or 0, null_vysledek or 0))

    if d.get('statistiky'):
        r.append('')
        r.append('Statistiky panelů:')
        sirka = max(len(popis) for popis, _ in d['statistiky'])
        for popis, pocet in d['statistiky']:
            r.append('  %-*s  %6d' % (sirka, popis, pocet))

    if d['tabulky']:
        no_calc, k_revizi, missing, bad_navteq, cache = d['tabulky']
        r.append('')
        r.append('Stav dalších tabulek - crp_missing %d, crp_cache_temp %d'
                 % (missing, cache))

    if d['cache']:
        r.append('Cache spotplánů rvloadid %d: %d řádků' % (d['rv_ids'], d['cache'][0]))

    if d['ids_kontrola']:
        r.append('Konzistence IDS (rvloadid %d) vs IMS - chybí v IDS: %d, starší modifikace: %d, '
                 'zastaralý navteqver: %d, osiřelé facepidy: %d'
                 % (d['rv_ids'], d['chybi_v_ids'], d['starsi_modifikace'],
                    d['stary_navteqver'], d['osirele_facepidy']))
    else:
        r.append('Konzistence IDS vs IMS vynechána - ids.v_panel zatím neobsahuje rvloadid %d '
                 '(obsahuje: %s).' % (d['rv_ids'],
                                      ', '.join(str(x) for x in d['ids_rvloadidy']) or 'nic'))

    if d.get('csv_soubory'):
        r.append('')
        r.append('Seznamy panelů uloženy do:')
        for nazev, cesta in sorted(d['csv_soubory'].items()):
            r.append('  %s: %s' % (nazev, cesta))

    if d['varovani']:
        r.append('')
        r.append('VAROVÁNÍ:')
        for v in d['varovani']:
            r.append('  - ' + v)
    else:
        r.append('')
        r.append('Bez varování.')

    return nadpis, '\n'.join(r)


def sestav_payload(nadpis, text):
    """Tělo requestu pro Power Automate - Adaptive Card.

    Tvar je převzatý ze send_teams_message() v crp_se/crp_engine.py, aby obě
    prostředí posílala do Teams stejný formát.
    """
    text = '%s\nOdeslano: %s' % (text, dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    return {
        'type': 'message',
        'attachments': [
            {
                'contentType': 'application/vnd.microsoft.card.adaptive',
                'content': {
                    'type': 'AdaptiveCard',
                    '$schema': 'http://adaptivecards.io/schemas/adaptive-card.json',
                    'version': '1.4',
                    'body': [
                        {'type': 'TextBlock', 'text': nadpis,
                         'weight': 'Bolder', 'size': 'Medium'},
                        {'type': 'TextBlock', 'text': text, 'wrap': True},
                    ],
                },
            }
        ],
    }


# ---------------------------------------------------------------------------
# Odeslání
# ---------------------------------------------------------------------------

def posli_do_teams(cfg, payload, log):
    teams = cfg['TEAMS'] if 'TEAMS' in cfg else {}
    if not teams or teams.get('enabled', 'false').strip().lower() != 'true':
        log('Teams notifikace vypnutá v config.ini, neodesílám.')
        return False

    url = teams.get('webhook_url', '').strip()
    if not url:
        log('V config.ini chybí webhook_url, neodesílám.')
        return False

    req = urllib.request.Request(
        url, data=json.dumps(payload).encode('utf-8'), method='POST',
        headers={'Content-Type': 'application/json'})
    timeout = float(teams.get('timeout_sec', '10'))

    with urllib.request.urlopen(req, timeout=timeout) as odpoved:
        log('Teams odpověděl HTTP %s.' % odpoved.status)
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def uprav_vystup_konzole():
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass


def main():
    uprav_vystup_konzole()

    p = argparse.ArgumentParser(description='Přehled o nočním běhu CRP BALT do Teams.')
    p.add_argument('--config', default=VYCHOZI_CONFIG, help='cesta ke config.ini')
    p.add_argument('--date', help='den běhu ve tvaru RRRR-MM-DD (výchozí dnešek)')
    p.add_argument('--dry-run', action='store_true',
                   help='jen vypíše přehled, neodesílá do Teams (CSV se uloží)')
    p.add_argument('--no-csv', action='store_true', help='neukládat CSV se seznamy panelů')
    p.add_argument('--export-dir', help='adresář pro CSV (výchozí exports/ v kořeni projektu)')
    args = p.parse_args()

    # interpolation=None: webhook_url obsahuje '%2F' apod. (URL-encoded znaky) a
    # vychozi '%' interpolace v configparseru by na tom spadla.
    cfg = configparser.ConfigParser(interpolation=None)
    if not cfg.read(args.config, encoding='utf-8'):
        print('Nepodařilo se načíst config: %s' % args.config)
        return 2

    # Relativní cesty z config.ini se berou vůči adresáři nástroje, ať si
    # night_report drží logy i exporty u sebe.
    log_soubor = VYCHOZI_LOG
    if 'NIGHT_REPORT' in cfg and cfg['NIGHT_REPORT'].get('log_file'):
        log_soubor = os.path.join(ADRESAR_NASTROJE, cfg['NIGHT_REPORT']['log_file'])

    def log(zprava):
        radek = '%s %s' % (dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), zprava)
        print(radek)
        try:
            os.makedirs(os.path.dirname(log_soubor), exist_ok=True)
            with open(log_soubor, 'a', encoding='utf-8') as f:
                f.write(radek + '\n')
        except OSError:
            pass

    den = (dt.datetime.strptime(args.date, '%Y-%m-%d').date() if args.date
           else dt.date.today())

    export_adresar = VYCHOZI_EXPORT
    if 'NIGHT_REPORT' in cfg and cfg['NIGHT_REPORT'].get('export_dir'):
        export_adresar = os.path.join(ADRESAR_NASTROJE, cfg['NIGHT_REPORT']['export_dir'])
    if args.export_dir:
        export_adresar = args.export_dir

    rv_ids, rv_ims = VYCHOZI_RV_IDS, VYCHOZI_RV_IMS
    if 'NIGHT_REPORT' in cfg:
        rv_ids = int(cfg['NIGHT_REPORT'].get('rvloadid_ids', rv_ids))
        rv_ims = int(cfg['NIGHT_REPORT'].get('rvloadid_ims', rv_ims))

    db = None
    try:
        db = Db(cfg)
        d = seber_data(db, den, rv_ids, rv_ims)
        if not args.no_csv:
            razitko = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
            d['csv_soubory'] = uloz_csv(seber_seznamy(db, d), export_adresar, razitko, log)
        nadpis, text = sestav_zpravu(d)
    except Exception:
        # I selhání samotné kontroly je informace, která patří do Teams.
        log('Kontrola selhala:\n' + traceback.format_exc())
        nadpis = 'CRP BALT - kontrola nočního běhu SELHALA (%s)' % den.strftime('%d.%m.%Y')
        text = ('Skript crp_night_report.py nedoběhl.\n\n'
                + traceback.format_exc())
        if not args.dry_run:
            try:
                posli_do_teams(cfg, sestav_payload(nadpis, text), log)
            except Exception:
                log('Nepodařilo se odeslat ani chybovou zprávu:\n' + traceback.format_exc())
        return 2
    finally:
        if db is not None:
            db.zavri()

    print('=' * 70)
    print(nadpis)
    print('=' * 70)
    print(text)
    print('=' * 70)

    if args.dry_run:
        print('\n--dry-run: payload, který by se odeslal:')
        print(json.dumps(sestav_payload(nadpis, text), ensure_ascii=False, indent=2))
    else:
        try:
            posli_do_teams(cfg, sestav_payload(nadpis, text), log)
        except Exception:
            log('Odeslání do Teams selhalo:\n' + traceback.format_exc())
            return 2

    log('Hotovo, stav: %s, varování: %d' % (d['stav'], len(d['varovani'])))

    if d['stav'] in (STAV_CHYBA, STAV_NEPROBEHL):
        return 2
    return 1 if d['varovani'] else 0


if __name__ == '__main__':
    sys.exit(main())
