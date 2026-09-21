# CRP BALT – datový slovník (schéma `rel_pub`)

39 tabulek schématu `rel_pub` z `ddl/schema_rel_pub.sql` (export `imsbalt_prod`), seskupených podle role v
procesu. U každé je uvedeno, která funkce ji plní/čte – podrobný popis toku viz
[`01_proces_crp_balt.md`](01_proces_crp_balt.md). Typy sloupců jsou přesně podle DDL; PostGIS geometrie je typu
`topo.geometry`.

## Seznam panelů k zpracování

### `crp_all_panel`
Kompletní seznam panelů pro **plný** běh (`auto_rel_pub_ccf`) – i mezipaměť mezi během napříč voláními.
Plní/čte: `auto_rel_pub_ccf()`.

| Sloupec | Typ |
|---|---|
| mid | bigint |
| pid | bigint |
| the_geom | topo.geometry |
| buffer_geom | topo.geometry |
| vac_all_1 | double precision |
| maxdist | double precision |
| intersection_type | character varying |
| last_vacid | bigint |
| err_vac_res | character varying |

### `crp_geom_panel` / `crp_geom_panel_temp` / `crp_geom_panel_log`
Obdoba `crp_all_panel` pro **přírůstkový** běh (`auto_rel_pub_inc`). `_temp` slouží jen k odstranění duplicit
(`TRUNCATE` + `INSERT DISTINCT` + zpět), `_log` je trvalá evidence zpracovaných panelů po loadech (sloupce navíc:
`load_id bigint`, `desc_text character varying`, `stamp timestamp(0) DEFAULT now()`). Sloupce shodné s
`crp_all_panel` (v `crp_geom_panel` navíc typovaná geometrie `topo.geometry(Point,4326)`, v `_temp` je
`vac_all_2` místo `vac_all_1`). Plní/čte: `auto_rel_pub_inc()`.

## Průsečíky s trasami / evidence

### `crp_all_intersection` / `crp_intersection`
Prostorový průnik bufferu panelu s trasou respondenta – `crp_all_intersection` pro plný běh, `crp_intersection`
pro přírůstkový. Sloupce: `mid bigint`, `sid integer`, `week_day_name character varying(4)`, `week_day_type
character varying(4)`. Plní: `auto_rel_pub_ccf()` / `auto_rel_pub_inc()`.

### `crp_evidence`
Finální seznam panel×trip (`mid`, `sid`, `week_day_name`, `week_day_type`, `prob double precision`) – sloučení
přímých průsečíků a substituovaných (viz `crp_nearest20`). Vstup do CCF výpočtu (`crp_ccf_output`). Plní/čte:
obě hlavní funkce.

### `crp_panel_wo_its`
Panely bez přímého průsečíku s trasou (`intersection_type = 'SUBSTITUTE'`) – mezistupeň před dohledáním
nejbližšího náhradního panelu. Sloupce: `mid`, `pid` (bigint), `the_geom topo.geometry(Point,4326)`,
`buffer_geom topo.geometry`, `vac_all_1 double precision`.

### `crp_nearest20`
20 geometricky nejbližších panelů k panelu bez průsečíku, z nich vybraný nejlepší podle rozdílu VAC
(`ST_DWithin` 5 km, KNN). Sloupce: `mid_woi`, `best_mid_near`, `best_pid_near` (bigint), `vac_woi`,
`best_vac_near`, `geometric_distance`, `value_difference` (double precision), `the_geom`, `buffer_geom`
(topo.geometry, jen v `auto_rel_pub_ccf` verzi).

## CCF výpočet

### `crp_ccf_output`
Řádek = respondent × panel × face. Vstup pro dopočet `ts_rots`/VA/VAC. Sloupce: `pid`, `faceid`, `facepid`
(bigint), `sid` (integer), `respondent_id` (varchar), `respweight`, `va`, `vac`, `ts_rots` (double precision),
`modality`, `dayofweek` (varchar(255)). Plní/čte: obě hlavní funkce (společná tabulka, `TRUNCATE` na začátku
každého běhu).

### `crp_ts_rots_temp`
Mezivýpočet technické viditelnosti na úrovni `pid`+`faceid`: `ims_rots`, `ims_vac` (integer), `ims_va`, `ts_rots`,
`ts_va`, `ts_va_adj` (double precision), `ts_vac` (integer), `ts_freq` (integer).

### `crp_control_sum`
Kontrolní součty po loadu: `pid`, `faceid` (bigint), `sum_of_ts_rots`, `sum_of_ts_vac` (double precision),
`load_id` (integer), `load_type character varying(4)` (`'INIT'`/`'INCR'`), `inserted timestamp(0) DEFAULT now()`.
Jen `INSERT`, nikdy `TRUNCATE` – historická evidence všech loadů.

## Snapshot vstupu z IMS

### `crp_ims_input` / `crp_ims_input_inc`
Široký snapshot vlastností panelu (z `pnl`) + týdenní VAC/VA/ROTS podle `transptype` (`all`/`veh`/`pub`/`ped`/
`bic`) z `res.res_online_ea_output_round`. `crp_ims_input` pro plný běh, `crp_ims_input_inc` pro přírůstkový;
sloupce identické (`pid`, `mid`, `facepid`, `faceid_db` bigint; `pnlkey`, `pnldesc2`, `face_userid`, `ownid_desc`,
`pnlillum_desc`, `pnlmotion(_desc)`, `pnlsubtype_desc`, `pnlvaienv`, `location_name`, `location_aadf`, `agl_name`,
`geoareaname`, `district`, `iarea_name`, `pnldesc`, `release`, `err_vac_res` varchar; `ownid`, `faces`, `pos`,
`display_time`, `loop_time`, `sizeid`, `sheet_id`, `location_id`, `vac_week_*`, `rots_week_*` integer;
`frmwidth`, `frmheight` double precision; `va_week_* numeric(4,3)`; `vacid bigint`). Plní: `create_ims_input_ccf()`
/ `create_ims_input_inc()`. Čte: `auto_rel_pub_*` (join na `faceid_db`) a `f_fill_crp_cache_temp()`.

### `crp_ims_input_inc_log`
Historizace `crp_ims_input_inc` po každém loadu (stejné sloupce + `row_id integer NOT NULL`, `created timestamp
DEFAULT now()`, `cache_ok boolean DEFAULT false` – nastaví `f_fill_crp_cache_temp()`, pokud pro daný `vacid` vznikl
záznam v cache). Jen `INSERT`/`UPDATE`, nikdy mazáno.

## Fronta přepočtu / problémové panely

### `crp_no_calc` / `crp_no_calc_inc`
Panely bez VAC nebo s chybou výpočtu, vyřazené z aktuálního zpracování. `crp_no_calc` (plný běh): `pid bigint`,
`in_queue`/`calculated`/`processed boolean DEFAULT false`, `err_vac_res varchar`. `crp_no_calc_inc` (přírůstkový
běh) navíc: `mid bigint`, `ignore boolean`, `created`/`updated timestamp(0) DEFAULT now()`,
`created_by`/`updated_by varchar DEFAULT current_user()`, `complexity_index numeric`, `routing_difficulty text`
(triggerově aktualizované – `update_timestamp_trigger`, `update_updated_by_trigger`). Řídí zařazení do fronty
`oper.oper_vac_queue` a návrat do zpracování po dopočtu.

### `crp_pnl_to_revise`
Panely k ruční revizi – buď s trvalou chybou výpočtu (`err_vac_res`), nebo s `NULL`/zápornou VA či VAC po loadu
(`null_zero_result boolean`). Sloupce: `pid`, `mid`, `complexity_index`, `routing_difficulty`, `va`, `vac`,
`load_id`, `resolved`/`processed boolean DEFAULT false`, `created`/`updated`/`created_by`/`updated_by` (stejný
vzor jako `crp_no_calc_inc`, se stejnými triggery).

### `crp_bad_navteq`
Panely se špatnou (neaktuální) verzí navteq mapových dat (`navteqver`), zjištěné vůči `fnc.f_params()`. Sloupce:
`mid`, `pid` (bigint), `navteqver character varying(4)`, `processed integer DEFAULT 0`, `stamp timestamp(0)
DEFAULT now()`.

### `crp_missing`
Ruční vstup – seznam `pid bigint`, které se mají zahrnout do dalšího běhu "zvenku" (typicky i panely vrácené z
fronty přepočtu). Po každém běhu se vyprázdní (`TRUNCATE`).

### `crp_unpublish_list`
Panely k odpublikování z `res.res_release_publish` (změna statusu, demolice) v aktuálním běhu. Sloupce: `pid`,
`mid` (bigint), `desc_text varchar`, `stamp timestamp(0) DEFAULT now()`.

## Referenční/importovaná data (jen čtení uvnitř `rel_pub` funkcí)

### `crp_routing`
Trasy respondentů (import z RM/Routing modulu) – hlavní prostorový vstup pro průsečíky. Klíčové sloupce: `id
varchar NOT NULL`, `geom topo.geometry(MultiLineString,4326)` (GiST index `idx_routing`), `sid integer`,
`respondent_id varchar`, `week_day_name(_type)`, `trip_freq double precision`, `routingmode varchar(3)`, plus
metadata z importu (`wave`, `mqid`, `tripid`, `segmentid`, `tripdate`, `country`, ...).

### `crp_resp_weight_input`
Váhy respondentů: `respondent_id`, `respondent_num`, `country character varying(3)`, `benchwgt_pp double
precision`.

### `crp_freq_trips_vector`
Rozdělení frekvence tripů do pravděpodobnostních pásem: `weekdaytype(name) character varying(2)`, `freq_from`,
`freq_to`, `prob double precision`.

## Cache dynamické viditelnosti (digitální/pohyblivé panely)

### `crp_spotplans`
Distinct kombinace `rvloadid`, `spot_length`, `spot_pause` z `ids.ids_vac_calc_campaign_cache2` pro `rvloadid =
100` – řídí smyčku ve `f_fill_crp_cache_temp()`.

### `crp_cache_temp`
Pracovní výstup `f_fill_crp_cache_temp()` před kopií do `ids.ids_vac_calc_campaign_cache2`: `rvloadid`, `faceid`,
`vacid`, `facepid` (integer), `spot_length`, `spot_pause`, `vac`, `vac_orig` (double precision), `rots`,
`rots_orig` (numeric), `group_def varchar`, `period_hours double precision`.

### `crp_cache_log`
Trvalý log obsahu `crp_cache_temp` po každém běhu (stejné sloupce + `row_id integer NOT NULL`, `created timestamp
DEFAULT now()`).

### `crp_cache_del_log`
Log záznamů smazaných z `ids.ids_vac_calc_campaign_cache2` (demolované panely). Stejné sloupce jako
`crp_cache_log`, ale `facepid`/`faceid`/`vacid bigint` a `deleted_at timestamp with time zone DEFAULT now()`.

## Logování

### `crp_log_table`
Perzistentní log (`log_msg()`): `row_id integer NOT NULL`, `timestamp timestamp(6) DEFAULT now()`, `desc_text
text`, `type varchar(20)` (INFO/ERROR), `source varchar(50)`, `current_usr varchar(200) DEFAULT current_user()`.

## Výstup / ETL do IDS

### `etl_ccf_rel_pub` / `etl_ccf_rel_pub_inc`
Finální výstup CCF výpočtu, který čte cílový systém IDS. `etl_ccf_rel_pub` z `auto_rel_pub_ccf()`,
`etl_ccf_rel_pub_inc` z `auto_rel_pub_inc()` (**jediný skutečně "continuous" výstup**). Sloupce: `pid`, `mid`,
`faceid`, `facepid` (integer), `vacid bigint`, `respondent_id varchar`, `country character varying(3)`,
`weekdayname varchar`, `resp_weight`, `ts_rots`, `va`, `vac` (double precision), `load_id integer`.

### `etl_ccf_rel_pub_loads` / `etl_ccf_rel_pub_loads_inc`
Evidence loadů – zdroj `load_id` (sekvence `load_id_seq` / `load_id_inc_seq`). Sloupce: `timestamp timestamp(6)
DEFAULT now()`, `current_user varchar`, `load_id bigint DEFAULT nextval(...)`, `processed integer DEFAULT 0`.

## Nepoužívané tabulky

Existují v DDL, ale žádná funkce v tomto schématu je nečte ani neplní (ověřeno grepem přes všech 7 funkcí):

- **`crp_ims_input2`** – varianta `crp_ims_input` se sloupci `*_brutto_daily_*` místo `*_week_*` (denní místo
  týdenní hrubé hodnoty). Vypadá jako připravená/opuštěná rozšíření.
- **`crp_ims_temp`** – další, zúžená varianta stejné struktury (`*_brutto_weekly_*`).
- **`crp_new_mids`** – jen `mid bigint`, `pid bigint`.
- **`crp_ccf_input`** – `mid`, `sid`, `pid` + `respondent_id`, `routingmode`, `tsyear`, `desc_text` – vypadá na
  předchůdce dnešního `crp_ccf_output` vstupu.
- **`crp_ccf_output_inc`** – sloupcově identická s `crp_ccf_output`, ale přírůstková funkce ve skutečnosti sdílí
  `crp_ccf_output` (viz výše) – tahle tabulka zůstala nevyužitá.
- **`crp_freq_trips`** – "široký" formát frekvenční matice (sloupce `mo`..`su`), nahrazený `crp_freq_trips_vector`.

U CRP ZA byly nepoužívané jen `crp_ims_input2`, `crp_new_mids` a `crp_routing_ps` (ta v BALT DDL vůbec neexistuje)
– sady se tedy neshodují, viz [`01_proces_crp_balt.md`](01_proces_crp_balt.md#zjištěné-rozdíly-oproti-crp-za).
