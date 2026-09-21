# Dokumentace CRP BALT

Ručně psaná dokumentace k procesu CRP BALT (Continuous Release Publishing, Pobaltí – Litva, Lotyšsko, Estonsko),
zdroj DDL: [`../ddl/schema_rel_pub.sql`](../ddl/schema_rel_pub.sql) (export z databáze `imsbalt_prod`, schéma
`rel_pub`).

- [`01_proces_crp_balt.md`](01_proces_crp_balt.md) – co proces dělá, jak na sebe funkce navazují, datový tok,
  zjištěné rozdíly oproti sesterskému procesu CRP ZA.
- [`02_datovy_slovnik.md`](02_datovy_slovnik.md) – tabulky schématu `rel_pub`, jejich účel, sloupce a kdo je
  čte/plní.

Než sáhneš do syrového `schema_rel_pub.sql`, začni tady – podrobnosti viz [`../CLAUDE.md`](../CLAUDE.md).

Poslední aktualizace odpovídá DDL exportu z 21.9.2026.
