# Data Governance Considerations

## PII Assessment

The Instacart Market Basket dataset used in this pipeline contains **no personally
identifiable information (PII)**. Specifically:

- **Users** are identified only by an anonymized integer `user_id` — no names, emails,
  addresses, phone numbers, or any other identifying attributes were ever present in
  the source data. Instacart anonymized this before publishing the dataset.
- **Orders** are identified by `order_id` with no associated payment information,
  delivery addresses, or timestamps precise enough to identify an individual.
- **Products, aisles, and departments** are generic catalog reference data with no
  user-linkable attributes.

This means no masking, tokenization, or access restriction policies are required for
this specific dataset. However, documenting this assessment explicitly matters: "no
PII present" is a conclusion that should be reached deliberately and recorded, not
assumed by default.

---

## What a Production Implementation Would Add

In a real production environment handling genuine customer transaction data (e.g., the
actual Instacart platform, or a comparable retail/beverage data system like the target
role's domain), the following governance controls would be appropriate:

### 1. PII Masking Policies

Snowflake's Dynamic Data Masking would be applied to any columns containing real
customer identifiers (names, emails, phone numbers, loyalty IDs) in the raw and
staging layers. Masking policies would expose full values only to roles with explicit
data access grants (e.g., a `data_engineer` role for pipeline work) while returning
masked/hashed values to analytical roles (e.g., a `data_analyst` role for BI
reporting). This enforces the principle that PII exposure is opt-in and auditable,
not the default.

Example Snowflake masking policy pattern:

```sql
CREATE MASKING POLICY email_mask AS (val STRING) RETURNS STRING ->
  CASE WHEN CURRENT_ROLE() IN ('DATA_ENGINEER') THEN val
       ELSE SHA2(val)  -- analysts see a consistent hash, not the real email
  END;

ALTER TABLE raw.raw_users MODIFY COLUMN email
  SET MASKING POLICY email_mask;
```

### 2. Role-Based Access Controls (RBAC)

The current project uses `ACCOUNTADMIN` for all dbt operations — appropriate for a
single-user personal project, but not acceptable in production. A real implementation
would define at minimum:

- A **pipeline service role** with only the privileges dbt actually needs: `USAGE` on
  the warehouse, `READ` on the `raw` schema, `CREATE TABLE/VIEW` on the `analytics`
  schema. No DDL beyond what's needed, no access to other databases.
- A **read-only analyst role** with `SELECT` on the `analytics` schema only — no
  access to `raw`, no ability to modify any objects.
- A **PII-access role** granted only to users with a legitimate need and documented
  approval, controlling access to any unmasked PII columns.

### 3. Data Retention Policies

Raw transaction data would have an explicit retention window — for example, 2 years of
order history retained in the raw layer, with older partitions archived or dropped on
a schedule. Snowflake's Time Travel window (configurable per table) would be set
explicitly rather than left at defaults, and a retention runbook would document what
data is kept, for how long, and under what legal/compliance basis (e.g., GDPR right to
erasure implications for any EU customer data).

### 4. Lineage and Audit Trail

The dbt lineage graph generated in this project (see `docs/lineage_graph.png`) already
provides transformation-level lineage: anyone can trace any mart column back to its
raw source. In a production setting, this would be complemented by:

- Column-level lineage tracking in a data catalog (e.g., Alation, Collibra, or
  Snowflake's own data catalog features) for compliance audit purposes.
- Snowflake's Query History and Access History features enabled for monitoring who
  accessed which data and when — particularly important for PII-adjacent tables.
- dbt run artifacts (manifest.json, run_results.json) stored and versioned so the
  pipeline's state at any point in time is reconstructable.

---

## Summary

| Control           | This Project                    | Production Recommendation               |
| ----------------- | ------------------------------- | --------------------------------------- |
| PII present       | No — dataset is anonymized      | Assess per source system                |
| Column masking    | Not required                    | Dynamic Data Masking on PII columns     |
| Role-based access | ACCOUNTADMIN (personal project) | Scoped service + analyst + PII roles    |
| Data retention    | No policy (trial account)       | Explicit window per table, documented   |
| Lineage           | dbt docs lineage graph          | dbt + data catalog for compliance audit |
| Access auditing   | Not configured                  | Snowflake Access History enabled        |
