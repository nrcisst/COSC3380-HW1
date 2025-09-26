-- =====================================
-- checkdb.sql
-- Purpose: Collect ALL SQL commands run by every role (A/B/C/D)
--          for referential integrity + normalization checking.
--          Each teammate pastes their tested queries under their section.
-- =====================================

-- =====================================
-- Role A: Parser
-- =====================================
-- (usually just Python parsing, but if you query catalogs, paste here)


-- =====================================
-- Role B: Referential Integrity
-- =====================================

-- Check foreign key in table t1 referencing t2
SELECT COUNT(*) 
FROM "t1" T1 
LEFT JOIN "t2" T2 ON T1."fk_column" = T2."pk_column";  -- Checking all rows in t1, even with no match

SELECT COUNT(*) 
FROM "t1" T1 
JOIN "t2" T2 ON T1."fk_column" = T2."pk_column";  -- Only matching rows

-- Check foreign key in table t2 referencing t3
SELECT COUNT(*) 
FROM "t2" T2 
LEFT JOIN "t3" T3 ON T2."fk_column" = T3."pk_column";

SELECT COUNT(*) 
FROM "t2" T2 
JOIN "t3" T3 ON T2."fk_column" = T3."pk_column";

-- Table t3 has no foreign keys, so referential integrity is valid (RI=Y)

-- =====================================
-- Role C: Normalization
-- =====================================
-- Example:
-- SELECT COUNT(DISTINCT x) FROM t1;
-- SELECT COUNT(DISTINCT x, y) FROM t1;
-- SELECT COUNT(x), COUNT(DISTINCT x) FROM t1;


-- =====================================
-- Role D: Output / Orchestration
-- =====================================
-- (mostly file writing in Python, not much SQL —
-- but add any global SQL like summary SELECTs if you use them)
