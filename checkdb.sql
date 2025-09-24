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
-- Example:
-- SELECT COUNT(*) FROM t1;
-- SELECT COUNT(*) 
-- FROM t1 JOIN t2 ON t1.fk = t2.pk;


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
