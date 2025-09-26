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
-- >>> ROLE B AUTOLOG ANCHOR (do not remove) <<<


-- ----- testcase: mytest (Role B, 2025-09-26 13:37:51) -----
SELECT COUNT(*) AS fk_violations FROM public.enrollments child LEFT JOIN public.students parent ON child.sid = parent.sid WHERE child.sid IS NOT NULL AND parent.sid IS NULL;
SELECT COUNT(*) FROM public.enrollments;
SELECT COUNT(*) FROM public.enrollments c JOIN public.students p ON c.sid = p.sid;
SELECT COUNT(*) AS fk_violations FROM public.enrollments child LEFT JOIN public.courses parent ON child.cid = parent.cid WHERE child.cid IS NOT NULL AND parent.cid IS NULL;
SELECT COUNT(*) FROM public.enrollments;
SELECT COUNT(*) FROM public.enrollments c JOIN public.courses p ON c.cid = p.cid;
SELECT COUNT(*) AS fk_violations FROM public.payments child LEFT JOIN public.students parent ON child.sid = parent.sid WHERE child.sid IS NOT NULL AND parent.sid IS NULL;
SELECT COUNT(*) FROM public.payments;
SELECT COUNT(*) FROM public.payments c JOIN public.students p ON c.sid = p.sid;


-- ----- testcase: mytest (Role B, 2025-09-26 13:31:57) -----
SELECT COUNT(*) AS fk_violations FROM public.t4 child LEFT JOIN public.t1 parent ON child.k1 = parent.k1 WHERE child.k1 IS NOT NULL AND parent.k1 IS NULL;
SELECT COUNT(*) FROM public.t4;
SELECT COUNT(*) FROM public.t4 c JOIN public.t1 p ON c.k1 = p.k1;
SELECT COUNT(*) AS fk_violations FROM public.t4 child LEFT JOIN public.t2 parent ON child.k2 = parent.k2 WHERE child.k2 IS NOT NULL AND parent.k2 IS NULL;
SELECT COUNT(*) FROM public.t4;
SELECT COUNT(*) FROM public.t4 c JOIN public.t2 p ON c.k2 = p.k2;
SELECT COUNT(*) AS fk_violations FROM public.t4 child LEFT JOIN public.t3 parent ON child.k3 = parent.k3 WHERE child.k3 IS NOT NULL AND parent.k3 IS NULL;
SELECT COUNT(*) FROM public.t4;
SELECT COUNT(*) FROM public.t4 c JOIN public.t3 p ON c.k3 = p.k3;

-- =====================================
-- Role C: Normalization
-- =====================================
-- >>> ROLE C AUTOLOG ANCHOR (do not remove) <<<
-- ----- testcase: mytest (Role C, 2025-09-26 13:39:43) -----
SELECT COUNT(*), COUNT(DISTINCT name) FROM students;
SELECT COUNT(*), COUNT(DISTINCT level) FROM students;
SELECT COUNT(*), COUNT(DISTINCT dept) FROM courses;
SELECT COUNT(DISTINCT (dept,chair)), COUNT(DISTINCT dept) FROM courses;
SELECT COUNT(*), COUNT(DISTINCT sid) FROM enrollments;
SELECT COUNT(*), COUNT(DISTINCT cid) FROM enrollments;
SELECT COUNT(*), COUNT(DISTINCT sid) FROM payments;
SELECT COUNT(DISTINCT (sid,amount)), COUNT(DISTINCT sid) FROM payments;
SELECT COUNT(*), COUNT(DISTINCT amount) FROM payments;



-- =====================================
-- Role D: Output / Orchestration
-- =====================================
-- (mostly file writing in Python, not much SQL —
-- but add any global SQL like summary SELECTs if you use them)