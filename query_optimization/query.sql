-- the most efficient method of joining three tables is using inner and left join. Since, instr_id col is in both id_map and sp_500, 
-- inner join is used on instr_id to populate all the instr_id from sp500. Similarly, id col is in both id_map and esg_scores, 
-- and since all the ids are needed, left join on id is used to populate the ids and their correspoding scores (also null rows are epected for non existing id in esg_scores)


-- Create temp table to join the tables
CREATE TEMP TABLE TEMP_SP500_ESG_SCORES AS (
	SELECT i.id, i.instr_id, i.name, COALESCE(E.total_score,0) AS total_score, COALESCE(E.e_score,0) AS e_score, COALESCE(E.s_score,0) AS s_score, COALESCE(E.g_score,0) AS g_score
		FROM DATA_CHALLENGES.ID_MAP I
	INNER JOIN DATA_CHALLENGES.SP500 S 
		ON I.INSTR_ID = S.INSTR_ID
	LEFT JOIN DATA_CHALLENGES.ESG_SCORES E
		ON I.ID = E.ID
	ORDER BY I.ID
);

--SELECT * FROM TEMP_SP500_ESG_SCORES

-- Create temp table to fill rank col
CREATE TEMP TABLE TEMP_SP500_ESG_SCORES_RANK AS (
	SELECT id, instr_id, name, total_score, e_score, s_score, g_score,
	PERCENT_RANK() OVER (ORDER BY total_score asc) per_rank 
	FROM TEMP_SP500_ESG_SCORES
);

--SELECT * FROM TEMP_SP500_ESG_SCORES_RANK

-- Insert into SP500_ESG_SCORES from temp table
INSERT INTO DATA_CHALLENGES.SP500_ESG_SCORES (
	id
	,instr_id
	,name
	,total_score
	,e_score
	,s_score
	,g_score
	,rank
)
SELECT 
	id
	,instr_id
	,name
	,total_score
	,e_score
	,s_score
	,g_score
	,per_rank
FROM TEMP_SP500_ESG_SCORES_RANK;

--Find Median for each score
WITH 
	MEDIAN_TOTAL_SCORE AS (
		select percentile_cont(0.5) within group (order by total_score) as total_score_median
		from DATA_CHALLENGES.SP500_ESG_SCORES
),
	MEDIAN_E_SCORE AS (
		select percentile_cont(0.5) within group (order by e_score) as e_score_median
		from DATA_CHALLENGES.SP500_ESG_SCORES
),
	MEDIAN_S_SCORE AS (
		select percentile_cont(0.5) within group (order by s_score) as s_score_median
		from DATA_CHALLENGES.SP500_ESG_SCORES
),
	MEDIAN_G_SCORE AS (
		select percentile_cont(0.5) within group (order by g_score) as g_score_median
		from DATA_CHALLENGES.SP500_ESG_SCORES
);

select cast(total_score_median as numeric(15,6)) as total_score_median, cast(e_score_median as numeric(15,6)) as e_score_median, cast(s_score_median as numeric(15,6)) as s_score_median, cast(g_score_median as numeric(15,6)) as g_score_median 
from MEDIAN_TOTAL_SCORE, MEDIAN_E_SCORE, MEDIAN_S_SCORE, MEDIAN_G_SCORE;


-- Suggest a database and ETL architecture if there were a much larger universe of companies eg. 50,000. 
-- This universe would be updated on a weekly basis and rankings would need to be recomputed upon update.

-- With 50,000 rows of data that updates on a weekly basins, RDBMS is still a good choice.
-- ETL Arch:
-- Schedule a job that runs on a weekly basis that extracts, transforms and loads data into raw, stage and target tables. 
-- If data is inconsistent, then it's good to have raw table (truncate after each load) where all the cleaning or deduplication is done before inserting into stage table.
-- Also, it is good to have additional col - such as batch id on each table, so that transformation into target table can be done only for that rows with that batch id. This will avoid selecting any unchanged data.
-- Create sp for id_map, that merges on ids and update the rows for name (comapny name may change), or insert new rows. Similarly for esg_scores to update or insert new scores.
-- Then compute for the rankings.




	