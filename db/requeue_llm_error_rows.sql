-- Re-queue rows that were saved as "irrelevant" because the LLM call FAILED, not because the post
-- was judged irrelevant.
--
-- Background: aura-llm relayed upstream errors (e.g. "Error from Ollama: 429") as HTTP 200 text.
-- The analyser found no JSON in it and stored the default SentimentResponse: sentiment_score = 0,
-- sentiment_category = 'invalid', and marked the row's passes done so it was never retried.
-- A real "irrelevant" verdict from the prompts comes back as {"category": "media.movie",
-- "positivity_score": -1}, i.e. category stays media.movie - so category 'invalid' + score 0
-- identifies the failed calls. (The analyser now throws on a no-JSON reply instead.)
--
-- What this does, per table, for rows in the window:
--   1. copies the affected rows' current values into requeue_llm_error_backup (for rollback);
--   2. restores sentiment_category to the most common real category used for that keyword in the
--      same table (NULL if none - the analyser treats NULL as media.movie);
--   3. clears sentiment_score and sentiment_passes so every sentiment pass runs on the row again.
-- The rows have no mentions (the copier removes score <= 0 rows), so nothing else needs touching.
--
-- Usage (window in days, default 7):
--   psql -d aura -v days=7 -f requeue_llm_error_rows.sql
-- Rollback for one run:
--   see the commented block at the bottom.

\if :{?days}
\else
  \set days 7
\endif

BEGIN;

CREATE TABLE IF NOT EXISTS requeue_llm_error_backup (
    requeued_at        timestamptz NOT NULL DEFAULT now(),
    source_table       text        NOT NULL,
    id                 text        NOT NULL,
    sentiment_score    integer,
    sentiment_category text,
    sentiment_passes   text[]
);

-- Affected rows: category 'invalid', score 0, posted within the window.
CREATE TEMP TABLE requeue_rows ON COMMIT DROP AS
SELECT 'x_posts'::text AS source_table, id::text AS id, keyword, sentiment_score, sentiment_category, sentiment_passes
  FROM x_posts
 WHERE sentiment_category = 'invalid' AND sentiment_score = 0
   AND created_at > now() - make_interval(days => :days)
UNION ALL
SELECT 'youtube_comments', id::text, keyword, sentiment_score, sentiment_category, sentiment_passes
  FROM youtube_comments
 WHERE sentiment_category = 'invalid' AND sentiment_score = 0
   AND published_at > now() - make_interval(days => :days)
UNION ALL
SELECT 'instagram_posts', id::text, keyword, sentiment_score, sentiment_category, sentiment_passes
  FROM instagram_posts
 WHERE sentiment_category = 'invalid' AND sentiment_score = 0
   AND "timestamp" > now() - make_interval(days => :days)
UNION ALL
SELECT 'reddit_posts', id::text, keyword, sentiment_score, sentiment_category, sentiment_passes
  FROM reddit_posts
 WHERE sentiment_category = 'invalid' AND sentiment_score = 0
   AND created_at > now() - make_interval(days => :days);

INSERT INTO requeue_llm_error_backup (source_table, id, sentiment_score, sentiment_category, sentiment_passes)
SELECT source_table, id, sentiment_score, sentiment_category, sentiment_passes FROM requeue_rows;

-- Most common real category per (table, keyword), to undo the 'invalid' overwrite.
CREATE TEMP TABLE keyword_category ON COMMIT DROP AS
SELECT DISTINCT ON (source_table, keyword) source_table, keyword, sentiment_category
  FROM (
        SELECT 'x_posts' AS source_table, keyword, sentiment_category, count(*) AS n FROM x_posts
         WHERE sentiment_category IS NOT NULL AND sentiment_category <> 'invalid' GROUP BY 2, 3
        UNION ALL
        SELECT 'youtube_comments', keyword, sentiment_category, count(*) FROM youtube_comments
         WHERE sentiment_category IS NOT NULL AND sentiment_category <> 'invalid' GROUP BY 2, 3
        UNION ALL
        SELECT 'instagram_posts', keyword, sentiment_category, count(*) FROM instagram_posts
         WHERE sentiment_category IS NOT NULL AND sentiment_category <> 'invalid' GROUP BY 2, 3
        UNION ALL
        SELECT 'reddit_posts', keyword, sentiment_category, count(*) FROM reddit_posts
         WHERE sentiment_category IS NOT NULL AND sentiment_category <> 'invalid' GROUP BY 2, 3
       ) c
 ORDER BY source_table, keyword, n DESC;

UPDATE x_posts t
   SET sentiment_score = NULL, sentiment_passes = '{}',
       sentiment_category = (SELECT kc.sentiment_category FROM keyword_category kc
                              WHERE kc.source_table = 'x_posts' AND kc.keyword = t.keyword)
  FROM requeue_rows r WHERE r.source_table = 'x_posts' AND r.id = t.id::text;

UPDATE youtube_comments t
   SET sentiment_score = NULL, sentiment_passes = '{}',
       sentiment_category = (SELECT kc.sentiment_category FROM keyword_category kc
                              WHERE kc.source_table = 'youtube_comments' AND kc.keyword = t.keyword)
  FROM requeue_rows r WHERE r.source_table = 'youtube_comments' AND r.id = t.id::text;

UPDATE instagram_posts t
   SET sentiment_score = NULL, sentiment_passes = '{}',
       sentiment_category = (SELECT kc.sentiment_category FROM keyword_category kc
                              WHERE kc.source_table = 'instagram_posts' AND kc.keyword = t.keyword)
  FROM requeue_rows r WHERE r.source_table = 'instagram_posts' AND r.id = t.id::text;

UPDATE reddit_posts t
   SET sentiment_score = NULL, sentiment_passes = '{}',
       sentiment_category = (SELECT kc.sentiment_category FROM keyword_category kc
                              WHERE kc.source_table = 'reddit_posts' AND kc.keyword = t.keyword)
  FROM requeue_rows r WHERE r.source_table = 'reddit_posts' AND r.id = t.id::text;

SELECT source_table, count(*) AS requeued FROM requeue_rows GROUP BY 1 ORDER BY 1;

COMMIT;

-- Rollback of the most recent run (restores score, category and passes from the backup):
-- BEGIN;
-- WITH last AS (SELECT * FROM requeue_llm_error_backup
--                WHERE requeued_at = (SELECT max(requeued_at) FROM requeue_llm_error_backup))
-- UPDATE x_posts t SET sentiment_score = l.sentiment_score, sentiment_category = l.sentiment_category,
--        sentiment_passes = l.sentiment_passes FROM last l WHERE l.source_table = 'x_posts' AND l.id = t.id::text;
-- (repeat the UPDATE for youtube_comments, instagram_posts, reddit_posts)
-- COMMIT;
