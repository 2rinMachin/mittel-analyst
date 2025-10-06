CREATE OR REPLACE VIEW event_counts_post AS
SELECT
    post_id,
    SUM(CASE WHEN kind = 'view' THEN 1 ELSE 0 END) as views,
    SUM(CASE WHEN kind = 'like' THEN 1 ELSE 0 END) as likes,
    SUM(CASE WHEN kind = 'share' THEN 1 ELSE 0 END) as shares
FROM events
GROUP BY post_id;

CREATE OR REPLACE VIEW event_counts_author AS
SELECT
    a.author.id AS user_id,
    SUM(CASE WHEN e.kind = 'view' THEN 1 ELSE 0 END) AS views,
    SUM(CASE WHEN e.kind = 'like' THEN 1 ELSE 0 END) AS likes,
    SUM(CASE WHEN e.kind = 'share' THEN 1 ELSE 0 END) AS shares
FROM
    events e
    JOIN articles a ON a._id = e.post_id
GROUP BY a.author.id;

CREATE OR REPLACE VIEW event_counts_user AS
SELECT
    e.user_id AS user_id,
    SUM(CASE WHEN e.kind = 'view' THEN 1 ELSE 0 END) AS views,
    SUM(CASE WHEN e.kind = 'like' THEN 1 ELSE 0 END) AS likes,
    SUM(CASE WHEN e.kind = 'share' THEN 1 ELSE 0 END) AS shares
FROM events e
GROUP BY e.user_id;

CREATE OR REPLACE VIEW comment_counts_received AS
SELECT
    a.author.id as user_id,
    SUM(a.commentsCount) as user_comments_received
FROM articles a
GROUP BY a.author.id;

CREATE OR REPLACE VIEW comment_counts_made AS
SELECT
    c.author.id as user_id,
    COUNT(*) as user_comments_made
FROM comments c
GROUP BY c.author.id;

CREATE OR REPLACE VIEW counts_tag AS
SELECT
    t.tag as category,
    SUM(ecp.views) as views,
    SUM(ecp.likes) as likes,
    SUM(ecp.shares) as shares,
    SUM(a.commentsCount) AS comments
FROM
    event_counts_post ecp
    JOIN articles a ON a._id = ecp.post_id
    CROSS JOIN UNNEST (a.tags) AS t (tag)
GROUP BY
    t.tag;

CREATE OR REPLACE VIEW post_scores AS
SELECT
    a._id AS post_id,
    a.author.id AS author_id,
    a.title AS title,
    COALESCE(ec.views, 0) AS views,
    COALESCE(ec.likes, 0) AS likes,
    COALESCE(ec.shares, 0) AS shares,
    a.commentsCount AS comments,
    date_diff ('hour', a.createdAt, now ()) AS hours_since_publication,
    (
      (COALESCE(ec.views, 0) * 0.1) + (COALESCE(ec.likes, 0) * 1) + (COALESCE(ec.shares, 0) * 2) + (a.commentsCount * 1.5)
    ) / POWER(date_diff ('hour', a.createdAt, now ()) + 2, 1.5) AS score,
    a.tags
FROM
    articles a
    LEFT JOIN event_counts_post ec ON ec.post_id = a._id
