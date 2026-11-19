package com.lit.fire.flame;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SentimentAnalysis {

    // Without an explicit timeout, a request the LLM server accepts but never responds to (e.g.
    // mid-crash or overloaded) blocks the calling thread forever - connectTimeout covers the TCP
    // handshake, socketTimeout covers waiting on a stalled response so callers fall through to the
    // existing 30s-sleep retry loop instead of hanging indefinitely.
    private static final RequestConfig HTTP_REQUEST_CONFIG = RequestConfig.custom()
            .setConnectTimeout(15000)
            .setSocketTimeout(120000)
            .build();

    private static final String DB_URL;
    private static final String USER;
    private static final String PASS;
    private static final String LLM_URL;
    private static final String PROMPT_MEDIA_MOVIE_TEMPLATE;
    private static final String PROMPT_MEDIA_MOVIE_INVALID_TEMPLATE;
    private static final String PROMPT_MEDIA_MOVIE_POSITIVE_TEMPLATE;
    private static final String PROMPT_MEDIA_MOVIE_NEGATIVE_TEMPLATE;
    private static final String PROMPT_MEDIA_MOVIE_NEUTRAL_TEMPLATE;
    private static final String PROMPT_MEDIA_CELEBRITY_INVALID_TEMPLATE;
    private static final String PROMPT_MEDIA_CELEBRITY_POSITIVE_TEMPLATE;
    private static final String PROMPT_MEDIA_CELEBRITY_NEGATIVE_TEMPLATE;
    private static final String PROMPT_MEDIA_CELEBRITY_NEUTRAL_TEMPLATE;
    private static final String PROMPT_MEDIA_CELEBRITY_TEMPLATE;
    private static final String PROMPT_POLITICS_POLITICIAN_TEMPLATE;
    private static final String PROMPT_POLITICS_CAMPAIGN_TEMPLATE;
    private static final String PROMPT_POLITICS_PARTY_TEMPLATE;
    private static final String PROMPT_CLASSIFICATION_MOVIE_TEMPLATE;

    // How many LLM calls the scoring loop keeps in flight at once (llm.parallelism, default 8).
    // The cloud model answers ~1s per call and handled 16 concurrent calls without a 429 in
    // testing; set it to 1 for the old one-at-a-time behaviour.
    private static final int LLM_PARALLELISM;
    private static final ExecutorService LLM_POOL;

    static {
        Properties props = new Properties();
        try (InputStream input = SentimentAnalysis.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                System.out.println("Sorry, unable to find application.properties");
                throw new RuntimeException("application.properties not found in the classpath");
            }
            props.load(input);
            DB_URL = props.getProperty("db.url");
            USER = props.getProperty("db.user");
            PASS = props.getProperty("db.password");
            LLM_URL = props.getProperty("llm.url");
            PROMPT_MEDIA_MOVIE_TEMPLATE = props.getProperty("prompt.media.movie");
            PROMPT_MEDIA_MOVIE_INVALID_TEMPLATE = props.getProperty("prompt.media.movie.invalid");
            PROMPT_MEDIA_MOVIE_POSITIVE_TEMPLATE = props.getProperty("prompt.media.movie.positive");
            PROMPT_MEDIA_MOVIE_NEGATIVE_TEMPLATE = props.getProperty("prompt.media.movie.negative");
            PROMPT_MEDIA_MOVIE_NEUTRAL_TEMPLATE = props.getProperty("prompt.media.movie.neutral");
            PROMPT_MEDIA_CELEBRITY_INVALID_TEMPLATE = props.getProperty("prompt.media.celebrity.invalid");
            PROMPT_MEDIA_CELEBRITY_POSITIVE_TEMPLATE = props.getProperty("prompt.media.celebrity.positive");
            PROMPT_MEDIA_CELEBRITY_NEGATIVE_TEMPLATE = props.getProperty("prompt.media.celebrity.negative");
            PROMPT_MEDIA_CELEBRITY_NEUTRAL_TEMPLATE = props.getProperty("prompt.media.celebrity.neutral");
            PROMPT_MEDIA_CELEBRITY_TEMPLATE = props.getProperty("prompt.media.celebrity");
            PROMPT_POLITICS_POLITICIAN_TEMPLATE = props.getProperty("prompt.politics.politician");
            PROMPT_POLITICS_CAMPAIGN_TEMPLATE = props.getProperty("prompt.politics.campaign");
            PROMPT_POLITICS_PARTY_TEMPLATE = props.getProperty("prompt.politics.party");
            PROMPT_CLASSIFICATION_MOVIE_TEMPLATE = props.getProperty("prompt.classification.movie");
            LLM_PARALLELISM = Math.max(1, Integer.parseInt(props.getProperty("llm.parallelism", "8").trim()));
            LLM_POOL = Executors.newFixedThreadPool(LLM_PARALLELISM, r -> {
                Thread t = new Thread(r, "llm-call");
                t.setDaemon(true);
                return t;
            });
        } catch (IOException ex) {
            throw new RuntimeException("Error loading application.properties", ex);
        }
    }

    // Max rows each sentiment/classification step handles per loop pass, so every step and table
    // gets a turn every pass instead of one step draining a huge backlog first.
    private static final int PASS_LIMIT = 200;

    // Each sentiment prompt (the initial category prompt, then the .invalid/.positive/.negative/
    // .neutral refinement prompts) is applied to a row at most once. The names of the prompts that
    // have already run are recorded in the row's sentiment_passes array, and every pass skips rows
    // that already carry its name - so restarting the service doesn't re-run the LLM over rows it
    // has already scored. Clearing sentiment_passes (e.g. to '{}') re-queues a row for every pass.
    private static final String PASS_INITIAL = "initial";
    private static final String PASS_INVALID = "invalid";
    private static final String PASS_POSITIVE = "positive";
    private static final String PASS_NEGATIVE = "negative";
    private static final String PASS_NEUTRAL = "neutral";
    private static final String[] SENTIMENT_TABLES = {"x_posts", "instagram_posts", "youtube_comments", "reddit_posts"};

    public static void main(String[] args) {
        // Must finish before any pass starts, since every pass query filters on sentiment_passes.
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
            for (String table : SENTIMENT_TABLES) {
                ensureSentimentPassesColumn(conn, table);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to set up sentiment_passes tracking column", e);
        }

        ExecutorService executor = Executors.newFixedThreadPool(5);
        try {
            executor.submit(() -> {
                // Poll continuously: drain both tables, wait, then re-check for rows that
                // data collection inserted in the meantime. Runs until the process is stopped.
                // A dropped connection (e.g. Postgres restarting) reconnects after a minute instead
                // of ending the loop, so the service keeps scoring new posts unattended.
                long pollIntervalMillis = TimeUnit.MINUTES.toMillis(5);
                long reconnectDelayMillis = TimeUnit.MINUTES.toMillis(1);
                while (!Thread.currentThread().isInterrupted()) {
                    try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
                        while (!Thread.currentThread().isInterrupted()) {
                            markRetweets(conn);
                            // Rows sent for initial scoring this pass. Classification isn't counted:
                            // a row it can't classify stays eligible and would keep the loop spinning.
                            int scored = 0;
                            scored += processTable(conn, "x_posts", "created_at");
                            processClassification(conn, "x_posts", "created_at");
                            scored += processTable(conn, "instagram_posts", "timestamp");
                            processClassification(conn, "instagram_posts", "timestamp");
                            scored += processTableYoutube(conn, "published_at");
                            processClassificationYoutube(conn, "published_at");
                            scored += processRedditPosts(conn);
                            processClassificationReddit(conn, "created_at");
                            // Go straight into the next pass while there is a backlog; only wait
                            // for new posts once a pass found nothing left to score.
                            if (scored == 0) {
                                Thread.sleep(pollIntervalMillis);
                            }
                        }
                    } catch (SQLException | RuntimeException e) {
                        // RuntimeException too: an unexpected bug in one pass would otherwise end
                        // this task silently (the executor swallows it) and scoring would stop.
                        System.err.println("Error in the scoring loop: " + e
                                + ". Reconnecting in " + TimeUnit.MILLISECONDS.toSeconds(reconnectDelayMillis) + "s...");
                        e.printStackTrace();
                        try {
                            Thread.sleep(reconnectDelayMillis);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
//            executor.submit(() -> {
//                try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
//                    processTable(conn, "instagram_posts");
//                } catch (SQLException e) {
//                    System.err.println("Error processing instagram_posts: " + e.getMessage());
//                }
//            });
//            executor.submit(() -> {
//                try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
//                    processTable(conn, "youtube_comments");
//                } catch (SQLException e) {
//                    System.err.println("Error processing youtube_comments: " + e.getMessage());
//                }
//            });
//            executor.submit(() -> {
//                try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
//                    processRedditPosts(conn);
//                } catch (SQLException e) {
//                    System.err.println("Error processing reddit_posts: " + e.getMessage());
//                }
//            });
            executor.submit(() -> {
                try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
                    processTableFilterInvalidPosts(conn, "x_posts", "created_at");
                    processTableFilterPositivePosts(conn, "x_posts", "created_at");
                    processTableFilterNegativePosts(conn, "x_posts", "created_at");
                    processTableFilterNeutralPosts(conn, "x_posts", "created_at");
                } catch (SQLException e) {
                    System.err.println("Error processing x_posts: " + e.getMessage());
                }
            });
//            executor.submit(() -> {
//                try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
//                    processTableFilterPositivePosts(conn, "x_posts");
//                } catch (SQLException e) {
//                    System.err.println("Error processing x_posts: " + e.getMessage());
//                }
//            });
//            executor.submit(() -> {
//                try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
//                    processTableFilterNegativePosts(conn, "x_posts");
//                } catch (SQLException e) {
//                    System.err.println("Error processing x_posts: " + e.getMessage());
//                }
//            });
//            executor.submit(() -> {
//                try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
//                    processTableFilterNeutralPosts(conn, "x_posts");
//                } catch (SQLException e) {
//                    System.err.println("Error processing x_posts: " + e.getMessage());
//                }
//            });
            executor.submit(() -> {
                try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
                    processTableFilterInvalidPosts(conn, "youtube_comments", "published_at");
                    processTableFilterPositivePosts(conn, "youtube_comments", "published_at");
                    processTableFilterNegativePosts(conn, "youtube_comments", "published_at");
                    processTableFilterNeutralPosts(conn, "youtube_comments", "published_at");
                } catch (SQLException e) {
                    System.err.println("Error processing x_posts: " + e.getMessage());
                }
            });
            executor.submit(() -> {
                try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
                    processTableFilterInvalidPosts(conn, "instagram_posts", "timestamp");
                    processTableFilterPositivePosts(conn, "instagram_posts", "timestamp");
                    processTableFilterNegativePosts(conn, "instagram_posts", "timestamp");
                    processTableFilterNeutralPosts(conn, "instagram_posts", "timestamp");
                } catch (SQLException e) {
                    System.err.println("Error processing instagram_posts: " + e.getMessage());
                }
            });
            executor.submit(() -> {
                try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
                    processRedditPostsFilterInvalidPosts(conn, "created_at");
                    processRedditPostsFilterPositivePosts(conn, "created_at");
                    processRedditPostsFilterNegativePosts(conn, "created_at");
                    processRedditPostsFilterNeutralPosts(conn, "created_at");
                } catch (SQLException e) {
                    System.err.println("Error processing reddit_posts: " + e.getMessage());
                }
            });
        } finally {
            // shutdown() only stops new submissions: the one-shot refinement tasks finish on their
            // own, and the polling task above runs until the process is stopped. Previously this
            // waited 60 minutes and then called shutdownNow(), which interrupted the polling loop
            // and made the whole service exit after about an hour.
            executor.shutdown();
            try {
                while (!executor.awaitTermination(1, TimeUnit.DAYS)) {
                    // keep waiting - the polling loop is meant to run indefinitely
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    @FunctionalInterface
    private interface LlmCall<T> {
        T call(String[] row) throws IOException;
    }

    @FunctionalInterface
    private interface RowWriter<T> {
        void write(String[] row, T result) throws SQLException;
    }

    // Runs the LLM call for every row on LLM_POOL (LLM_PARALLELISM at a time) and writes each result
    // on the calling thread as soon as it arrives, so the shared JDBC connection is only ever used by
    // one thread. A row whose call fails (after callSentimentApi's own retries) is logged and
    // skipped without writing anything, exactly as the old sequential loops did.
    private static <T> void runLlmBatch(String label, List<String[]> rows, LlmCall<T> call, RowWriter<T> writer)
            throws SQLException {
        CompletionService<T> completion = new ExecutorCompletionService<>(LLM_POOL);
        Map<Future<T>, String[]> rowOf = new IdentityHashMap<>();
        for (String[] row : rows) {
            rowOf.put(completion.submit(() -> call.call(row)), row);
        }
        try {
            for (int i = 0; i < rows.size(); i++) {
                Future<T> done = completion.take();
                String[] row = rowOf.get(done);
                try {
                    writer.write(row, done.get());
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    System.err.println("Error calling LLM for " + label + " ID: " + row[0] + ". "
                            + (cause instanceof IOException ? cause.getMessage() : String.valueOf(cause)));
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            // Only does anything if we bailed out early (interrupt or a DB error while writing).
            for (Future<T> f : rowOf.keySet()) {
                f.cancel(true);
            }
        }
    }

    // Adds the sentiment_passes column the first time the service runs against a table. Rows that
    // already have a sentiment_score were scored by the initial prompt before tracking existed, so
    // they're backfilled as having had that pass; nothing is known about which refinement prompts
    // already ran on them, so those each run once more and are then recorded.
    private static void ensureSentimentPassesColumn(Connection conn, String tableName) throws SQLException {
        String existsSql = "SELECT 1 FROM information_schema.columns" +
                " WHERE table_schema = current_schema() AND table_name = ? AND column_name = 'sentiment_passes'";
        try (PreparedStatement pstmt = conn.prepareStatement(existsSql)) {
            pstmt.setString(1, tableName);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return;
                }
            }
        }

        boolean autoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("ALTER TABLE " + tableName + " ADD COLUMN sentiment_passes TEXT[] NOT NULL DEFAULT '{}'");
            int backfilled = stmt.executeUpdate("UPDATE " + tableName + " SET sentiment_passes = ARRAY['" + PASS_INITIAL + "']" +
                    " WHERE sentiment_score IS NOT NULL");
            conn.commit();
            System.out.println("Added sentiment_passes to " + tableName + "; marked " + backfilled + " already-scored rows as initial-pass done");
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(autoCommit);
        }
    }

    // Called once a prompt has produced a result for the row (whether or not that result changed the
    // stored score), so the same prompt is never run on the row again. Not called when the LLM call
    // itself failed (IOException), so those rows are retried on a later pass.
    private static void markPassDone(Connection conn, String tableName, String id, String pass) throws SQLException {
        String sql = "UPDATE " + tableName + " SET sentiment_passes = array_append(sentiment_passes, ?)" +
                " WHERE id = ? AND NOT (? = ANY(sentiment_passes))";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, pass);
            pstmt.setString(2, id);
            pstmt.setString(3, pass);
            pstmt.executeUpdate();
        }
    }

    // Retweets carry a deterministic "RT @user: " prefix in the raw text, so this is checked
    // directly in SQL rather than burning an LLM call on it.
    private static void markRetweets(Connection conn) throws SQLException {
        String sql = "UPDATE x_posts SET is_retweet = true WHERE text LIKE 'RT @%' AND is_retweet = false";
        try (Statement stmt = conn.createStatement()) {
            int updated = stmt.executeUpdate(sql);
            if (updated > 0) {
                System.out.println("Marked " + updated + " x_posts rows as retweets");
            }
        }
    }

    private static int processRedditPosts(Connection conn) throws SQLException {
        // Newest first and capped like the other tables, so a big Reddit backlog doesn't hold up
        // the rest of the loop.
        String sql = "SELECT id, title, text, keyword, sentiment_category FROM reddit_posts" +
                " WHERE keyword IS NOT NULL AND TRIM(keyword) <> ''" +
                " AND NOT ('" + PASS_INITIAL + "' = ANY(sentiment_passes))" +
                " ORDER BY created_at DESC NULLS LAST LIMIT " + PASS_LIMIT;
        List<String[]> batch = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                batch.add(new String[]{
                        rs.getString("id"),
                        rs.getString("title"),
                        rs.getString("text"),
                        rs.getString("keyword"),
                        rs.getString("sentiment_category")
                });
            }
        }

        runLlmBatch("reddit_posts sentiment", batch,
                row -> getRedditSentiment(row[1], row[2], row[3], row[4]),
                (row, sentimentResponse) -> {
                    System.out.println("Updating sentiment score for id: " + row[0] + ", keyword: " + row[3] + ", score: " + sentimentResponse.getPositivityScore());
                    updateSentimentScore(conn, "reddit_posts", row[0], sentimentResponse);
                    markPassDone(conn, "reddit_posts", row[0], PASS_INITIAL);
                });
        return batch.size();
    }

    // Reddit posts carry a separate title and body, so score both and average them rather
    // than scoring a single text column like the other platforms.
    private static SentimentResponse getRedditSentiment(String title, String text, String keyword, String category) throws IOException {
        if (text != null && !text.trim().isEmpty()) {
            SentimentResponse titleSentiment = getAverageSentimentScore(title, keyword, category);
            SentimentResponse textSentiment = getAverageSentimentScore(text, keyword, category);
            SentimentResponse combined = new SentimentResponse();
            combined.setPositivityScore((titleSentiment.getPositivityScore() + textSentiment.getPositivityScore()) / 2);
            combined.setCategory(textSentiment.getCategory());
            combined.setIsPromotional(combinePromotional(titleSentiment.getIsPromotional(), textSentiment.getIsPromotional()));
            return combined;
        }
        return getAverageSentimentScore(title, keyword, category);
    }

    // Either half being promotional (e.g. a promotional title with a neutral body, or vice
    // versa) is enough to mark the whole Reddit post as promotional.
    private static Boolean combinePromotional(Boolean a, Boolean b) {
        if (a == null && b == null) {
            return null;
        }
        return Boolean.TRUE.equals(a) || Boolean.TRUE.equals(b);
    }

    private static void processRedditPostsFilterInvalidPosts(Connection conn, String timestampColumn) throws SQLException {
        String sql = "SELECT id, title, text, keyword, sentiment_category FROM reddit_posts WHERE sentiment_score IS NOT NULL AND sentiment_score > 0" +
                " AND NOT ('" + PASS_INVALID + "' = ANY(sentiment_passes))" +
                " ORDER BY " + timestampColumn + " DESC NULLS LAST";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String id = rs.getString("id");
                String title = rs.getString("title");
                String text = rs.getString("text");
                String keyword = rs.getString("keyword");
                String category = rs.getString("sentiment_category");

                if (keyword != null && !keyword.trim().isEmpty()) {
                    try {
                        String tempCategory = category + ".invalid";
                        SentimentResponse sentimentResponse = getRedditSentiment(title, text, keyword, tempCategory);
                        System.out.println("Updating sentiment score for id (invalid posts): " + id + ", keyword: " + keyword + ", score: " + sentimentResponse.getPositivityScore());
                        if (sentimentResponse.getPositivityScore() <= 0) {
                            sentimentResponse.setCategory(category);
                            updateSentimentScore(conn, "reddit_posts", id, sentimentResponse);
                        }
                        markPassDone(conn, "reddit_posts", id, PASS_INVALID);
                    } catch (IOException e) {
                        System.err.println("Error calling sentiment analysis API for reddit_posts ID: " + id + ". " + e.getMessage());
                    }
                }
            }
        }
    }

    private static void processRedditPostsFilterPositivePosts(Connection conn, String timestampColumn) throws SQLException {
        String sql = "SELECT id, title, text, keyword, sentiment_category FROM reddit_posts WHERE sentiment_score IS NOT NULL AND sentiment_score >= 75" +
                " AND NOT ('" + PASS_POSITIVE + "' = ANY(sentiment_passes))" +
                " ORDER BY " + timestampColumn + " DESC NULLS LAST";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String id = rs.getString("id");
                String title = rs.getString("title");
                String text = rs.getString("text");
                String keyword = rs.getString("keyword");
                String category = rs.getString("sentiment_category");

                if (keyword != null && !keyword.trim().isEmpty()) {
                    try {
                        String tempCategory = category + ".positive";
                        SentimentResponse sentimentResponse = getRedditSentiment(title, text, keyword, tempCategory);
                        System.out.println("Updating sentiment score for id (positive posts): " + id + ", keyword: " + keyword + ", score: " + sentimentResponse.getPositivityScore());
                        if (sentimentResponse.getPositivityScore() >= 75) {
                            sentimentResponse.setCategory(category);
                            updateSentimentScore(conn, "reddit_posts", id, sentimentResponse);
                        }
                        markPassDone(conn, "reddit_posts", id, PASS_POSITIVE);
                    } catch (IOException e) {
                        System.err.println("Error calling sentiment analysis API for reddit_posts ID: " + id + ". " + e.getMessage());
                    }
                }
            }
        }
    }

    private static void processRedditPostsFilterNegativePosts(Connection conn, String timestampColumn) throws SQLException {
        String sql = "SELECT id, title, text, keyword, sentiment_category FROM reddit_posts WHERE sentiment_score IS NOT NULL AND sentiment_score <= 50" +
                " AND NOT ('" + PASS_NEGATIVE + "' = ANY(sentiment_passes))" +
                " ORDER BY " + timestampColumn + " DESC NULLS LAST";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String id = rs.getString("id");
                String title = rs.getString("title");
                String text = rs.getString("text");
                String keyword = rs.getString("keyword");
                String category = rs.getString("sentiment_category");

                if (keyword != null && !keyword.trim().isEmpty()) {
                    try {
                        String tempCategory = category + ".negative";
                        SentimentResponse sentimentResponse = getRedditSentiment(title, text, keyword, tempCategory);
                        System.out.println("Updating sentiment score for id (negative posts): " + id + ", keyword: " + keyword + ", score: " + sentimentResponse.getPositivityScore());
                        if (sentimentResponse.getPositivityScore() <= 50) {
                            sentimentResponse.setCategory(category);
                            updateSentimentScore(conn, "reddit_posts", id, sentimentResponse);
                        }
                        markPassDone(conn, "reddit_posts", id, PASS_NEGATIVE);
                    } catch (IOException e) {
                        System.err.println("Error calling sentiment analysis API for reddit_posts ID: " + id + ". " + e.getMessage());
                    }
                }
            }
        }
    }

    private static void processRedditPostsFilterNeutralPosts(Connection conn, String timestampColumn) throws SQLException {
        String sql = "SELECT id, title, text, keyword, sentiment_category FROM reddit_posts WHERE sentiment_score IS NOT NULL AND sentiment_score > 50 AND sentiment_score < 75" +
                " AND NOT ('" + PASS_NEUTRAL + "' = ANY(sentiment_passes))" +
                " ORDER BY " + timestampColumn + " DESC NULLS LAST";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String id = rs.getString("id");
                String title = rs.getString("title");
                String text = rs.getString("text");
                String keyword = rs.getString("keyword");
                String category = rs.getString("sentiment_category");

                if (keyword != null && !keyword.trim().isEmpty()) {
                    try {
                        String tempCategory = category + ".neutral";
                        SentimentResponse sentimentResponse = getRedditSentiment(title, text, keyword, tempCategory);
                        System.out.println("Updating sentiment score for id (neutral posts): " + id + ", keyword: " + keyword + ", score: " + sentimentResponse.getPositivityScore());
                        if (sentimentResponse.getPositivityScore() > 50 && sentimentResponse.getPositivityScore() < 75) {
                            sentimentResponse.setCategory(category);
                            updateSentimentScore(conn, "reddit_posts", id, sentimentResponse);
                        }
                        markPassDone(conn, "reddit_posts", id, PASS_NEUTRAL);
                    } catch (IOException e) {
                        System.err.println("Error calling sentiment analysis API for reddit_posts ID: " + id + ". " + e.getMessage());
                    }
                }
            }
        }
    }

    private static int processTable(Connection conn, String tableName, String timestampColumn) throws SQLException {
        // Only score rows the initial prompt hasn't run on yet, newest timestamp first, in a capped
        // pass so classification still gets a turn. Rows already scored (including those scored 0)
        // are never re-sent to the LLM - see sentiment_passes.
        String sql = "SELECT id, text, keyword, sentiment_category FROM " + tableName +
                " WHERE text IS NOT NULL AND TRIM(text) <> ''" +
                " AND keyword IS NOT NULL AND TRIM(keyword) <> ''" +
                " AND NOT ('" + PASS_INITIAL + "' = ANY(sentiment_passes))" +
                " ORDER BY " + timestampColumn + " DESC NULLS LAST LIMIT " + PASS_LIMIT;

        // Buffer into memory and close the ResultSet before issuing updates.
        java.util.List<String[]> batch = new java.util.ArrayList<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                batch.add(new String[]{
                        rs.getString("id"),
                        rs.getString("text"),
                        rs.getString("keyword"),
                        rs.getString("sentiment_category")
                });
            }
        }

        runLlmBatch(tableName + " sentiment", batch,
                row -> getAverageSentimentScore(row[1], row[2], row[3]),
                (row, sentimentResponse) -> {
                    System.out.println("Updating sentiment score for id (generic): " + row[0] + ", keyword: " + row[2] + ", score: " + sentimentResponse.getPositivityScore());
                    updateSentimentScore(conn, tableName, row[0], sentimentResponse);
                    markPassDone(conn, tableName, row[0], PASS_INITIAL);
                });
        return batch.size();
    }

    // YouTube comments are replies to a video, not self-contained posts - a short reaction like
    // "Super" or "can't wait!" has no keyword or cinematic anchor of its own, so scoring/classifying
    // the comment text alone starves the LLM of the context it needs to tell a genuine fan reaction
    // apart from an unrelated comment that happened to be scraped under this keyword. Prefixing the
    // video title (mirrors how reddit's title+text combination gives the LLM the same kind of anchor).
    private static int processTableYoutube(Connection conn, String timestampColumn) throws SQLException {
        String sql = "SELECT id, text, video_title, keyword, sentiment_category FROM youtube_comments" +
                " WHERE text IS NOT NULL AND TRIM(text) <> ''" +
                " AND keyword IS NOT NULL AND TRIM(keyword) <> ''" +
                " AND NOT ('" + PASS_INITIAL + "' = ANY(sentiment_passes))" +
                " ORDER BY " + timestampColumn + " DESC NULLS LAST LIMIT " + PASS_LIMIT;

        java.util.List<String[]> batch = new java.util.ArrayList<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                batch.add(new String[]{
                        rs.getString("id"),
                        rs.getString("text"),
                        rs.getString("video_title"),
                        rs.getString("keyword"),
                        rs.getString("sentiment_category")
                });
            }
        }

        runLlmBatch("youtube_comments sentiment", batch,
                row -> {
                    String videoTitle = row[2];
                    String combinedText = (videoTitle != null && !videoTitle.trim().isEmpty()) ? videoTitle + "\n" + row[1] : row[1];
                    return getAverageSentimentScore(combinedText, row[3], row[4]);
                },
                (row, sentimentResponse) -> {
                    System.out.println("Updating sentiment score for youtube_comments id: " + row[0] + ", keyword: " + row[3] + ", score: " + sentimentResponse.getPositivityScore());
                    updateSentimentScore(conn, "youtube_comments", row[0], sentimentResponse);
                    markPassDone(conn, "youtube_comments", row[0], PASS_INITIAL);
                });
        return batch.size();
    }

    private static void processClassificationYoutube(Connection conn, String timestampColumn) throws SQLException {
        String sql = "SELECT id, text, video_title, keyword, author FROM youtube_comments" +
                " WHERE text IS NOT NULL AND TRIM(text) <> ''" +
                " AND keyword IS NOT NULL AND TRIM(keyword) <> ''" +
                " AND sentiment_category = 'media.movie'" +
                " AND author_type IS NULL" +
                // Only relevant rows (score > 0) reach mentions; capped so the loop keeps cycling.
                " AND sentiment_score > 0" +
                " ORDER BY " + timestampColumn + " DESC NULLS LAST LIMIT " + PASS_LIMIT;
        List<String[]> batch = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String text = rs.getString("text");
                String videoTitle = rs.getString("video_title");
                batch.add(new String[]{
                        rs.getString("id"),
                        (videoTitle != null && !videoTitle.trim().isEmpty()) ? videoTitle + "\n" + text : text,
                        rs.getString("keyword"),
                        rs.getString("author")
                });
            }
        }

        runLlmBatch("youtube_comments classification", batch,
                row -> getContentClassification(row[1], row[2], row[3]),
                (row, classificationResponse) -> {
                    System.out.println("Updating classification for youtube_comments id: " + row[0] + ", author_type: " + classificationResponse.getAuthorType());
                    updateClassification(conn, "youtube_comments", row[0], classificationResponse);
                });
    }

    // Classification (author_type/content_intent/predicted_region/topic_category) is only defined
    // for media.movie today, and unlike sentiment it doesn't need periodic recomputation, so this
    // only processes rows that haven't been classified yet.
    private static void processClassification(Connection conn, String tableName, String timestampColumn) throws SQLException {
        String sql = "SELECT id, text, keyword, author FROM " + tableName +
                " WHERE text IS NOT NULL AND TRIM(text) <> ''" +
                " AND keyword IS NOT NULL AND TRIM(keyword) <> ''" +
                " AND sentiment_category = 'media.movie'" +
                " AND author_type IS NULL" +
                // Only relevant rows (score > 0) reach mentions; capped so the loop keeps cycling.
                " AND sentiment_score > 0" +
                " ORDER BY " + timestampColumn + " DESC NULLS LAST LIMIT " + PASS_LIMIT;
        List<String[]> batch = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                batch.add(new String[]{
                        rs.getString("id"),
                        rs.getString("text"),
                        rs.getString("keyword"),
                        rs.getString("author")
                });
            }
        }

        runLlmBatch(tableName + " classification", batch,
                row -> getContentClassification(row[1], row[2], row[3]),
                (row, classificationResponse) -> {
                    System.out.println("Updating classification for id: " + row[0] + ", author_type: " + classificationResponse.getAuthorType());
                    updateClassification(conn, tableName, row[0], classificationResponse);
                });
    }

    private static void processClassificationReddit(Connection conn, String timestampColumn) throws SQLException {
        String sql = "SELECT id, title, text, keyword, author FROM reddit_posts" +
                " WHERE keyword IS NOT NULL AND TRIM(keyword) <> ''" +
                " AND sentiment_category = 'media.movie'" +
                " AND author_type IS NULL" +
                // Only relevant rows (score > 0) reach mentions; capped so the loop keeps cycling.
                " AND sentiment_score > 0" +
                " ORDER BY " + timestampColumn + " DESC NULLS LAST LIMIT " + PASS_LIMIT;
        List<String[]> batch = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String title = rs.getString("title");
                String text = rs.getString("text");
                batch.add(new String[]{
                        rs.getString("id"),
                        (text != null && !text.trim().isEmpty()) ? title + "\n" + text : title,
                        rs.getString("keyword"),
                        rs.getString("author")
                });
            }
        }

        runLlmBatch("reddit_posts classification", batch,
                row -> getContentClassification(row[1], row[2], row[3]),
                (row, classificationResponse) -> {
                    System.out.println("Updating classification for reddit_posts id: " + row[0] + ", author_type: " + classificationResponse.getAuthorType());
                    updateClassification(conn, "reddit_posts", row[0], classificationResponse);
                });
    }

    private static ClassificationResponse getContentClassification(String text, String keyword, String author) throws IOException {
        return callClassificationApi(PROMPT_CLASSIFICATION_MOVIE_TEMPLATE, text, keyword, author);
    }

    private static ClassificationResponse callClassificationApi(String promptTemplate, String text, String keyword, String author) throws IOException {
        int maxRetries = 20;
        int retryCount = 0;
        int proseReplies = 0;
        while (true) {
            try (CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(HTTP_REQUEST_CONFIG).build()) {
                HttpPost httpPost = new HttpPost(LLM_URL);
                Gson gson = new Gson();

                if (promptTemplate == null) {
                    System.err.println("Classification prompt template is null for keyword: " + keyword);
                    return new ClassificationResponse();
                }

                String promptString = promptTemplate.replace("{keyword}", keyword)
                        .replace("{text}", text)
                        .replace("{author}", author != null ? author : "");
                PromptRequest payload = new PromptRequest(promptString);
                String jsonPayload = gson.toJson(payload);

                StringEntity entity = new StringEntity(jsonPayload, "UTF-8");
                httpPost.setEntity(entity);
                httpPost.setHeader("Accept", "application/json");
                httpPost.setHeader("Content-type", "application/json; charset=UTF-8");

                try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                    String responseString = EntityUtils.toString(response.getEntity());
                    System.out.println("Raw classification response from LLM: " + responseString);
                    int firstBrace = responseString.indexOf('{');
                    int lastBrace = responseString.lastIndexOf('}');

                    if (firstBrace == -1 || lastBrace == -1 || lastBrace <= firstBrace) {
                        if (isServiceError(response.getStatusLine().getStatusCode(), responseString)) {
                            // An error body (e.g. "Error from Ollama: 429"), not a classification - retry
                            // and, failing that, let the caller skip the row so it is classified later.
                            throw new IOException("LLM returned no JSON for classification (HTTP "
                                    + response.getStatusLine().getStatusCode() + "): " + abbreviate(responseString));
                        }
                        // A prose reply about the post itself: ask again quickly, then give up the
                        // same way as unparseable JSON below.
                        if (++proseReplies < MAX_PROSE_REPLIES) {
                            continue;
                        }
                        System.err.println("LLM replied without JSON " + MAX_PROSE_REPLIES + " times for classification: "
                                + abbreviate(responseString));
                        return new ClassificationResponse();
                    }

                    String jsonResponse = responseString.substring(firstBrace, lastBrace + 1);
                    try {
                        ClassificationResponse classificationResponse = gson.fromJson(jsonResponse, ClassificationResponse.class);
                        return classificationResponse != null ? classificationResponse : new ClassificationResponse();
                    } catch (JsonSyntaxException e) {
                        System.err.println("Failed to parse extracted classification JSON: " + jsonResponse);
                        return new ClassificationResponse();
                    }
                }
            } catch (IOException e) {
                if (++retryCount >= maxRetries) {
                    throw e;
                }
                System.err.println("LLM call failed (" + e.getMessage() + "). Retry " + retryCount + "/" + maxRetries + " in 30 seconds...");
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting to retry", ie);
                }
            }
        }
    }

    private static void updateClassification(Connection conn, String tableName, String id, ClassificationResponse response) throws SQLException {
        String sql = "UPDATE " + tableName + " SET author_type = ?, content_intent = ?, predicted_region = ?, topic_category = ? WHERE id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, response.getAuthorType());
            pstmt.setString(2, response.getContentIntent());
            pstmt.setString(3, response.getPredictedRegion());
            pstmt.setString(4, response.getTopicCategory());
            pstmt.setString(5, id);
            pstmt.executeUpdate();
        }
    }

    private static void processTableFilterInvalidPosts(Connection conn, String tableName, String timestampColumn) throws SQLException {
        String sql = "SELECT id, text, keyword, sentiment_category FROM " + tableName + " WHERE sentiment_score IS NOT NULL AND sentiment_score > 0" +
                " AND NOT ('" + PASS_INVALID + "' = ANY(sentiment_passes))" +
                " ORDER BY " + timestampColumn + " DESC NULLS LAST";//v2
//        String sql = "SELECT id, text, keyword, sentiment_category FROM " + tableName + " WHERE keyword = 'parasakthi' AND text LIKE '%oast%'";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String id = rs.getString("id");
                String text = rs.getString("text");
                String keyword = rs.getString("keyword");
                String category = rs.getString("sentiment_category");

                if (text != null && !text.trim().isEmpty() && keyword != null && !keyword.trim().isEmpty()) {
                    try {
                        String tempCategory = category + ".invalid";
                        SentimentResponse sentimentResponse = getAverageSentimentScore(text, keyword, tempCategory);
                        System.out.println("Updating sentiment score for id (invalid posts): " + id + ", keyword: " + keyword + ", score: " + sentimentResponse.getPositivityScore());
                        if (sentimentResponse.getPositivityScore() <= 0) {
                            sentimentResponse.setCategory(category);
                            updateSentimentScore(conn, tableName, id, sentimentResponse);
                        }
                        markPassDone(conn, tableName, id, PASS_INVALID);
                    } catch (IOException e) {
                        System.err.println("Error calling sentiment analysis API for " + tableName + " ID: " + id + ". " + e.getMessage());
                    }
                }
            }
        }
    }

    private static void processTableFilterPositivePosts(Connection conn, String tableName, String timestampColumn) throws SQLException {
        String sql = "SELECT id, text, keyword, sentiment_category FROM " + tableName + " WHERE sentiment_score IS NOT NULL AND sentiment_score >= 75" +
                " AND NOT ('" + PASS_POSITIVE + "' = ANY(sentiment_passes))" +
                " ORDER BY " + timestampColumn + " DESC NULLS LAST";//v2
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String id = rs.getString("id");
                String text = rs.getString("text");
                String keyword = rs.getString("keyword");
                String category = rs.getString("sentiment_category");

                if (text != null && !text.trim().isEmpty() && keyword != null && !keyword.trim().isEmpty()) {
                    try {
                        String tempCategory = category + ".positive";
                        SentimentResponse sentimentResponse = getAverageSentimentScore(text, keyword, tempCategory);
                        System.out.println("Updating sentiment score for id (positive posts): " + id + ", keyword: " + keyword + ", score: " + sentimentResponse.getPositivityScore());
                        if (sentimentResponse.getPositivityScore() >= 75) {
                            sentimentResponse.setCategory(category);
                            updateSentimentScore(conn, tableName, id, sentimentResponse);
                        }
                        markPassDone(conn, tableName, id, PASS_POSITIVE);
                    } catch (IOException e) {
                        System.err.println("Error calling sentiment analysis API for " + tableName + " ID: " + id + ". " + e.getMessage());
                    }
                }
            }
        }
    }

    private static void processTableFilterNegativePosts(Connection conn, String tableName, String timestampColumn) throws SQLException {
        String sql = "SELECT id, text, keyword, sentiment_category FROM " + tableName + " WHERE sentiment_score IS NOT NULL AND sentiment_score <= 50" +
                " AND NOT ('" + PASS_NEGATIVE + "' = ANY(sentiment_passes))" +
                " ORDER BY " + timestampColumn + " DESC NULLS LAST";//v2
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String id = rs.getString("id");
                String text = rs.getString("text");
                String keyword = rs.getString("keyword");
                String category = rs.getString("sentiment_category");

                if (text != null && !text.trim().isEmpty() && keyword != null && !keyword.trim().isEmpty()) {
                    try {
                        String tempCategory = category + ".negative";
                        SentimentResponse sentimentResponse = getAverageSentimentScore(text, keyword, tempCategory);
                        System.out.println("Updating sentiment score for id (negative posts): " + id + ", keyword: " + keyword + ", score: " + sentimentResponse.getPositivityScore());
                        if (sentimentResponse.getPositivityScore() <= 50) {
                            sentimentResponse.setCategory(category);
                            updateSentimentScore(conn, tableName, id, sentimentResponse);
                        }
                        markPassDone(conn, tableName, id, PASS_NEGATIVE);
                    } catch (IOException e) {
                        System.err.println("Error calling sentiment analysis API for " + tableName + " ID: " + id + ". " + e.getMessage());
                    }
                }
            }
        }
    }

    private static void processTableFilterNeutralPosts(Connection conn, String tableName, String timestampColumn) throws SQLException {
        String sql = "SELECT id, text, keyword, sentiment_category FROM " + tableName + " WHERE sentiment_score IS NOT NULL AND sentiment_score > 50 AND sentiment_score < 75" +
                " AND NOT ('" + PASS_NEUTRAL + "' = ANY(sentiment_passes))" +
                " ORDER BY " + timestampColumn + " DESC NULLS LAST";//v2
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String id = rs.getString("id");
                String text = rs.getString("text");
                String keyword = rs.getString("keyword");
                String category = rs.getString("sentiment_category");

                if (text != null && !text.trim().isEmpty() && keyword != null && !keyword.trim().isEmpty()) {
                    try {
                        String tempCategory = category + ".neutral";
                        SentimentResponse sentimentResponse = getAverageSentimentScore(text, keyword, tempCategory);
                        System.out.println("Updating sentiment score for id (neutral posts): " + id + ", keyword: " + keyword + ", score: " + sentimentResponse.getPositivityScore());
                        if (sentimentResponse.getPositivityScore() > 50 && sentimentResponse.getPositivityScore() < 75) {
                            sentimentResponse.setCategory(category);
                            updateSentimentScore(conn, tableName, id, sentimentResponse);
                        }
                        markPassDone(conn, tableName, id, PASS_NEUTRAL);
                    } catch (IOException e) {
                        System.err.println("Error calling sentiment analysis API for " + tableName + " ID: " + id + ". " + e.getMessage());
                    }
                }
            }
        }
    }

    private static SentimentResponse getAverageSentimentScore(String text, String keyword, String category) throws IOException {
        SentimentResponse sentimentResponse = new SentimentResponse();
        HashMap<String, String> prompts = new HashMap<>();
        prompts.put("media.movie", PROMPT_MEDIA_MOVIE_TEMPLATE);
        prompts.put("media.movie.invalid", PROMPT_MEDIA_MOVIE_INVALID_TEMPLATE);
        prompts.put("media.movie.positive", PROMPT_MEDIA_MOVIE_POSITIVE_TEMPLATE);
        prompts.put("media.movie.negative", PROMPT_MEDIA_MOVIE_NEGATIVE_TEMPLATE);
        prompts.put("media.movie.neutral", PROMPT_MEDIA_MOVIE_NEUTRAL_TEMPLATE);
        prompts.put("media.celebrity", PROMPT_MEDIA_CELEBRITY_TEMPLATE);
        prompts.put("media.celebrity.invalid", PROMPT_MEDIA_CELEBRITY_INVALID_TEMPLATE);
        prompts.put("media.celebrity.positive", PROMPT_MEDIA_CELEBRITY_POSITIVE_TEMPLATE);
        prompts.put("media.celebrity.negative", PROMPT_MEDIA_CELEBRITY_NEGATIVE_TEMPLATE);
        prompts.put("media.celebrity.neutral", PROMPT_MEDIA_CELEBRITY_NEUTRAL_TEMPLATE);
        prompts.put("politics.politician", PROMPT_POLITICS_POLITICIAN_TEMPLATE);
        prompts.put("politics.campaign", PROMPT_POLITICS_CAMPAIGN_TEMPLATE);
        prompts.put("politics.party", PROMPT_POLITICS_PARTY_TEMPLATE);

        if ((category == null) || (category.equalsIgnoreCase("invalid"))) {
            category = "media.movie";
        }

        String prompt = prompts.get(category);
        System.out.println("Category: " + category);
        if (prompt == null) {
            System.out.println("Prompt is null for : " + category + " for keyword : " + keyword);
            return sentimentResponse;
        }

        sentimentResponse = callSentimentApi(prompt, text, keyword);
        int score = (int) Math.round(sentimentResponse.getPositivityScore());

        if (score < 0) {
            return sentimentResponse;
        }

        SentimentResponse finalSentimentResponse = new SentimentResponse();
        finalSentimentResponse.setCategory(category);
        finalSentimentResponse.setPositivityScore(score);
        finalSentimentResponse.setIsPromotional(sentimentResponse.getIsPromotional());

        return finalSentimentResponse;
    }

    private static SentimentResponse callSentimentApi(String promptTemplate, String text, String keyword) throws IOException {
        int maxRetries = 20;
        int retryCount = 0;
        int proseReplies = 0;
        while (true) {
            try (CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(HTTP_REQUEST_CONFIG).build()) {
                HttpPost httpPost = new HttpPost(LLM_URL);
                Gson gson = new Gson();

                if (promptTemplate == null) {
                    System.err.println("Prompt template is null for keyword: " + keyword);
                    SentimentResponse errorResponse = new SentimentResponse();
                    errorResponse.setPositivityScore(-1.0);
                    return errorResponse;
                }

                String promptString = promptTemplate.replace("{keyword}", keyword).replace("{text}", text);
                PromptRequest payload = new PromptRequest(promptString);
                String jsonPayload = gson.toJson(payload);

                StringEntity entity = new StringEntity(jsonPayload, "UTF-8");
                httpPost.setEntity(entity);
                httpPost.setHeader("Accept", "application/json");
                httpPost.setHeader("Content-type", "application/json; charset=UTF-8");

                try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                    String responseString = EntityUtils.toString(response.getEntity());
                    System.out.println("Raw response from LLM: " + responseString);
                    SentimentResponse sentimentResponse;
                    String jsonResponse = null;
                    int firstBrace = responseString.indexOf('{');
                    int lastBrace = responseString.lastIndexOf('}');

                    if (firstBrace != -1 && lastBrace != -1 && lastBrace > firstBrace) {
                        jsonResponse = responseString.substring(firstBrace, lastBrace + 1);
                    }

                    if (jsonResponse == null) {
                        if (isServiceError(response.getStatusLine().getStatusCode(), responseString)) {
                            // The call failed (e.g. aura-llm relays "Error from Ollama: 429" with
                            // HTTP 200), which says nothing about the post. Throw so the retry loop
                            // below backs off and, if it keeps failing, the caller skips the row
                            // WITHOUT saving a score or marking the pass done.
                            throw new IOException("LLM returned no JSON (HTTP " + response.getStatusLine().getStatusCode()
                                    + "): " + abbreviate(responseString));
                        }
                        // The model answered in prose instead (e.g. "Please provide the input post..."
                        // for an emoji-only or link-only post). That's about the post itself and
                        // repeats on retry, so try a couple more times quickly, then treat it like
                        // unparseable JSON below (irrelevant) instead of 20 retries 30s apart.
                        if (++proseReplies < MAX_PROSE_REPLIES) {
                            System.err.println("LLM replied without JSON (" + abbreviate(responseString) + "). Asking again ("
                                    + proseReplies + "/" + MAX_PROSE_REPLIES + ")...");
                            continue;
                        }
                        System.err.println("LLM replied without JSON " + MAX_PROSE_REPLIES + " times for keyword " + keyword
                                + "; treating the post as irrelevant.");
                        SentimentResponse irrelevant = new SentimentResponse();
                        irrelevant.setPositivityScore(-1.0);
                        return irrelevant;
                    }

                    try {
                        sentimentResponse = gson.fromJson(jsonResponse, SentimentResponse.class);
                    } catch (JsonSyntaxException e) {
                        System.err.println("Failed to parse extracted JSON: " + jsonResponse);
                        sentimentResponse = new SentimentResponse();
                        sentimentResponse.setPositivityScore(-1.0);
                        return sentimentResponse;
                    }

                    if (sentimentResponse == null) {
                        System.err.println("Failed to parse JSON response: " + jsonResponse);
                        return new SentimentResponse();
                    }
                    return sentimentResponse;
                }
            } catch (IOException e) {
                if (++retryCount >= maxRetries) {
                    throw e;
                }
                System.err.println("LLM call failed (" + e.getMessage() + "). Retry " + retryCount + "/" + maxRetries + " in 30 seconds...");
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting to retry", ie);
                }
            }
        }
    }

    // Prose (non-JSON) replies allowed per post before it's treated as unparseable.
    private static final int MAX_PROSE_REPLIES = 3;

    // True when a no-JSON reply is a failed call rather than the model talking about the post:
    // a non-200 status, an empty body, or aura-llm's relayed error text ("Error from Ollama: 429",
    // "Internal Server Error: ...", "Bedrock Error: ...").
    private static boolean isServiceError(int statusCode, String body) {
        if (statusCode != 200 || body == null || body.isBlank()) {
            return true;
        }
        String b = body.trim();
        return b.startsWith("Error from") || b.startsWith("Internal Server Error") || b.startsWith("Bedrock Error")
                || b.startsWith("Error:");
    }

    private static String abbreviate(String s) {
        if (s == null) return "null";
        String oneLine = s.replaceAll("\\s+", " ").trim();
        return oneLine.length() <= 200 ? oneLine : oneLine.substring(0, 200) + "...";
    }

    private static boolean updateSentimentScore(Connection conn, String tableName, String id, SentimentResponse sentimentResponse) throws SQLException {
        int sentimentScore = (int) Math.round(sentimentResponse.getPositivityScore());
        if (sentimentScore > 100) {
            System.err.println("Skipping update for " + tableName + " ID: " + id + " - LLM returned out-of-range score: " + sentimentScore);
            return false;
        }
        sentimentScore = Math.max(sentimentScore, 0);
        String category = sentimentResponse.getCategory();
        Boolean isPromotional = sentimentResponse.getIsPromotional();
        String sql = "UPDATE " + tableName + " SET sentiment_score = ?, sentiment_category = ?" +
                (isPromotional != null ? ", is_promotional = ?" : "") +
                " WHERE id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            int idx = 1;
            pstmt.setInt(idx++, sentimentScore);
            pstmt.setString(idx++, category);
            if (isPromotional != null) {
                pstmt.setBoolean(idx++, isPromotional);
            }
            pstmt.setString(idx, id);
            pstmt.executeUpdate();
        }
        return true;
    }

    private static class PromptRequest {
        private final String prompt;

        public PromptRequest(String prompt) {
            this.prompt = prompt;
        }

        public String getPrompt() {
            return prompt;
        }
    }

    private static class SentimentResponse {
        private String category;
        private double positivity_score;
        // Null means the prompt used for this call doesn't classify promotional content, so
        // updateSentimentScore leaves any previously-stored value untouched instead of
        // overwriting it with a default.
        private Boolean is_promotional;

        SentimentResponse() {
            this.category = "invalid";
            this.positivity_score = 0;
        }

        public double getPositivityScore() {
            return positivity_score;
        }

        public String getCategory() {
            return category;
        }

        public Boolean getIsPromotional() {
            return is_promotional;
        }

        public void setCategory(String category) {
            this.category = category;
        }

        public void setPositivityScore(double positivityScore) {
            this.positivity_score = positivityScore;
        }

        public void setIsPromotional(Boolean isPromotional) {
            this.is_promotional = isPromotional;
        }
    }

    private static class ClassificationResponse {
        private String author_type;
        private String content_intent;
        private String predicted_region;
        private String topic_category;

        public String getAuthorType() {
            return author_type;
        }

        public String getContentIntent() {
            return content_intent;
        }

        public String getPredictedRegion() {
            return predicted_region;
        }

        public String getTopicCategory() {
            return topic_category;
        }
    }
}
