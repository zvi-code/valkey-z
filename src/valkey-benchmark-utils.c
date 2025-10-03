#include "valkey-benchmark-utils.h"
#include <ctype.h>
#include <valkey/valkey.h>
#include <math.h>
#include <pthread.h>
#include <string.h>
#include <stdlib.h>
#include "zmalloc.h"
#include <time.h>

#include "fmacros.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <sys/time.h>
#include <signal.h>
#include <assert.h>
#include <math.h>
#include <pthread.h>
#include <stdatomic.h>

#include "sds.h"
#include "ae.h"
#include <valkey/valkey.h>
#ifdef USE_OPENSSL
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <valkey/tls.h>
#endif
#ifdef USE_RDMA
#include <valkey/rdma.h>
#endif
#include "adlist.h"
#include "dict.h"
#include "zmalloc.h"
#include "crc16_slottable.h"
#include "hdr_histogram.h"
#include "cli_common.h"
#include "mt19937-64.h"

extern uint16_t crc16(const char *buf, int len);

#define RANDPTR_INITIAL_SIZE 8
#define DEFAULT_LATENCY_PRECISION 3
#define MAX_LATENCY_PRECISION 4
#define MAX_THREADS 500
#define CLUSTER_SLOTS 16384
#define CONFIG_LATENCY_HISTOGRAM_MIN_VALUE 10L              /* >= 10 usecs */
#define CONFIG_LATENCY_HISTOGRAM_MAX_VALUE 3000000L         /* <= 3 secs(us precision) */
#define CONFIG_LATENCY_HISTOGRAM_INSTANT_MAX_VALUE 3000000L /* <= 3 secs(us precision) */
#define SHOW_THROUGHPUT_INTERVAL 250                        /* 250ms */

#define CLIENT_GET_EVENTLOOP(c) (c->thread_id >= 0 ? config.threads[c->thread_id]->el : config.el)

#define VECTOR_PLACEHOLDER "__v_rd__"  // Exactly 8 characters for 2 floats
#define VECTOR_PLACEHOLDER_LEN 8 // length of VECTOR_PLACEHOLDER strings
#define VECTOR_NUM_RAND_DIM (VECTOR_PLACEHOLDER_LEN/sizeof(float)) // Number of random dimensions for vector generation
#define VECTOR_PLACEHOLDER_INDEX 11

#define CLUSTER_PLACEHOLDER "{tag}"
#define CLUSTER_PLACEHOLDER_LEN 5 // length of CLUSTER_PLACEHOLDER strings
#define CLUSTER_PLACEHOLDER_INDEX 10

#define PLACEHOLDER_NORMAL_LEN 12 // length of BENCHMARK_PLACEHOLDERS strings
#define PLACEHOLDER_NUM_OF 12
#define PLACEHOLDER_NORMAL_NUM_OF 10  // Number of normal placeholders excluding vector and cluster placeholders
// TODO: Use existing vectors\fields in the index as base for vector\tag\numeric generation
static const char *PLACEHOLDERS[PLACEHOLDER_NUM_OF] = {
    "__rand_int__", "__rand_1st__", "__rand_2nd__", "__rand_3rd__", "__rand_4th__",
    "__rand_5th__", "__rand_6th__", "__rand_7th__", "__rand_8th__", "__rand_9th__",
    CLUSTER_PLACEHOLDER,
    VECTOR_PLACEHOLDER  // Vector placeholder
};

struct benchmarkThread;
struct clusterNode;
struct serverConfig;

/* 
FT,INFO index_name
Response:
[ARR][array with 26 elements]
[STA]  Status: index_name
[STA]  Status: grocery_products
[STA]  Status: index_options
[ARR]  [array with 0 elements]
[STA]  Status: index_definition
[ARR]  [array with 6 elements]
[STA]    Status: key_type
[STA]    Status: HASH
[STA]    Status: prefixes
[ARR]    [array with 1 elements]
[STA]      Status: vec:
[STA]    Status: default_score
[STR]    1
[STA]  Status: attributes
[ARR]  [array with 2 elements]
[ARR]    [array with 8 elements]
[STA]      Status: identifier
[STA]      Status: vector_field
[STA]      Status: attribute
[STA]      Status: vector_field
[STA]      Status: type
[STA]      Status: VECTOR
[STA]      Status: index
[ARR]      [array with 12 elements]
[STA]        Status: capacity
[INT]        102400
[STA]        Status: dimensions
[INT]        768
[STA]        Status: distance_metric
[STA]        Status: COSINE
[STA]        Status: size
[STR]        2
[STA]        Status: data_type
[STA]        Status: FLOAT32
[STA]        Status: algorithm
[ARR]        [array with 8 elements]
[STA]          Status: name
[STA]          Status: HNSW
[STA]          Status: m
[INT]          16
[STA]          Status: ef_construction
[INT]          200
[STA]          Status: ef_runtime
[INT]          200
[STA]    Status: curr_vectors
[INT]  100230
[STA]  Status: curr_deleted_vectors
[INT]  100228
[ARR]  [array with 10 elements]
[STA]    Status: identifier
[STA]    Status: category
[STA]    Status: attribute
[STA]    Status: category
[STA]    Status: type
[STA]    Status: TAG
[STA]    Status: SEPARATOR
[STA]    Status: ,
[STA]    Status: size
[STR]    2
[STA]  Status: num_docs
[STR]  2
[STA]  Status: num_terms
[STR]  0
[STA]  Status: num_records
[STR]  4
[STA]  Status: hash_indexing_failures
[STR]  0
[STA]  Status: backfill_in_progress
[STR]  0
[STA]  Status: backfill_complete_percent
[STR]  1.000000
[STA]  Status: mutation_queue_size
[STR]  0
*/
/* struct to hold exact ft.info response data */
typedef struct searchFtInfoResponse {
    sds index_name;          /* Index name */
    /* Index options, currently unused */
    sds key_type;           /* Key type (e.g., HASH) */
    sds* prefixes;           /* Key prefixes for the index */
    int prefixes_count; /* Number of prefixes */
    sds default_score;      /* Default score for documents */
    sds identifier;         /* Identifier for the index */
    sds attribute;        /* Attributes of the index */
    sds type;              /* Type of the index (e.g., VECTOR) */


} searchFtInfoResponse;


/* Tag distribution structure */
typedef struct tagDistribution {
    sds pattern;            /* Tag pattern with optional placeholders */
    double percentage;      /* Percentage of keys with this tag */
    double cumulative;      /* Cumulative percentage for selection */
} tagDistribution;

typedef struct searchRuntimeConfig {
    /* Tag distribution fields */
    tagDistribution *tag_dists; /* Array of tag distributions */
    int n_dists;               /* Number of distributions */
    sds tag_filter;                      /* Filter pattern for queries */
} searchRuntimeConfig;
/* Search index configuration */
typedef struct searchIndex {
    sds name;               /* Index name */
    sds algorithm;          /* Index algorithm type (e.g., HNSW, FLAT) */
    sds prefix;             /* Index key prefix */
    int nocontent;           /* Use NOCONTENT option for FT.SEARCH */
    sds vector_field;       /* Vector field name */
    int vector_dim;         /* Vector dimension */    
    sds tag_field;          /* Tag field name if exists*/
    sds numeric_field;      /* Numeric field name if exists */
    int ef_construction;    /* EF Construction for vector search */
    int m;                  /* HNSW M parameter */
    int ef_search;          /* EF Search for vector search */
    int k;                  /* Number of nearest neighbors to return */
    sds metric;            /* Distance metric (e.g., L2, COSINE) */
    searchRuntimeConfig curr_conf; /* Runtime configuration for search */
} searchIndex;

/* Vector placeholder callback information */
typedef enum {
    VECTOR_PHASE_PREFILL,
    VECTOR_PHASE_INSERT,
    VECTOR_PHASE_QUERY
} VectorPhase;

/* Callback function type for vector placeholder replacement */
typedef void (*VectorPlaceholderCallback)(char *vector_data, const char *key, VectorPhase phase, int dim);

/* Base vector for efficient vector generation */
static float *base_vector = NULL;
static int base_vector_dim = 0;

/* Locations of the placeholders __rand_int__, __rand_1st__,
 * __rand_2nd, etc. within the RESP encoded command buffer. */
static struct placeholders {
    size_t cmd_len;                     /* length of the command */
    size_t count[PLACEHOLDER_NUM_OF];    /* number of each placeholder in the command */
    size_t len[PLACEHOLDER_NUM_OF];      /* length of each placeholder */
    size_t *indices[PLACEHOLDER_NUM_OF]; /* pointer to indices for each placeholder */
    size_t *index_data;                 /* allocation holding all index data */
} placeholders;

typedef struct _client {
    valkeyContext *context;
    sds obuf;
    char **stagptr;     /* Pointers to slot hashtags (cluster mode only) */
    size_t staglen;     /* Number of pointers in client->stagptr */
    size_t stagfree;    /* Number of unused pointers in client->stagptr */
    size_t written;     /* Bytes of 'obuf' already written */
    long long start;    /* Start time of a request */
    long long latency;  /* Request latency */
    int seqlen;         /* Number of commands in the command sequence */
    int pending;        /* Number of pending requests (replies to consume) */
    int prefix_pending; /* If non-zero, number of pending prefix commands. Commands
                           such as auth and select are prefixed to the pipeline of
                           benchmark commands and discarded after the first send. */
    int prefixlen;      /* Size in bytes of the pending prefix commands */
    int thread_id;
    struct clusterNode *cluster_node;
    int slots_last_update;
    uint64_t paused : 1;
    uint64_t reuse : 1;
} *client;


/* Threads. */
typedef struct benchmarkThread {
    int index;
    pthread_t thread;
    aeEventLoop *el;
    list *paused_clients;
} benchmarkThread;



/* Cluster. */
typedef struct clusterNode {
    valkeyContext *ctx;
    char *ip;
    int port;
    sds name;
    int flags;
    sds replicate; /* Primary ID if node is a replica */
    int *slots;
    int slots_count;
    int *updated_slots;      /* Used by updateClusterSlotsConfiguration */
    int updated_slots_count; /* Used by updateClusterSlotsConfiguration */
    int replicas_count;
    struct serverConfig *server_config;
} clusterNode;

typedef struct serverConfig {
    sds save;
    sds appendonly;
} serverConfig;

static struct config {
    aeEventLoop *el;
    enum valkeyConnectionType ct;
    cliConnInfo conn_info;
    valkeyContext *conn_ctx;
    int tls;
    int mptcp;
    struct cliSSLconfig sslconfig;
    int numclients;
    _Atomic int liveclients;
    int requests;
    _Atomic int requests_issued;
    _Atomic int requests_finished;
    _Atomic int previous_requests_finished;
    int last_printed_bytes;
    long long previous_tick;
    int keysize;
    int datasize;
    int replace_placeholders;
    int keyspacelen;
    int sequential_replacement;
    int keepalive;
    int pipeline;
    long long start;
    long long totlatency;
    const char *title;
    list *clients;
    list *paused_clients;
    int quiet;
    int csv;
    int loop;
    int idlemode;
    sds input_dbnumstr;
    char *tests;
    int stdinarg; /* get last arg from stdin. (-x option) */
    int precision;
    int num_threads;
    struct benchmarkThread **threads;
    int cluster_mode;
    readFromReplica read_from_replica;
    int cluster_node_count;
    struct clusterNode **cluster_nodes;
    int cluster_primary_node_count;
    struct clusterNode **cluster_primary_nodes;
    int selected_node_count;
    struct clusterNode **selected_nodes;
    struct serverConfig *server_config;
    struct hdr_histogram *latency_histogram;
    struct hdr_histogram *current_sec_latency_histogram;
    _Atomic int is_fetching_slots;
    _Atomic int is_updating_slots;
    _Atomic int slots_last_update;
    int enable_tracking;
    int num_functions;
    int num_keys_in_fcall;
    pthread_mutex_t liveclients_mutex;
    pthread_mutex_t is_updating_slots_mutex;
    int resp3; /* use RESP3 */
    int rps;
    atomic_uint_fast64_t last_time_ns;
    uint64_t time_per_token;
    uint64_t time_per_burst;
    int use_search; /* Use search indexes */
    searchIndex search;
    int print_search_results; /* Print FT.SEARCH results */
} config;


/* Prototypes */
static void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask);
static void createMissingClients(client c);
static benchmarkThread *createBenchmarkThread(int index);
static void freeBenchmarkThread(benchmarkThread *thread);
static void freeBenchmarkThreads(void);
static void *execBenchmarkThread(void *ptr);
static void benchmark(const char *title, char *cmd, int len);
static clusterNode *createClusterNode(char *ip, int port);
// static serverConfig *getServerConfig(enum valkeyConnectionType ct, const char *ip_or_path, int port);
static sds selectTagByDistribution(void);
static void parseTagDistributions(const char *distributions_str);
static valkeyContext *getValkeyContext(enum valkeyConnectionType ct, const char *ip_or_path, int port);
static void freeServerConfig(serverConfig *cfg);
static int fetchClusterSlotsConfiguration(client c);
static void updateClusterSlotsConfiguration(void);
static long long showThroughput(struct aeEventLoop *eventLoop, long long id, void *clientData);

/* Dict callbacks */
static uint64_t dictSdsHash(const void *key);
static int dictSdsKeyCompare(const void *key1, const void *key2);

#define UNUSED(V) ((void)V)

/* Implementation */
static long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec) * 1000000;
    ust += tv.tv_usec;
    return ust;
}

static long long mstime(void) {
    return ustime() / 1000;
}

static long long nstime(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

/* Exact field name matcher */
static int exact_field_matcher(const char *line, const char *prefix) {
    if (!line || !prefix) return 0;
    
    /* Find the colon separator */
    const char *colon = strchr(line, ':');
    if (!colon) return 0;
    
    size_t key_len = colon - line;
    size_t prefix_len = strlen(prefix);
    
    return (key_len == prefix_len && strncmp(line, prefix, prefix_len) == 0);
}

/* Prefix matcher for fields with common prefixes */
static int prefix_field_matcher(const char *line, const char *prefix) {
    if (!line || !prefix) return 0;
    return strncmp(line, prefix, strlen(prefix)) == 0;
}

/* Parse integer value */
static long long parse_integer_value(const char *value) {
    if (!value) return 0;
    
    /* Skip whitespace */
    while (*value == ' ' || *value == '\t') value++;
    
    /* Handle quoted values */
    if (*value == '"') {
        value++;
        char *end_quote = strchr(value, '"');
        if (end_quote) {
            char buf[64];
            size_t len = end_quote - value;
            if (len < sizeof(buf)) {
                memcpy(buf, value, len);
                buf[len] = '\0';
                return atoll(buf);
            }
        }
    }
    
    return atoll(value);
}

/* Parse floating point value and return as fixed-point integer (scaled by 1000) */
static long long parse_float_as_fixed(const char *value) {
    if (!value) return 0;
    
    while (*value == ' ' || *value == '\t') value++;
    
    double d = atof(value);
    return (long long)(d * 1000.0);
}

/* Parse memory value with unit suffixes (K, M, G) */
static long long parse_memory_value(const char *value) {
    if (!value) return 0;
    
    while (*value == ' ' || *value == '\t') value++;
    
    char *end;
    long long val = strtoll(value, &end, 10);
    
    if (end && *end) {
        switch (toupper(*end)) {
            case 'K': return val * 1024;
            case 'M': return val * 1024 * 1024;
            case 'G': return val * 1024 * 1024 * 1024;
        }
    }
    
    return val;
}

/* Parse percentile value (e.g., "p50=236.543,p99=430.079") - extract p50 */
static long long parse_percentile_p50(const char *value) {
    if (!value) return 0;
    const char *p50 = strstr(value, "p50=");
    if (!p50) return 0;
    return (long long)(atof(p50 + 4) * 1000);  /* Return as microseconds */
}

/* Parse percentile value - extract p99 */
static long long parse_percentile_p99(const char *value) {
    if (!value) return 0;
    const char *p99 = strstr(value, "p99=");
    if (!p99) return 0;
    return (long long)(atof(p99 + 4) * 1000);
}

/* Parse percentile value - extract p99.9 */
static long long parse_percentile_p999(const char *value) {
    if (!value) return 0;
    const char *p999 = strstr(value, "p99.9=");
    if (!p999) return 0;
    return (long long)(atof(p999 + 6) * 1000);
}

/* Sum aggregation */
static void aggregate_sum(void **opaque, long long value, int node_idx, int is_last_node) {
    UNUSED(node_idx);
    
    if (!*opaque) {
        *opaque = zcalloc(sizeof(long long));
        *(long long *)*opaque = 0;
    }
    
    *(long long *)*opaque += value;
    UNUSED(is_last_node);
}

/* Average aggregation */
static void aggregate_average(void **opaque, long long value, int node_idx, int is_last_node) {
    typedef struct {
        long long sum;
        int count;
    } avg_state_t;
    
    if (!*opaque) {
        *opaque = zcalloc(sizeof(avg_state_t));
        ((avg_state_t *)*opaque)->sum = 0;
        ((avg_state_t *)*opaque)->count = 0;
    }
    
    avg_state_t *state = (avg_state_t *)*opaque;
    state->sum += value;
    state->count++;
    
    if (is_last_node && state->count > 0) {
        long long avg = state->sum / state->count;
        state->sum = avg;
        state->count = 1;
    }
    
    UNUSED(node_idx);
}

/* Min/Max aggregation */
static void aggregate_minmax(void **opaque, long long value, int node_idx, int is_last_node) {
    typedef struct {
        long long min_val;
        long long max_val;
        int initialized;
    } minmax_state_t;
    
    if (!*opaque) {
        *opaque = zcalloc(sizeof(minmax_state_t));
        ((minmax_state_t *)*opaque)->initialized = 0;
    }
    
    minmax_state_t *state = (minmax_state_t *)*opaque;
    
    if (!state->initialized) {
        state->min_val = value;
        state->max_val = value;
        state->initialized = 1;
    } else {
        if (value < state->min_val) state->min_val = value;
        if (value > state->max_val) state->max_val = value;
    }
    
    UNUSED(node_idx);
    UNUSED(is_last_node);
}

/* Maximum aggregation */
static void aggregate_max(void **opaque, long long value, int node_idx, int is_last_node) {
    if (!*opaque) {
        *opaque = zcalloc(sizeof(long long));
        *(long long *)*opaque = value;
    } else {
        long long *max_val = (long long *)*opaque;
        if (value > *max_val) {
            *max_val = value;
        }
    }
    
    UNUSED(node_idx);
    UNUSED(is_last_node);
}

/* Display integer value */
static void display_cmdstat(const char *field_name, const char* cmdstat, void *opaque, int node_count) {
    UNUSED(node_count);
    UNUSED(cmdstat);
    if (!opaque) return;
    printf("%s: %lld", field_name, *(long long *)opaque);
}

static void display_calls(const char *field_name, void *opaque, int node_count) {
    UNUSED(node_count);
    UNUSED(field_name);
    display_cmdstat(field_name, "calls", opaque, node_count);
}
static void display_usec(const char *field_name, void *opaque, int node_count) {
    UNUSED(node_count);
    UNUSED(field_name);
    display_cmdstat(field_name, "usec", opaque, node_count);
}
static void display_usec_per_call(const char *field_name, void *opaque, int node_count) {
    UNUSED(node_count);
    UNUSED(field_name);
    display_cmdstat(field_name, "usec_per_call", opaque, node_count);
}
static void display_failed(const char *field_name, void *opaque, int node_count) {
    UNUSED(node_count);
    UNUSED(field_name);
    display_cmdstat(field_name, "failed", opaque, node_count);
}
static void display_rejected(const char *field_name, void *opaque, int node_count) {
    UNUSED(node_count);
    UNUSED(field_name);
    display_cmdstat(field_name, "rejected", opaque, node_count);
}

/* Display integer value */
static void display_integer(const char *field_name, void *opaque, int node_count) {
    UNUSED(node_count);
    if (!opaque) return;
    printf("%s: %lld", field_name, *(long long *)opaque);
}

/* Display memory in MB */
static void display_memory_mb(const char *field_name, void *opaque, int node_count) {
    UNUSED(node_count);
    if (!opaque) return;
    long long bytes = *(long long *)opaque;
    printf("%s: %.2f MB", field_name, (double)bytes / (1024.0 * 1024.0));
}

/* Display memory in human-readable format */
static void display_memory_human(const char *field_name, void *opaque, int node_count) {
    UNUSED(node_count);
    if (!opaque) return;
    long long bytes = *(long long *)opaque;
    
    const char *units[] = {"B", "KB", "MB", "GB", "TB"};
    int unit_idx = 0;
    double size = (double)bytes;
    
    while (size >= 1024.0 && unit_idx < 4) {
        size /= 1024.0;
        unit_idx++;
    }
    
    printf("%s: %.2f %s", field_name, size, units[unit_idx]);
}

/* Display percentage (from fixed-point value scaled by 1000) */
static void display_percentage(const char *field_name, void *opaque, int node_count) {
    UNUSED(node_count);
    if (!opaque) return;
    long long fixed_val = *(long long *)opaque;
    printf("%s: %.3f%%", field_name, (double)fixed_val / 1000.0);
}

/* Display floating point value (from fixed-point scaled by 1000) */
static void display_float(const char *field_name, void *opaque, int node_count) {
    UNUSED(node_count);
    if (!opaque) return;
    long long fixed_val = *(long long *)opaque;
    printf("%s: %.3f", field_name, (double)fixed_val / 1000.0);
}

/* Display min/max range */
static void display_minmax(const char *field_name, void *opaque, int node_count) {
    UNUSED(node_count);
    if (!opaque) return;
    
    typedef struct {
        long long min_val;
        long long max_val;
        int initialized;
    } minmax_state_t;
    
    minmax_state_t *state = (minmax_state_t *)opaque;
    if (state->initialized) {
        if (state->min_val == state->max_val) {
            printf("%s: %lld", field_name, state->min_val);
        } else {
            printf("%s: %lld - %lld", field_name, state->min_val, state->max_val);
        }
    }
}

/* Display latency in microseconds */
static void display_latency_usec(const char *field_name, void *opaque, int node_count) {
    UNUSED(node_count);
    if (!opaque) return;
    long long usec = *(long long *)opaque;
    printf("%s: %.2f us", field_name, (double)usec / 1000.0);
}

/* Calculate rate per second */
static void diff_rate_per_second(const char *field_name, fieldSnapshot *old, 
                                 fieldSnapshot *new_snap, long long time_delta_ms, int node_idx) {
    if (!old || !new_snap || !old->valid || !new_snap->valid || time_delta_ms <= 0) {
        return;
    }
    
    if (node_idx < 0) {
        /* Cluster-wide rate */
        long long delta = new_snap->value - old->value;
        double rate = (double)delta / ((double)time_delta_ms / 1000.0);
        printf("%s/sec: %.2f (delta: %lld)", field_name, rate, delta);
    } else {
        /* Per-node rate */
        if (old->per_node_values && new_snap->per_node_values &&
            node_idx < old->node_count && node_idx < new_snap->node_count) {
            long long node_delta = new_snap->per_node_values[node_idx] - 
                                   old->per_node_values[node_idx];
            double node_rate = (double)node_delta / ((double)time_delta_ms / 1000.0);
            printf("Node %d: %.2f/sec (delta: %lld)", node_idx, node_rate, node_delta);
        }
    }
}

/* Calculate memory growth rate */
static void diff_memory_growth(const char *field_name, fieldSnapshot *old, 
                               fieldSnapshot *new_snap, long long time_delta_ms, int node_idx) {
    if (!old || !new_snap || !old->valid || !new_snap->valid || time_delta_ms <= 0) {
        return;
    }
    
    long long delta = new_snap->value - old->value;
    double mb_per_sec = (double)(delta / (1024 * 1024)) / ((double)time_delta_ms / 1000.0);
    
    if (node_idx < 0) {
        printf("%s growth: %.2f MB/sec (total delta: %lld MB)", 
               field_name, mb_per_sec, delta / (1024 * 1024));
    } else {
        if (old->per_node_values && new_snap->per_node_values &&
            node_idx < old->node_count && node_idx < new_snap->node_count) {
            long long node_delta = new_snap->per_node_values[node_idx] - 
                                   old->per_node_values[node_idx];
            double node_mb_per_sec = (double)(node_delta / (1024 * 1024)) / 
                                     ((double)time_delta_ms / 1000.0);
            printf("Node %d: %.2f MB/sec (delta: %lld MB)", 
                   node_idx, node_mb_per_sec, node_delta / (1024 * 1024));
        }
    }
}

/* Calculate percentage change */
static void diff_percentage_change(const char *field_name, fieldSnapshot *old, 
                                   fieldSnapshot *new_snap, long long time_delta_ms, int node_idx) {
    UNUSED(time_delta_ms);
    
    if (!old || !new_snap || !old->valid || !new_snap->valid) {
        return;
    }
    
    if (node_idx < 0) {
        if (old->value == 0) {
            printf("%s: N/A (initial value was 0)", field_name);
        } else {
            long long delta = new_snap->value - old->value;
            double pct_change = ((double)delta / (double)old->value) * 100.0;
            printf("%s change: %.2f%% (from %lld to %lld)", 
                   field_name, pct_change, old->value, new_snap->value);
        }
    }
}


/* Calculate average latency change */
static void diff_latency_change(const char *field_name, fieldSnapshot *old, 
                                fieldSnapshot *new_snap, long long time_delta_ms, int node_idx) {
    UNUSED(time_delta_ms);
    
    if (!old || !new_snap || !old->valid || !new_snap->valid) {
        return;
    }
    
    if (node_idx < 0) {
        long long delta_usec = new_snap->value - old->value;
        double delta_ms = (double)delta_usec / 1000.0;
        printf("%s delta: %.2f ms (from %.2f to %.2f ms)", 
               field_name, delta_ms, 
               (double)old->value / 1000.0, 
               (double)new_snap->value / 1000.0);
    }
}


/* Parse cmdstat line and extract calls value */
static long long parse_cmdstat_calls(const char *value) {
    /* value points to: "calls=2858704123,usec=275125382879,..." */
    const char *calls_start = strstr(value, "calls=");
    if (!calls_start) return 0;
    return atoll(calls_start + 6);
}

/* Parse cmdstat line and extract usec value */
static long long parse_cmdstat_usec_per_call(const char *value) {
    const char *usec_start = strstr(value, "usec_per_call=");
    if (!usec_start) return 0;

    /* Move past "usec_per_call=" */
    usec_start += 14;
    
    /* Parse until comma or end */
    char buf[32];
    int i = 0;
    while (*usec_start && *usec_start != ',' && i < 31) {
        buf[i++] = *usec_start++;
    }
    buf[i] = '\0';
    
    return atoll(buf);
}

/* Parse cmdstat line and extract usec value */
static long long parse_cmdstat_usec(const char *value) {
    const char *usec_start = strstr(value, "usec=");
    if (!usec_start) return 0;
    
    /* Move past "usec=" */
    usec_start += 5;
    
    /* Parse until comma or end */
    char buf[32];
    int i = 0;
    while (*usec_start && *usec_start != ',' && i < 31) {
        buf[i++] = *usec_start++;
    }
    buf[i] = '\0';
    
    return atoll(buf);
}

/* Parse rejected_calls */
static long long parse_cmdstat_rejected(const char *value) {
    const char *rejected_start = strstr(value, "rejected_calls=");
    if (!rejected_start) return 0;
    return atoll(rejected_start + 15);
}

/* Parse failed_calls */
static long long parse_cmdstat_failed(const char *value) {
    const char *failed_start = strstr(value, "failed_calls=");
    if (!failed_start) return 0;
    return atoll(failed_start + 13);
}

/* Diff callback that calculates usec_per_call from deltas and shows distribution */
static void diff_cmdstat_latency(const char *field_name, fieldSnapshot *old_calls, 
                                 fieldSnapshot *new_calls, fieldSnapshot *old_usec,
                                 fieldSnapshot *new_usec, long long time_delta_ms) {
    if (!old_calls || !new_calls || !old_usec || !new_usec || 
        !old_calls->valid || !new_calls->valid || 
        !old_usec->valid || !new_usec->valid || time_delta_ms <= 0) {
        // print full detailed debug info and exit
        // print all details about old calls and new calls and time delta
        printf("%s: insufficient data for latency calculation\n", field_name);
        printf("  old_calls valid: %d, value: %lld\n", old_calls ? old_calls->valid : 0, old_calls ? old_calls->value : 0);
        printf("  new_calls valid: %d, value: %lld\n", new_calls ? new_calls->valid : 0, new_calls ? new_calls->value : 0);
        printf("  old_usec valid: %d, value: %lld\n", old_usec ? old_usec->valid : 0, old_usec ? old_usec->value : 0);
        printf("  new_usec valid: %d, value: %lld\n", new_usec ? new_usec->valid : 0, new_usec ? new_usec->value : 0);
        printf("  time_delta_ms: %lld\n", time_delta_ms);
        return;      
    }
    
    long long calls_diff = new_calls->value - old_calls->value;
    long long usec_diff = new_usec->value - old_usec->value;
    double interval_sec = (double)time_delta_ms / 1000.0;
    
    /* Cluster-wide metrics */
    double calls_per_sec = (double)calls_diff / interval_sec;
    double usec_per_call_interval = (calls_diff > 0) ? 
        (double)usec_diff / (double)calls_diff : 0.0;
    
    printf("%s:\n", field_name);
    printf("    Calls/sec: %.2f (total: %lld)\n", calls_per_sec, calls_diff);
    printf("    Avg latency (interval): %.2f usec\n", usec_per_call_interval);
    
    /* Per-node distribution analysis */
    if (old_calls->per_node_values && new_calls->per_node_values &&
        old_usec->per_node_values && new_usec->per_node_values) {
        
        printf("   Per-node distribution:\n");
        
        /* Calculate per-node metrics */
        typedef struct {
            int node_idx;
            double calls_per_sec;
            double usec_per_call;
            long long calls_diff;
        } node_metric_t;
        
        node_metric_t *metrics = zcalloc(old_calls->node_count * sizeof(node_metric_t));
        int valid_count = 0;
        
        double total_calls_per_sec = 0.0;
        double min_latency = 0.0, max_latency = 0.0;
        double min_calls_rate = 0.0, max_calls_rate = 0.0;
        
        for (int i = 0; i < old_calls->node_count && i < new_calls->node_count; i++) {
            long long node_calls_diff = new_calls->per_node_values[i] - 
                                        old_calls->per_node_values[i];
            long long node_usec_diff = new_usec->per_node_values[i] - 
                                       old_usec->per_node_values[i];
            
            if (node_calls_diff > 0) {
                metrics[valid_count].node_idx = i;
                metrics[valid_count].calls_diff = node_calls_diff;
                metrics[valid_count].calls_per_sec = (double)node_calls_diff / interval_sec;
                metrics[valid_count].usec_per_call = (double)node_usec_diff / (double)node_calls_diff;
                
                total_calls_per_sec += metrics[valid_count].calls_per_sec;
                
                if (valid_count == 0) {
                    min_latency = max_latency = metrics[valid_count].usec_per_call;
                    min_calls_rate = max_calls_rate = metrics[valid_count].calls_per_sec;
                } else {
                    if (metrics[valid_count].usec_per_call < min_latency) 
                        min_latency = metrics[valid_count].usec_per_call;
                    if (metrics[valid_count].usec_per_call > max_latency) 
                        max_latency = metrics[valid_count].usec_per_call;
                    if (metrics[valid_count].calls_per_sec < min_calls_rate)
                        min_calls_rate = metrics[valid_count].calls_per_sec;
                    if (metrics[valid_count].calls_per_sec > max_calls_rate)
                        max_calls_rate = metrics[valid_count].calls_per_sec;
                }
                
                valid_count++;
            }
        }
        
        if (valid_count > 0) {
            /* Distribution summary */
            printf("      Latency range: %.2f - %.2f usec\n", min_latency, max_latency);
            printf("      Calls/sec range: %.2f - %.2f\n", min_calls_rate, max_calls_rate);
            
            /* Show individual nodes */
            for (int i = 0; i < valid_count; i++) {
                double call_pct = (total_calls_per_sec > 0) ? 
                    (metrics[i].calls_per_sec / total_calls_per_sec * 100.0) : 0.0;
                printf("      Node %d: %.2f calls/sec (%.1f%%), %.2f usec/call\n",
                       metrics[i].node_idx, 
                       metrics[i].calls_per_sec,
                       call_pct,
                       metrics[i].usec_per_call);
            }
            
            /* Identify outliers */
            if (valid_count > 1 && max_latency > min_latency * 1.5) {
                printf("      WARNING: Latency variance detected (%.1fx difference)\n",
                       max_latency / min_latency);
            }
            if (valid_count > 1 && max_calls_rate > min_calls_rate * 1.5) {
                printf("      WARNING: Load imbalance detected (%.1fx difference)\n",
                       max_calls_rate / min_calls_rate);
            }
        }
        
        zfree(metrics);
    }
}

/* Wrapper diff callback for the snapshot comparison framework */
static void diff_ftsearch_performance(const char *field_name, fieldSnapshot *old, 
                                      fieldSnapshot *new_snap, long long time_delta_ms, 
                                      int node_idx) {
    UNUSED(field_name);
    UNUSED(old);
    UNUSED(new_snap);
    UNUSED(time_delta_ms);
    UNUSED(node_idx);
    /* This callback is a placeholder - actual work done in custom compare function */
}

/* Specialized comparison for FT.SEARCH command stats */
void compareFTSearchCmdstat(clusterSnapshot *snap1, clusterSnapshot *snap2,
                           int calls_idx, int usec_idx, long long time_delta_ms) {
    if (!snap1 || !snap2 || time_delta_ms <= 0) return;
    
    fieldSnapshot *old_calls = &snap1->fields[calls_idx];
    fieldSnapshot *new_calls = &snap2->fields[calls_idx];
    fieldSnapshot *old_usec = &snap1->fields[usec_idx];
    fieldSnapshot *new_usec = &snap2->fields[usec_idx];
    
    diff_cmdstat_latency("FT.SEARCH", old_calls, new_calls, old_usec, new_usec, time_delta_ms);
}


/* */
/* Convert FT.INFO RESP array response to line-based format 
ec-search-zvi-ec-1shard-no-tls.ajfdds.clustercfg.euw1devo.cache.amazonaws.com:6379> ft.info vindex_4dim
 1) index_name
 2) vindex_4dim
 3) index_options
 4) (empty array)
 5) index_definition
 6) 1) key_type
    2) HASH
    3) prefixes
    4) 1) vkey4dim:
    5) default_score
    6) "1"
 7) attributes
 8) 1)  1) identifier
        2) embed
        3) attribute
        4) embed
        5) type
        6) VECTOR
        7) algorithm
        8) HNSW
        9) data_type
       10) FLOAT32
       11) dim
       12) (integer) 4
       13) distance_metric
       14) L2
       15) M
       16) (integer) 16
       17) ef_construction
       18) (integer) 200
       19) ef_runtime
       20) (integer) 32
       21) capacity
       22) (integer) 5294080
       23) size
       24) "5288102"
 9) num_docs
10) (integer) 5288102
11) num_terms
12) (integer) 0
13) num_records
14) (integer) 5288102
15) hash_indexing_failures
16) "0"
17) gc_stats
18)  1) bytes_collected
     2) "0"
     3) total_ms_run
     4) "0"
     5) total_cycles
     6) "0"
     7) average_cycle_time_ms
     8) "nan"
     9) last_run_time_ms
    10) "0"
    11) gc_numeric_trees_missed
    12) "0"
    13) gc_blocks_denied
    14) "0"
19) cursor_stats
20) 1) global_idle
    2) (integer) 0
    3) global_total
    4) (integer) 0
    5) index_capacity
    6) (integer) 0
    7) index_total
    8) (integer) 0
21) dialect_stats
22) 1) dialect_1
    2) (integer) 0
    3) dialect_2
    4) (integer) 0
    5) dialect_3
    6) (integer) 0
    7) dialect_4
    8) (integer) 0
23) Index Errors
24) 1) indexing failures
    2) (integer) 0
    3) last indexing error
    4) N/A
    5) last indexing error key
    6) "N/A"
    7) background indexing status
    8) OK
25) backfill_in_progress
26) "0"
27) backfill_complete_percent
28) "1.000000"
29) mutation_queue_size
30) "0"
31) recent_mutations_queue_delay
32) "0 sec"
33) state
34) ready
35) pending_slots_count
36) "0"
ec-search-zvi-ec-1shard-no-tls.ajfdds.clustercfg.euw1devo.cache.amazonaws.com:6379> 
*/
static sds convertFtInfoToLines(valkeyReply *reply, const char *prefix) {
    if (!reply ) return NULL;
    // printf("Converting FT.INFO response to lines with prefix: %s\n", prefix ? prefix : "(none)");
    sds lines = sdsempty();
    if (reply->type != VALKEY_REPLY_ARRAY) {
        if (reply->type == VALKEY_REPLY_STRING || reply->type == VALKEY_REPLY_STATUS) {
            lines = sdscatprintf(lines, "%s:%s\n", prefix ? prefix : "", reply->str);
        } else if (reply->type == VALKEY_REPLY_INTEGER) {
            lines = sdscatprintf(lines, "%s:%lld\n", prefix ? prefix : "", reply->integer);
        } else {
            printf("Unexpected FT.INFO reply type: %d\n", reply->type);
        }
        return lines;
    }
    for (size_t i = 0; i < reply->elements; i++) {                
        valkeyReply *element_reply = reply->element[i];
        if (!element_reply) continue;
        if (element_reply->type == VALKEY_REPLY_ARRAY) {
            sds attr_lines = convertFtInfoToLines(element_reply, prefix);
            if (attr_lines) {
                lines = sdscatsds(lines, attr_lines);
                sdsfree(attr_lines);
            }
            continue;
        }
        if (i == reply->elements -1) {
            if (element_reply->type == VALKEY_REPLY_STRING || element_reply->type == VALKEY_REPLY_STATUS) {
                lines = sdscatprintf(lines, "%s:%s\n", prefix ? prefix : "", element_reply->str);
            } else if (element_reply->type == VALKEY_REPLY_INTEGER) {
                lines = sdscatprintf(lines, "%s:%lld\n", prefix ? prefix : "", element_reply->integer);
            }        
            break; /* No value for the last key */
        }
        char *key_name = element_reply->str;
        
        /* Build full key with prefix if provided */
        sds full_key = prefix ? sdscatprintf(sdsempty(), "%s.%s", prefix, key_name) 
                              : sdsnew(key_name);
        i++;
        valkeyReply *value = reply->element[i];
        if (value->type == VALKEY_REPLY_STRING || value->type == VALKEY_REPLY_STATUS) {
            lines = sdscatprintf(lines, "%s:%s\n", full_key, value->str);
        } else if (value->type == VALKEY_REPLY_INTEGER) {
            lines = sdscatprintf(lines, "%s:%lld\n", full_key, value->integer);
        } else if (value->type == VALKEY_REPLY_ARRAY) {
            /* Handle nested arrays */
            /* Attributes array contains one or more attribute definitions */
            sds attr_lines = convertFtInfoToLines(value, full_key);
            if (attr_lines) {
                lines = sdscatsds(lines, attr_lines);
                sdsfree(attr_lines);
            }
        } else {
            printf("Skipping unsupported value type %d for key %s\n", value->type, full_key);
        }
        
        sdsfree(full_key);
    }
    
    return lines;
}

/* Create snapshot from current cluster state 
# Modules

# search_coordinator

# search_core-management
search_cores_received:0
search_cores_requested:0

# search_global_ingestion
search_ingest_field_numeric:0
search_ingest_field_tag:0
search_ingest_field_vector:1000323
search_ingest_hash_blocked:0
search_ingest_hash_keys:2167580
search_ingest_json_blocked:0
search_ingest_json_keys:0
search_ingest_last_batch_size:0
search_ingest_total_batches:0
search_ingest_total_failures:0

# search_global_metrics
search_bounds_check_errors:0
search_hnsw_edges_marked_deleted:14112
search_hnsw_nodes_marked_deleted:882
search_interned_strings_memory:35397554
search_keys_bytes:20997522
search_num_flat_indexes:0
search_num_flat_nodes:0
search_num_hnsw_edges:16012224
search_num_hnsw_indexes:1
search_num_hnsw_nodes:1000764
search_num_interned_strings:1899884
search_num_numeric_indexes:0
search_num_numeric_records:0
search_num_tag_indexes:0
search_num_tags:0
search_tags_bytes:0
search_vectors_bytes:14400032
search_vectors_marked_deleted:1
search_vectors_memory_marked_deleted:16

# search_hnswlib
search_hnsw_add_exceptions_count:0
search_hnsw_create_exceptions_count:0
search_hnsw_modify_exceptions_count:441
search_hnsw_remove_exceptions_count:0
search_hnsw_search_exceptions_count:0

# search_index_metering
search_module_background_mspus:0

# search_index_stats
search_number_of_active_indexes:1
search_number_of_active_indexes_indexing:0
search_number_of_active_indexes_running_queries:0
search_number_of_attributes:1
search_number_of_indexes:1
search_total_active_write_threads:16
search_total_indexed_documents:1000323
search_total_indexing_time:0

# search_indexing
search_background_indexing_status:NO_ACTIVITY

# search_latency
search_hnsw_vector_index_search_latency_usec:p50=25.343,p99=44.031,p99.9=63.231

# search_memory
search_index_reclaimable_memory:16
search_search_extra_counter_00:0
search_used_memory:856192040
search_used_memory_bytes:856192040
search_used_memory_human:816.53MiB

# search_metering
search_ft_search_ecpus:0
search_prefix_write_error_cnt:0
search_suffix_write_error_cnt:0

# search_query
search_failure_requests_count:0
search_hybrid_requests_count:0
search_inline_filtering_requests_count:0
search_query_prefiltering_requests_cnt:0
search_result_record_dropped_count:0
search_successful_requests_count:3127457307

# search_rdb
search_rdb_load_failure_cnt:0
search_rdb_load_success_cnt:0
search_rdb_save_failure_cnt:0
search_rdb_save_success_cnt:0

# search_string_interning
search_string_interning_memory_bytes:1899884
search_string_interning_memory_human:1.81MiB
search_string_interning_store_size:1899885

# search_test-counters
search_test-counter-ForceCancels:0
search_test-counter-gRPCCancels:0

# search_thread-pool
search_query_queue_size:0
search_read_cpu_time_sec:126989.62995999999
search_reader_resumed_cnt:0
search_used_read_cpu:4.2168464057504291
search_used_write_cpu:0.00065063934711687642
search_worker_pool_suspend_cnt:0
search_write_cpu_time_sec:462.99530899999996
search_writer_queue_size:0
search_writer_resumed_cnt:0
search_writer_suspension_expired_cnt:0

# search_time_slice_mutex
search_time_slice_deletes:0
search_time_slice_queries:3127457309
search_time_slice_read_periods:3127457309
search_time_slice_read_time:85366119063
search_time_slice_upserts:1114643
search_time_slice_write_periods:2167580
search_time_slice_write_time:2890865782

# search_timeouts
search_cancel-timeouts:0

# search_vector_externing
search_vector_externing_deferred_entry_cnt:0
search_vector_externing_entry_count:0
search_vector_externing_generated_value_cnt:0
search_vector_externing_hash_extern_errors:0
search_vector_externing_lru_promote_cnt:0
search_vector_externing_num_lru_entries:0
search_network_bytes_out:0
search_network_bytes_in:0
*/
clusterSnapshot* createClusterSnapshot(const char *command, int num_fields, 
                                       infoFieldType *fields, int print_info) {
    clusterSnapshot *snapshot = zcalloc(sizeof(clusterSnapshot));
    snapshot->timestamp_ms = ustime() / 1000;
    snapshot->num_fields = num_fields;
    snapshot->fields = zcalloc(num_fields * sizeof(fieldSnapshot));
    snapshot->num_nodes = config.cluster_node_count;
    snapshot->node_identifiers = zcalloc(snapshot->num_nodes * sizeof(sds));
    
    /* Initialize fields */
    for (int i = 0; i < num_fields; i++) {
        snapshot->fields[i].field_name = fields[i].prefix_match;
        snapshot->fields[i].valid = 0;
        snapshot->fields[i].per_node_values = NULL;
        snapshot->fields[i].node_count = 0;
        
        if (fields[i].track_per_node) {
            snapshot->fields[i].per_node_values = zcalloc(snapshot->num_nodes * sizeof(long long));
            snapshot->fields[i].node_count = snapshot->num_nodes;
        }
    }
    
    /* Temporary storage for aggregation */
    void **field_opaques = zcalloc(num_fields * sizeof(void *));
    int *field_node_counts = zcalloc(num_fields * sizeof(int));

    /* Detect command type */
    int is_ftinfo = (strncasecmp(command, "FT.INFO", 7) == 0);
    // printf("Collecting data using command: %s is_ftinfo %s\n", command, is_ftinfo ? " (FT.INFO mode)" : "");
    /* Query each node */
    for (int node_idx = 0; node_idx < config.cluster_node_count; node_idx++) {
        clusterNode *node = config.cluster_nodes[node_idx];
        assert(node != NULL);
        int is_replica = node->replicate == NULL ? 0 : 1;
        /* Store node identifier */
        snapshot->node_identifiers[node_idx] = sdscatprintf(sdsempty(), 
                                                            "%s:%d", node->ip, node->port);
        
        valkeyContext *ctx = node->ctx ? node->ctx : getValkeyContext(config.ct, node->ip, node->port);
        assert(ctx != NULL);
        
        valkeyReply *reply = valkeyCommand(ctx, command);
        assert(reply != NULL);
        
        sds lines = NULL;
        
        if (is_ftinfo && reply->type == VALKEY_REPLY_ARRAY) {
            lines = convertFtInfoToLines(reply, NULL);
        } else if (reply->type == VALKEY_REPLY_STRING || reply->type == VALKEY_REPLY_STATUS) {
            lines = sdsnew(reply->str);
        }
        
        if (lines) {
            char *lines_copy = strdup(lines);
            char *saveptr;
            char *line = strtok_r(lines_copy, "\n", &saveptr);
            
            while (line != NULL) {
                /* Skip empty lines and comments */
                if (*line && *line != '#') {
                    for (int field_idx = 0; field_idx < num_fields; field_idx++) {
                        infoFieldType *field = &fields[field_idx];
                        
                        if (field->match(line, field->prefix_match)) {
                            char *colon = strchr(line, ':');
                            if (colon) {
                                char *value = colon + 1;
                                long long parsed_value = field->parse(value);
                                
                                /* Store per-node value if tracking */
                                if (field->track_per_node && 
                                    snapshot->fields[field_idx].per_node_values) {
                                    snapshot->fields[field_idx].per_node_values[node_idx] = parsed_value;
                                }
                                
                                /* Aggregate */
                                int is_last_node = (node_idx == config.cluster_node_count - 1);
                                if ((field->nodes_to_aggregate == FROM_ALL) || 
                                    (field->nodes_to_aggregate == FROM_PRIMARY_ONLY && !is_replica) || 
                                    (field->nodes_to_aggregate == FROM_REPLICA_ONLY && is_replica)) {
                                    field->agg(&field_opaques[field_idx], parsed_value, 
                                              node_idx, is_last_node);
                                    field_node_counts[field_idx]++;
                                }
                                
                                snapshot->fields[field_idx].valid = 1;
                            }
                            if (field->is_last) {
                                break; /* No need to check other fields */
                            }
                        }
                    }
                }
                line = strtok_r(NULL, "\n", &saveptr);
            }
            
            free(lines_copy);
            sdsfree(lines);
        }
        
        freeReplyObject(reply);
    }
    
    /* Store aggregated values */
    for (int field_idx = 0; field_idx < num_fields; field_idx++) {
        if (field_opaques[field_idx] && snapshot->fields[field_idx].valid) {
            /* Handle different opaque types */
            if (fields[field_idx].agg == aggregate_average) {
                typedef struct {
                    long long sum;
                    int count;
                } avg_state_t;
                avg_state_t *state = (avg_state_t *)field_opaques[field_idx];
                snapshot->fields[field_idx].value = state->count > 0 ? 
                    state->sum / state->count : 0;
            } else if (fields[field_idx].agg == aggregate_minmax) {
                typedef struct {
                    long long min_val;
                    long long max_val;
                    int initialized;
                } minmax_state_t;
                minmax_state_t *state = (minmax_state_t *)field_opaques[field_idx];
                snapshot->fields[field_idx].value = state->max_val;  /* Store max for diff */
            } else {
                snapshot->fields[field_idx].value = *(long long *)field_opaques[field_idx];
            }            
        }
    }
    if (print_info) {
        /* Display aggregated results */
        printf("\n========================================\n");
        printf("Cluster Aggregate: %s num_fields: %d\n", command, num_fields);
        printf("========================================\n");
        
        for (int field_idx = 0; field_idx < num_fields; field_idx++) {
            sds display_name = sdsempty();
            if (strcmp(fields[field_idx].name, "") == 0) {
                display_name = sdscatprintf(display_name, "%s", 
                                        fields[field_idx].prefix_match);
            } else {
                display_name = sdscatprintf(display_name, "%s.%s", 
                                        fields[field_idx].prefix_match, fields[field_idx].name);
            }            
            fields[field_idx].disp(display_name, 
                                    field_opaques[field_idx],
                                    snapshot->fields[field_idx].node_count);
            // print the distribution per-node if tracked using snapshot->fields[field_idx].per_node_values[node_idx]
            // show as single line distribution accross nodes
            if (fields[field_idx].track_per_node && snapshot->fields[field_idx].per_node_values) {
                printf(" | "); 
                printf("DIST:");
                for (int node_idx = 0; node_idx < snapshot->num_nodes; node_idx++) {                    
                    printf("[");
                    fields[field_idx].disp(snapshot->node_identifiers[node_idx], 
                                            &snapshot->fields[field_idx].per_node_values[node_idx],
                                            field_node_counts[field_idx]);
                    printf("]");
                }               
            } 
            printf("\n");
            sdsfree(display_name);
            
            if (field_opaques[field_idx]) {
                zfree(field_opaques[field_idx]);
            }
        }
    }
    zfree(field_opaques);
    return snapshot;
}

/* Free snapshot */
void freeClusterSnapshot(clusterSnapshot *snapshot) {
    if (!snapshot) return;
    
    for (int i = 0; i < snapshot->num_fields; i++) {
        // sdsfree(snapshot->fields[i].field_name);
        if (snapshot->fields[i].per_node_values) {
            zfree(snapshot->fields[i].per_node_values);
        }
    }
    
    for (int i = 0; i < snapshot->num_nodes; i++) {
        if (snapshot->node_identifiers[i]) {
            sdsfree(snapshot->node_identifiers[i]);
        }
    }
    
    zfree(snapshot->fields);
    zfree(snapshot->node_identifiers);
    zfree(snapshot);
}

/* Compare two snapshots and display differences */
void compareClusterSnapshots(clusterSnapshot *old, clusterSnapshot *new_snap,
                            int num_fields, infoFieldType *fields) {
    if (!old || !new_snap) {
        fprintf(stderr, "Cannot compare: missing snapshot\n");
        return;
    }
    
    long long time_delta_ms = new_snap->timestamp_ms - old->timestamp_ms;
    if (time_delta_ms <= 0) {
        fprintf(stderr, "Invalid time delta: %lld ms\n", time_delta_ms);
        return;
    }
    
    printf("\n========================================\n");
    printf("Cluster Statistics Delta\n");
    printf("========================================\n");
    printf("Time interval: %.2f seconds\n", (double)time_delta_ms / 1000.0);
    printf("From: %lld to %lld\n\n", old->timestamp_ms, new_snap->timestamp_ms);
    
    /* Compare each field */
    for (int field_idx = 0; field_idx < num_fields; field_idx++) {
        if (field_idx >= old->num_fields || field_idx >= new_snap->num_fields) {
            continue;
        }
        
        fieldSnapshot *old_field = &old->fields[field_idx];
        fieldSnapshot *new_field = &new_snap->fields[field_idx];
        
        if (!old_field->valid || !new_field->valid) {
            continue;
        }
        
        /* Ensure field names match */
        if (strcmp(old_field->field_name, new_field->field_name) != 0) {
            fprintf(stderr, "Field mismatch: %s vs %s\n", 
                   old_field->field_name, new_field->field_name);
            continue;
        }
        
        if (fields[field_idx].diff) {
            /* Display cluster-wide diff */
            fields[field_idx].diff(old_field->field_name, old_field, new_field, 
                                  time_delta_ms, -1);
            
            /* Display per-node diffs if available */
            if (fields[field_idx].track_per_node && old_field->per_node_values && 
                new_field->per_node_values) {
                printf("  Per-node breakdown:\n");
                for (int node_idx = 0; node_idx < old->num_nodes && 
                     node_idx < new_snap->num_nodes; node_idx++) {
                    if (sdscmp(old->node_identifiers[node_idx], 
                              new_snap->node_identifiers[node_idx]) == 0) {
                        fields[field_idx].diff(old_field->field_name, old_field, 
                                              new_field, time_delta_ms, node_idx);
                    }
                }
            }
        } else {
            /* Simple delta display */
            long long delta = new_field->value - old_field->value;
            printf("%s delta: %lld\n", old_field->field_name, delta);
        }
    }
    
    printf("========================================\n\n");
}

/* INFO SEARCH fields with temporal tracking */
infoFieldType search_info_fields[] = {
    /* Request rates */
    {"search_successful_requests_count", "", exact_field_matcher, 
     parse_integer_value, aggregate_sum, display_integer, diff_rate_per_second, 1, FROM_ALL, 1},
    {"search_failure_requests_count", "", exact_field_matcher, 
     parse_integer_value, aggregate_sum, display_integer, diff_rate_per_second, 1, FROM_ALL, 1},
    {"search_hybrid_requests_count", "", exact_field_matcher, 
     parse_integer_value, aggregate_sum, display_integer, diff_rate_per_second, 1, FROM_ALL, 1},

    /* Memory growth */
    {"search_used_memory_bytes", "", exact_field_matcher, 
     parse_integer_value, aggregate_sum, display_memory_mb, diff_memory_growth, 0, FROM_PRIMARY_ONLY, 1},
    {"search_index_reclaimable_memory", "", exact_field_matcher, 
     parse_integer_value, aggregate_sum, display_memory_mb, diff_memory_growth, 0, FROM_PRIMARY_ONLY, 1},
    /* Memory growth */
    {"search_ingest_field_vector", "", exact_field_matcher, 
     parse_integer_value, aggregate_sum, display_memory_mb, diff_memory_growth, 0, FROM_PRIMARY_ONLY, 1},
    /* Indexing rates */
    {"search_total_indexed_documents", "", exact_field_matcher, 
     parse_integer_value, aggregate_sum, display_integer, diff_rate_per_second, 1, FROM_ALL, 1},

    /* CPU usage */
    {"search_read_cpu_time_sec", "", exact_field_matcher, 
     parse_float_as_fixed, aggregate_sum, display_float, diff_rate_per_second, 1, FROM_ALL, 1},
    {"search_write_cpu_time_sec", "", exact_field_matcher, 
     parse_float_as_fixed, aggregate_sum, display_float, diff_rate_per_second, 1, FROM_PRIMARY_ONLY, 1},
    
    /* Queue sizes */
    {"search_query_queue_size", "", exact_field_matcher, 
     parse_integer_value, aggregate_average, display_integer, NULL, 1, FROM_ALL, 1},
    {"search_writer_queue_size", "", exact_field_matcher, 
     parse_integer_value, aggregate_average, display_integer, NULL, 1, FROM_PRIMARY_ONLY, 1},

    /* Latencies */
    {"search_hnsw_vector_index_search_latency_usec", "p50", exact_field_matcher, 
     parse_percentile_p50, aggregate_average, display_latency_usec, diff_latency_change, 1, FROM_ALL, 1},
    {"search_hnsw_vector_index_search_latency_usec", "p99", exact_field_matcher, 
     parse_percentile_p99, aggregate_average, display_latency_usec, diff_latency_change, 1, FROM_ALL, 1},
    {"search_hnsw_vector_index_search_latency_usec", "p999", exact_field_matcher, 
     parse_percentile_p999, aggregate_max, display_latency_usec, diff_latency_change, 1, FROM_ALL, 1},
    {"search_coordinator_server_search_index_partition_success_latency_usec", "p99",
     exact_field_matcher, parse_percentile_p99, aggregate_average, display_latency_usec, 
     diff_latency_change, 1, FROM_ALL, 1},
    /* Error rates */
    {"search_hnsw_add_exceptions_count", "", exact_field_matcher, 
     parse_integer_value, aggregate_sum, display_integer, diff_rate_per_second, 1, FROM_ALL, 1},
    {"search_bounds_check_errors", "", exact_field_matcher, 
     parse_integer_value, aggregate_sum, display_integer, diff_rate_per_second, 1, FROM_ALL, 1},
    {"search_num_hnsw_edges", "", exact_field_matcher, 
     parse_integer_value, aggregate_sum, display_integer, NULL, 0, FROM_PRIMARY_ONLY, 1},
    {"search_successful_requests_count", "", exact_field_matcher, 
     parse_integer_value, aggregate_sum, display_integer, NULL, 0, FROM_ALL, 1},
    {"search_number_of_indexes", "", exact_field_matcher, 
     parse_integer_value, aggregate_sum, display_integer, NULL, 0, FROM_PRIMARY_ONLY, 1},
    /* Track node count changes */
    {"search_num_hnsw_nodes", "", exact_field_matcher, 
     parse_integer_value, aggregate_sum, display_integer, diff_percentage_change, 0, FROM_PRIMARY_ONLY, 1},
    {"search_used_read_cpu", "", exact_field_matcher, 
     parse_float_as_fixed, aggregate_average, display_percentage, NULL, 1, FROM_ALL, 1},
    {"search_used_write_cpu", "", exact_field_matcher, 
     parse_float_as_fixed, aggregate_average, display_percentage, NULL, 1, FROM_PRIMARY_ONLY, 1},
    {"search_vectors_bytes", "", exact_field_matcher, 
     parse_integer_value, aggregate_sum, display_memory_human, NULL, 0, FROM_PRIMARY_ONLY, 1},
    {"search_interned_strings_memory", "", exact_field_matcher, 
     parse_integer_value, aggregate_sum, display_memory_human, NULL, 0, FROM_PRIMARY_ONLY, 1},
    /* Track maximum queue depth seen across nodes */
    {"search_query_queue_size", "", exact_field_matcher, 
     parse_integer_value, aggregate_max, display_integer, NULL, 0, FROM_ALL, 1}
};



/* FT.INFO fields with temporal tracking */
infoFieldType ftinfo_fields[] = {
    {"num_docs", "", exact_field_matcher, 
     parse_integer_value, aggregate_sum, display_integer, diff_rate_per_second, 1, FROM_PRIMARY_ONLY, 1},
    {"hash_indexing_failures", "", exact_field_matcher, 
     parse_integer_value, aggregate_sum, display_integer, diff_rate_per_second, 1, FROM_ALL, 1},
    {"mutation_queue_size", "", exact_field_matcher, 
     parse_integer_value, aggregate_average, display_integer, NULL, 1, FROM_PRIMARY_ONLY, 1},   
    {"num_records", "", exact_field_matcher, 
     parse_integer_value, aggregate_sum, display_integer, NULL, 0, FROM_PRIMARY_ONLY, 1},
    {"attributes.dim", "", exact_field_matcher, 
     parse_integer_value, aggregate_minmax, display_minmax, NULL, 0, FROM_PRIMARY_ONLY, 1},
    {"attributes.M", "", exact_field_matcher, 
     parse_integer_value, aggregate_minmax, display_minmax, NULL, 0, FROM_PRIMARY_ONLY, 1},
    {"attributes.capacity", "", exact_field_matcher, 
     parse_integer_value, aggregate_sum, display_integer, NULL, 0, FROM_PRIMARY_ONLY, 1},
    {"attributes.size", "", exact_field_matcher, 
     parse_integer_value, aggregate_sum, display_integer, NULL, 0, FROM_PRIMARY_ONLY, 1},
    {"mutation_queue_size", "", exact_field_matcher, 
     parse_integer_value, aggregate_average, display_integer, NULL, 0, FROM_PRIMARY_ONLY, 1}
};


infoFieldType info_fields[] = {
    /* If output were like "used_memory:1.5G" */
    {"used_memory", "", exact_field_matcher, 
     parse_memory_value, aggregate_sum, display_memory_mb, NULL, 0, FROM_PRIMARY_ONLY, 1},
    {"cmdstat_FT.SEARCH", "calls", prefix_field_matcher, 
     parse_cmdstat_calls, aggregate_sum, display_calls, diff_ftsearch_performance, 1, FROM_ALL, 0},
    {"cmdstat_FT.SEARCH", "usec", prefix_field_matcher, 
     parse_cmdstat_usec, aggregate_sum, display_usec, diff_ftsearch_performance, 1, FROM_ALL, 0},
    {"cmdstat_FT.SEARCH", "usec_per_call", prefix_field_matcher, 
     parse_cmdstat_usec_per_call, aggregate_average, display_usec_per_call, NULL, 1, FROM_ALL, 0},
    {"cmdstat_FT.SEARCH", "rejected", prefix_field_matcher, 
     parse_cmdstat_rejected, aggregate_sum, display_rejected, diff_rate_per_second, 1, FROM_ALL, 0},
    {"cmdstat_FT.SEARCH", "failed", prefix_field_matcher, 
     parse_cmdstat_failed, aggregate_sum, display_failed, diff_rate_per_second, 1, FROM_ALL, 1},
    {"cmdstat_hset", "calls", prefix_field_matcher, 
     parse_cmdstat_calls, aggregate_sum, display_calls, diff_ftsearch_performance, 1, FROM_PRIMARY_ONLY, 0},
    {"cmdstat_hset", "usec", prefix_field_matcher, 
     parse_cmdstat_usec, aggregate_sum, display_usec, diff_ftsearch_performance, 1, FROM_PRIMARY_ONLY, 0},
    {"cmdstat_hset", "usec_per_call", prefix_field_matcher, 
     parse_cmdstat_usec_per_call, aggregate_average, display_usec_per_call, NULL, 1, FROM_PRIMARY_ONLY, 0},
    {"cmdstat_hset", "rejected", prefix_field_matcher, 
     parse_cmdstat_rejected, aggregate_sum, display_rejected, diff_rate_per_second, 1, FROM_PRIMARY_ONLY, 0},
    {"cmdstat_hset", "failed", prefix_field_matcher, 
     parse_cmdstat_failed, aggregate_sum, display_failed, diff_rate_per_second, 1, FROM_PRIMARY_ONLY, 1}
};

// getMemoryInfoClusterGeneric();
void getFullInfo(const char *index_name) {
    long long search_memory = 0;
    long long search_reclaimable = 0;
    long long search_total_docs = 0;
    long long search_ingest_field_vector = 0;
    printf("\n------>\n");
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "FT.INFO %s", index_name);
    int ftinfo_num_fields = sizeof(ftinfo_fields) / sizeof(ftinfo_fields[0]);
    clusterSnapshot* ftinfo_snapshot = createClusterSnapshot(cmd, ftinfo_num_fields, ftinfo_fields, 1);

    int search_num_fields = sizeof(search_info_fields) / sizeof(search_info_fields[0]);
    clusterSnapshot* search_info_snapshot = createClusterSnapshot("INFO SEARCH", search_num_fields, search_info_fields, 1);

    int info_num_fields = sizeof(info_fields) / sizeof(info_fields[0]);
    clusterSnapshot* info_snapshot = createClusterSnapshot("INFO ALL", info_num_fields, info_fields, 1);

    for (int i = 0; i < search_info_snapshot->num_fields; i++) {
        if (search_info_snapshot->fields[i].valid) {
            if (sdscmp(search_info_snapshot->fields[i].field_name, "search_used_memory_bytes") == 0) {
                search_memory = search_info_snapshot->fields[i].value;
            } else if (sdscmp(search_info_snapshot->fields[i].field_name, "search_index_reclaimable_memory") == 0) {
                search_reclaimable = search_info_snapshot->fields[i].value;
            } else if (sdscmp(search_info_snapshot->fields[i].field_name, "search_total_indexed_documents") == 0) {
                search_total_docs = search_info_snapshot->fields[i].value;
            } else if (sdscmp(search_info_snapshot->fields[i].field_name, "search_ingest_field_vector") == 0) {
                search_ingest_field_vector = search_info_snapshot->fields[i].value;
            } 
            printf("> %s:%lld\n", info_snapshot->fields[i].field_name, info_snapshot->fields[i].value);
        }        
    }
    printf("> search_memory: %f MB, search_total_indexed_documents: %lld, search_reclaimable: %f MB, search_ingest_field_vector: %lld\n", search_memory / (1024.0 * 1024.0), search_total_docs, search_reclaimable / (1024.0 * 1024.0), search_ingest_field_vector);

    for (int i = 0; i < info_snapshot->num_fields; i++) {
        if (info_snapshot->fields[i].valid) {
            printf("> %s:%lld\n", info_snapshot->fields[i].field_name, info_snapshot->fields[i].value);
        }
    }
    for (int i = 0; i < ftinfo_snapshot->num_fields; i++) {
        if (ftinfo_snapshot->fields[i].valid) {
            printf("> %s:%lld\n", ftinfo_snapshot->fields[i].field_name, ftinfo_snapshot->fields[i].value);
        }
    }
    freeClusterSnapshot(ftinfo_snapshot);
    freeClusterSnapshot(search_info_snapshot);
    freeClusterSnapshot(info_snapshot);
    printf("------>\n");
}

clusterSnapshot* getSearchInfo(long long *search_memory, long long *search_reclaimable, 
                   long long *search_total_docs, long long *search_ingest_field_vector, 
                   long long *search_background_indexing_status) {
    int num_fields = sizeof(search_info_fields) / sizeof(search_info_fields[0]);
    clusterSnapshot* info_snapshot = createClusterSnapshot("INFO SEARCH", num_fields, search_info_fields, 0);
    /* Initialize aggregated values */
    *search_memory = 0;
    *search_reclaimable = 0;
    *search_total_docs = 0;
    *search_ingest_field_vector = 0;
    *search_background_indexing_status = 0;
    assert(info_snapshot);    
    for (int i = 0; i < info_snapshot->num_fields; i++) {
        if (info_snapshot->fields[i].valid) {
            if (sdscmp(info_snapshot->fields[i].field_name, "search_used_memory_bytes") == 0) {
                *search_memory = info_snapshot->fields[i].value;
            } else if (sdscmp(info_snapshot->fields[i].field_name, "search_index_reclaimable_memory") == 0) {
                *search_reclaimable = info_snapshot->fields[i].value;
            } else if (sdscmp(info_snapshot->fields[i].field_name, "search_total_indexed_documents") == 0) {
                *search_total_docs = info_snapshot->fields[i].value;
            } else if (sdscmp(info_snapshot->fields[i].field_name, "search_ingest_field_vector") == 0) {
                *search_ingest_field_vector = info_snapshot->fields[i].value;
            } else if (sdscmp(info_snapshot->fields[i].field_name, "search_background_indexing_status") == 0) {
                *search_background_indexing_status = info_snapshot->fields[i].value;
            }
            // printf("> %s:%lld\n", info_snapshot->fields[i].field_name, info_snapshot->fields[i].value);
        }

    }
    return info_snapshot;
}

clusterSnapshot* getInfoCluster(void) {
    int num_fields = sizeof(info_fields) / sizeof(info_fields[0]);
    clusterSnapshot* info_snapshot = createClusterSnapshot("INFO ALL", num_fields, info_fields, 0);
    // print the values in the snapshot
    assert(info_snapshot); 
    return info_snapshot;
}

/* Example 4: Get current FT.INFO statistics */
clusterSnapshot* getFtInfoStatistics(const char *index_name) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "FT.INFO %s", index_name);
    int num_fields = sizeof(ftinfo_fields) / sizeof(ftinfo_fields[0]);
    // getAggregatedClusterStats(cmd, num_fields, ftinfo_fields);
    clusterSnapshot* info_snapshot = createClusterSnapshot(cmd, num_fields, ftinfo_fields, 0);
    assert(info_snapshot); 
    for (int i = 0; i < info_snapshot->num_fields; i++) {
        if (info_snapshot->fields[i].valid) {
            printf("> %s:%lld", info_snapshot->fields[i].field_name, info_snapshot->fields[i].value);
        }
    }
    return info_snapshot;
}

void* compareInfoSnapshots(clusterSnapshot *old_infoall, clusterSnapshot *new_snap_infoall, clusterSnapshot *old_ftinfo, clusterSnapshot *new_snap_ftinfo, clusterSnapshot *old_infosearch, clusterSnapshot *new_snap_infosearch) {
    int ftinfo_num_fields = sizeof(ftinfo_fields) / sizeof(ftinfo_fields[0]);
    int search_num_fields = sizeof(search_info_fields) / sizeof(search_info_fields[0]);
    int info_num_fields = sizeof(info_fields) / sizeof(info_fields[0]);
    compareClusterSnapshots(old_infoall, new_snap_infoall, info_num_fields, info_fields);
    compareClusterSnapshots(old_ftinfo, new_snap_ftinfo, ftinfo_num_fields, ftinfo_fields);
    compareClusterSnapshots(old_infosearch, new_snap_infosearch, search_num_fields, search_info_fields);
    return NULL;
}