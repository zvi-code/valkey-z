/* Server benchmark utility.
 *
 * Copyright (c) 2009-2012, Redis Ltd.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "valkey-benchmark-utils.c"

/* Fast unique vector generation using key-based deterministic randomization */
static sds createVectorTemplate(uint64_t key_idx) {
    int dim = config.search.vector_dim - VECTOR_NUM_RAND_DIM;
    float *vector = zcalloc(config.search.vector_dim * sizeof(float));
    /* Use multiple hash passes for better distribution */
    uint64_t hash1 = key_idx * 0x9E3779B97F4A7C15ULL;
    uint64_t hash2 = key_idx * 0xBF58476D1CE4E5B9ULL;
    
    /* Generate full vector with mixed entropy sources */
    for (int i = 0; i < dim; i++) {
        /* Mix key_idx, dimension index, and hash values */
        uint64_t mixed = hash1 ^ (hash2 + i);
        mixed *= 0x94D049BB133111EBULL;
        mixed ^= mixed >> 31;
        mixed *= 0xBF58476D1CE4E5B9ULL;
        mixed ^= mixed >> 31;
        
        /* Convert to float in [-1, 1] with good distribution */
        uint32_t bits = (uint32_t)(mixed >> 32);
        vector[i] = (float)((int32_t)bits) / 2147483648.0f;
    }

    /* Optional: Normalize vector for cosine similarity
     NOT REALLY WORKING BEFORE REPLACEMENT */
    if (strcmp(config.search.metric, "COSINE") == 0) {
        float norm = 0.0f;
        for (int i = 0; i < dim; i++) {
            norm += vector[i] * vector[i];
        }
        norm = sqrtf(norm);
        if (norm > 0.0f) {
            for (int i = 0; i < dim; i++) {
                vector[i] /= norm;
            }
        }
    }
    /* Append the 8-byte placeholder (will be replaced in-place later) */
    memcpy(vector + dim, VECTOR_PLACEHOLDER, VECTOR_PLACEHOLDER_LEN); // Append placeholder for random part
    sds vector_data = sdsnewlen(vector, config.search.vector_dim * sizeof(float));
    /* Verify total size matches expected vector dimension */
    assert(sdslen(vector_data) == config.search.vector_dim * sizeof(float));
    zfree(vector);
    return vector_data;
}

/* Print FT.SEARCH results in a user-friendly format */
static void printSearchResults(valkeyReply *reply) {
    if (!reply || reply->type != VALKEY_REPLY_ARRAY) {
        printf("Invalid search result format\n");
        return;
    }
    
    if (reply->elements < 1) {
        printf("No search results\n");
        return;
    }
    
    /* First element is the total number of results */
    if (reply->element[0]->type == VALKEY_REPLY_INTEGER) {
        printf("\n=== Search Results (Total: %lld) ===\n", reply->element[0]->integer);
    }
    
    /* Results come in pairs: key, fields */
    for (size_t i = 1; i < reply->elements; i += 2) {
        if (i + 1 >= reply->elements) break;
        
        valkeyReply *keyReply = reply->element[i];
        valkeyReply *fieldsReply = reply->element[i + 1];
        
        /* Print the key */
        if ((keyReply->type == VALKEY_REPLY_STRING || keyReply->type == VALKEY_REPLY_STATUS)) {
            printf("\n  Result %zu: %s\n", (i + 1) / 2, keyReply->str);
        }
        
        /* Print the fields */
        if (fieldsReply && fieldsReply->type == VALKEY_REPLY_ARRAY) {
            for (size_t j = 0; j < fieldsReply->elements; j += 2) {
                if (j + 1 >= fieldsReply->elements) break;
                
                valkeyReply *fieldName = fieldsReply->element[j];
                valkeyReply *fieldValue = fieldsReply->element[j + 1];
                
                if ((fieldName->type == VALKEY_REPLY_STRING || fieldName->type == VALKEY_REPLY_STATUS) && (fieldValue->type == VALKEY_REPLY_STRING || fieldValue->type == VALKEY_REPLY_STATUS)) {
                    /* Check if it's a vector field (binary data) */
                    if (strstr(fieldName->str, "vector") != NULL || strstr(fieldName->str, "embedding") != NULL) {                        
                        printf("    %s: [binary vector data, %zu bytes]\n", fieldName->str, fieldValue->len);
                        // data is float32, print first 24 floats if available
                        if (fieldValue->len >= 96) { // 24 floats * 4 bytes each
                            printf("    First 24 floats: ");
                            for (size_t k = 0; k < 96; k += 4) {
                                float value;
                                memcpy(&value, fieldValue->str + k, sizeof(float));
                                printf("%f ", value);
                            }
                            printf("\n");
                        }
                    } else {
                        printf("    %s: %s\n", fieldName->str, fieldValue->str);
                    }
                }
            }
        }
    }
    printf("\n");
}

int isSelected(int is_primary) {
    if ((config.read_from_replica == FROM_REPLICA_ONLY && is_primary) || 
        ((config.read_from_replica == FROM_PRIMARY_ONLY) && !is_primary)) {
        return 0;
    }
    return 1;
}



static uint64_t dictSdsHash(const void *key) {
    return dictGenHashFunction((unsigned char *)key, sdslen((char *)key));
}

static int dictSdsKeyCompare(const void *key1, const void *key2) {
    int l1, l2;
    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

static dictType dtype = {
    dictSdsHash,       /* hash function */
    NULL,              /* key dup */
    dictSdsKeyCompare, /* key compare */
    NULL,              /* key destructor */
    NULL,              /* val destructor */
    NULL               /* allow to expand */
};

static valkeyContext *getValkeyContext(enum valkeyConnectionType ct, const char *ip_or_path, int port) {
    valkeyContext *ctx = NULL;
    valkeyReply *reply = NULL;
    struct timeval tv = {0};

    ctx = valkeyConnectWrapper(ct, ip_or_path, port, tv, 0, config.mptcp);
    printf("Connecting to %s", (ct != VALKEY_CONN_UNIX ? ip_or_path : ""));
    if (ct != VALKEY_CONN_UNIX) printf(":%d", port);
    printf("... ");
    fflush(stdout);
    if (ctx == NULL || ctx->err) {
        fprintf(stderr, "Could not connect to server at ");
        char *err = (ctx != NULL ? ctx->errstr : "");
        if (ct != VALKEY_CONN_UNIX)
            fprintf(stderr, "%s:%d: %s\n", ip_or_path, port, err);
        else
            fprintf(stderr, "%s: %s\n", ip_or_path, err);
        goto cleanup;
    }
    if (config.tls == 1) {
        printf("Negotiating TLS connection...\n");
        const char *err = NULL;
        if (cliSecureConnection(ctx, config.sslconfig, &err) == VALKEY_ERR && err) {
            fprintf(stderr, "Could not negotiate a TLS connection: %s\n", err);
            goto cleanup;
        }
    }
    if (config.conn_info.auth == NULL) return ctx;
    if (config.conn_info.user == NULL)
        reply = valkeyCommand(ctx, "AUTH %s", config.conn_info.auth);
    else
        reply = valkeyCommand(ctx, "AUTH %s %s", config.conn_info.user, config.conn_info.auth);
    if (reply != NULL) {
        if (reply->type == VALKEY_REPLY_ERROR) {
            if (ct != VALKEY_CONN_UNIX)
                fprintf(stderr, "Node %s:%d replied with error:\n%s\n", ip_or_path, port, reply->str);
            else
                fprintf(stderr, "Node %s replied with error:\n%s\n", ip_or_path, reply->str);
            freeReplyObject(reply);
            valkeyFree(ctx);
            exit(1);
        }
        freeReplyObject(reply);
        return ctx;
    }
    fprintf(stderr, "ERROR: failed to fetch reply from ");
    if (ct != VALKEY_CONN_UNIX)
        fprintf(stderr, "%s:%d\n", ip_or_path, port);
    else
        fprintf(stderr, "%s\n", ip_or_path);
cleanup:
    freeReplyObject(reply);
    valkeyFree(ctx);
    return NULL;
}

/* Fast vector generation using MT19937-64 */
static inline void generate_vector_fast(float *vector, unsigned int key_idx) {
    /* Use key index to seed global RNG */
    init_genrand64(key_idx * 2654435761U);
    
    /* Generate vector components */
    for (int i = 0; i < config.search.vector_dim; i++) {
        uint64_t r = genrand64_int64();
        /* Convert to float in range [-1, 1] */
        vector[i] = ((float)(r & 0x7FFFFFFF) / 0x40000000) - 1.0f;
    }
}

/* Initialize base vector for efficient generation */
static void initBaseVector(int dim) {
    if (base_vector && base_vector_dim != dim) {
        zfree(base_vector);
        base_vector = NULL;
    }
    
    if (!base_vector) {
        base_vector_dim = dim;
        base_vector = zcalloc(sizeof(float) * dim);
        
        /* Initialize with random values */
        init_genrand64(42); /* Fixed seed for reproducibility */
        
        for (int i = 0; i < dim; i++) {
            uint64_t r = genrand64_int64();
            base_vector[i] = ((float)(r & 0x7FFFFFFF) / 0x40000000) - 1.0f;
        }
    }
}

static sds getVectorKey(void) {
    sds key;
    if (config.cluster_mode) {
        key = sdscatprintf(sdsempty(), "%s{tag}:__rand_int__", config.search.prefix);
    } else {
        key = sdscatprintf(sdsempty(), "%s__rand_int__", config.search.prefix);
    }
    return key;
}



/* Benchmark function for vector operations with cluster awareness */
static int createVectorInsertCmdTemplate(char **cmd) {
    int len;   
    /* Generate key with appropriate cluster tag */
    sds key = getVectorKey();
    /* Validation checks */
    assert(config.search.vector_dim > 0 && config.use_search && config.search.vector_dim > VECTOR_NUM_RAND_DIM);    
    /* Build vector data: fixed part + placeholder */
    sds vector_binary = createVectorTemplate(0x736f6d6575736572); // "someusername" as base

    
    /* Build HSET command */
    if (config.search.tag_field && config.search.curr_conf.tag_dists) {
        sds selected_tag = selectTagByDistribution();
        len = valkeyFormatCommand(cmd, 
            "HSET %b %s %b %s %s", 
            key, sdslen(key), 
            config.search.vector_field,
            vector_binary, sdslen(vector_binary),
            config.search.tag_field, 
            selected_tag ? selected_tag : "");
        if (selected_tag) sdsfree(selected_tag);
    } else {
        len = valkeyFormatCommand(cmd, 
            "HSET %b %s %b", 
            key, sdslen(key), 
            config.search.vector_field,
            vector_binary, sdslen(vector_binary));
    }
    
    sdsfree(key);
    sdsfree(vector_binary);
    return len;
}

/* Benchmark function for vector operations with cluster awareness */
static int createSearchCmdTemplate(char **cmd) {
    int len;
    /* Validation checks */
    printf("Creating FT.SEARCH command template for index '%s' dimension %d/%ld k %d ef_search %d vector_field %s tag_field %s tag_filter %s nocontent %d\n", 
        config.search.name, config.search.vector_dim, VECTOR_NUM_RAND_DIM, 
        config.search.k, config.search.ef_search, config.search.vector_field, 
        config.search.tag_field, config.search.curr_conf.tag_filter, config.search.nocontent);
    assert(config.search.vector_dim > 0 && config.use_search && config.search.vector_dim > VECTOR_NUM_RAND_DIM);    
    /* Build vector data: fixed part + placeholder */
    sds vector_binary = createVectorTemplate(0x736f6d6575736572 ^ ((uint64_t)pthread_self() << 32)); // "someusername" as base
    
    /* Build KNN query */
    sds query;
    if (config.search.curr_conf.tag_filter && config.search.tag_field) {
        query = sdscatprintf(sdsempty(), 
            "@%s:{%s}=>[KNN %d @%s $query_vector EF_RUNTIME %d]", 
            config.search.tag_field, 
            config.search.curr_conf.tag_filter,
            config.search.k, 
            config.search.vector_field, 
            config.search.ef_search);
    } else {
        query = sdscatprintf(sdsempty(), 
            "*=>[KNN %d @%s $query_vector EF_RUNTIME %d]", 
            config.search.k, 
            config.search.vector_field, 
            config.search.ef_search);
    }
    
    /* Build FT.SEARCH command */
    if (config.search.nocontent) {
        len = valkeyFormatCommand(cmd, 
            "FT.SEARCH %b %b NOCONTENT PARAMS 2 query_vector %b DIALECT 2", 
            config.search.name, sdslen(config.search.name), 
            query, sdslen(query), 
            vector_binary, sdslen(vector_binary));
    } else {
        len = valkeyFormatCommand(cmd,
            "FT.SEARCH %b %b PARAMS 2 query_vector %b DIALECT 2", 
            config.search.name, sdslen(config.search.name),
            query, sdslen(query), 
            vector_binary, sdslen(vector_binary));
    }
    
    sdsfree(query);
    sdsfree(vector_binary);
    return len;
}

static void replacePlaceholderClusterTag(client c, const size_t *indices, const size_t count, char *cmd, _Atomic uint64_t *key_counter) {
    assert(c->thread_id >= 0);
    clusterNode *node = c->cluster_node;
    assert(node);
    int is_updating_slots = atomic_load_explicit(&config.is_updating_slots, 
                                                 memory_order_relaxed);                                                 
    if (is_updating_slots) updateClusterSlotsConfiguration();
    // update key with counter to ensure different keys
    uint64_t key_idx = atomic_fetch_add_explicit(key_counter, 1, memory_order_relaxed);
    assert(node->slots_count > 0);
    /* Select a random slot from this node */
    int slot = node->slots[key_idx % node->slots_count];
    const char *tag = crc16_slot_table[slot];
    int taglen = strlen(tag);
    assert(taglen <= 3); /* Ensure tag fits within placeholder */
    
    /* Replace all occurrences in-place (exactly 8 bytes) */
    for (size_t j = 0; j < count; j++) {
        char *placeholder = cmd + indices[j];    
        assert(placeholder[0] == '{');
        memcpy(placeholder + 1, tag, taglen);  // Copy tag
        placeholder[1 + taglen] = '}';         // Closing brace
        // Pad remaining bytes with random data if tag is shorter than 3 bytes
        if (taglen < 3) {
            for (int k = 0; k < 3 - taglen; k++) {
                placeholder[1 + taglen + 1 + k] = 'a' + (rand() % 26); // Random lowercase letter
            }
        }
    }
}


// TODO: If index already exists, we shuld check if it matches the current configuration.
// If it does not match, we should drop the index and recreate it.
// If it matches, we can skip index creation.
static void createDefaultSearchIndexes(void) {    
    if (!config.use_search) return;
    // connect to a primary node
    if (config.cluster_mode && config.cluster_primary_nodes[0]) {
        config.conn_info.hostip = config.cluster_primary_nodes[0]->ip;
        config.conn_info.hostport = config.cluster_primary_nodes[0]->port;
    }
    valkeyContext *ctx = config.conn_ctx;
    if (ctx == NULL) {
        fprintf(stderr, "No existing connection context, creating new\n");
        ctx = getValkeyContext(config.ct, config.conn_info.hostip, config.conn_info.hostport);
        if (ctx == NULL) {
            exit(1);
        }
    }
    /* Check if any indexes exist */
    valkeyReply *list_reply = valkeyCommand(ctx, "FT._LIST");
    int index_exists = 0;
    
    if (list_reply && list_reply->type == VALKEY_REPLY_ARRAY) {
        printf("Found %zu existing indexes: ", list_reply->elements);
        for (size_t j = 0; j < list_reply->elements; j++) {
            printf("found index '%s' ", list_reply->element[j]->str);
            if (strcmp(list_reply->element[j]->str, config.search.name) == 0) {
                index_exists = 1;
                getFullInfo(config.search.name);
            }            
        }
        printf("\n");
    } else {
        printf("Found 0 existing indexes: \n");
    }
    
    if (list_reply) {
        freeReplyObject(list_reply);
    }
    if (index_exists) {
        printf("Index '%s' already exists, skipping creation.\n", config.search.name);
        return;
    }
    valkeyReply *reply = NULL;
    /* Add TAG field if configured */
    if (config.search.tag_field) {
        reply = valkeyCommand(ctx, "FT.CREATE %s PREFIX 1 %s SCHEMA %s TAG %s VECTOR %s 12 TYPE FLOAT32 DIM %d DISTANCE_METRIC %s M %d EF_CONSTRUCTION %d EF_RUNTIME %d",
        config.search.name, config.search.prefix, config.search.tag_field, config.search.vector_field, config.search.algorithm, config.search.vector_dim, config.search.metric, config.search.m,
        config.search.ef_construction, config.search.ef_search);
    } else {
        reply = valkeyCommand(ctx, "FT.CREATE %s PREFIX 1 %s SCHEMA %s VECTOR %s 12 TYPE FLOAT32 DIM %d DISTANCE_METRIC %s M %d EF_CONSTRUCTION %d EF_RUNTIME %d",
        config.search.name, config.search.prefix, config.search.vector_field, config.search.algorithm, config.search.vector_dim, config.search.metric, config.search.m,
        config.search.ef_construction, config.search.ef_search);
    }
    
    if (reply && (reply->type == VALKEY_REPLY_STRING || reply->type == VALKEY_REPLY_STATUS)) {
        printf("Index created successfully\n");
    } else {
        fprintf(stderr, "Failed to create index: %s\n", 
                reply ? reply->str : "Unknown error");
        // if index already exists, we can ignore the error
        if (reply && reply->type == VALKEY_REPLY_ERROR && index_exists) {
            printf("Index '%s' already exists, ignoring error.\n", config.search.name);
        } else {
            fprintf(stderr, "Error creating index: %s\n", reply ? reply->str : "Unknown error");
            exit(1);
        }
    }
    if (reply) freeReplyObject(reply);            
}

/* Best-effort server config fetch: use INFO; skip CONFIG on managed services */
static void safeGetServerConfig(enum valkeyConnectionType ct, const char *host, int port, serverConfig *dst) {
    valkeyContext *ctx = getValkeyContext(ct, host, port);
    if (!ctx) return;

    /* 1) INFO server (non-privileged, should work on ElastiCache) */
    valkeyReply *r = valkeyCommand(ctx, "INFO SERVER");
    if (r && (r->type == VALKEY_REPLY_STRING || r->type == VALKEY_REPLY_STATUS)) {
        /* parse into dst if you want; or store raw */
        /* ... your existing parsing hook ... */
        freeReplyObject(r);
        r = NULL;
    } else if (r) { freeReplyObject(r); r = NULL; }

    /* 2) Optionally INFO memory (also non-privileged) */
    r = valkeyCommand(ctx, "INFO MEMORY");
    if (r && (r->type == VALKEY_REPLY_STRING || r->type == VALKEY_REPLY_STATUS)) {
        /* parse into dst if you want */
        freeReplyObject(r); r = NULL;
    } else if (r) { freeReplyObject(r); r = NULL; }

    valkeyFree(ctx);
}

static void freeServerConfig(serverConfig *cfg) {
    if (cfg->save) sdsfree(cfg->save);
    if (cfg->appendonly) sdsfree(cfg->appendonly);
    zfree(cfg);
}

void resetPlaceholders(void) {
    if (placeholders.index_data)
        zfree(placeholders.index_data); /* indices are a single contiguous allocation */
    memset(&placeholders, 0, sizeof(placeholders));
    for (size_t placeholder = 0; placeholder < PLACEHOLDER_NUM_OF; placeholder++) {
        placeholders.indices[placeholder] = NULL;
        placeholders.count[placeholder] = 0;
        /* Move past the placeholder - vector placeholder has different length */
        if (placeholder >= VECTOR_PLACEHOLDER_INDEX) {
            placeholders.len[placeholder] = VECTOR_PLACEHOLDER_LEN;
        } else if (placeholder == CLUSTER_PLACEHOLDER_INDEX) {
            placeholders.len[placeholder] = CLUSTER_PLACEHOLDER_LEN;
        } else {
            placeholders.len[placeholder] = PLACEHOLDER_NORMAL_LEN;
        }
               
    }
}

void initPlaceholders(const char *cmd, size_t cmd_len) {
    resetPlaceholders();
    placeholders.cmd_len = cmd_len;

    /* store placeholder locations in temp arrays */
    size_t total_count = 0;
    size_t *temp_indices[PLACEHOLDER_NUM_OF];
    for (size_t placeholder = 0; placeholder < PLACEHOLDER_NUM_OF; placeholder++) {
        size_t *count = &placeholders.count[placeholder];
        *count = 0;

        size_t temp_size = RANDPTR_INITIAL_SIZE;
        temp_indices[placeholder] = zcalloc(sizeof(size_t) * temp_size);
        const char *p = cmd;
        const char *end = cmd + cmd_len;
        while ((p = strstr(p, PLACEHOLDERS[placeholder])) != NULL && p < end) {
            if (*count == temp_size) {
                temp_size *= 2;
                temp_indices[placeholder] = zrealloc(temp_indices[placeholder], sizeof(size_t) * temp_size);
            }
            size_t index = p - cmd;
            temp_indices[placeholder][*count] = index;
            (*count)++;
            total_count++;
            /* Move past the placeholder - vector placeholder has different length */
            p += placeholders.len[placeholder];
        }
    }

    /* consolidate temp data into contiguous allocation */
    placeholders.index_data = zcalloc(sizeof(size_t) * total_count);
    size_t overall_index = 0;
    for (size_t placeholder = 0; placeholder < PLACEHOLDER_NUM_OF; placeholder++) {
        placeholders.indices[placeholder] = placeholders.index_data + overall_index;

        const size_t count = placeholders.count[placeholder];
        memcpy(placeholders.indices[placeholder], temp_indices[placeholder],
               sizeof(size_t) * count);
        overall_index += count;

        zfree(temp_indices[placeholder]);
    }
}

static void replacePlaceholder(const size_t *indices, const size_t count, char *cmd, _Atomic uint64_t *key_counter, unsigned placeholder_len) {
    if (count == 0) return;

    uint64_t key = 0;
    if (config.keyspacelen != 0) {
        if (config.sequential_replacement) {
            key = atomic_fetch_add_explicit(key_counter, 1, memory_order_relaxed);
        } else {
            key = random();
        }
        key %= config.keyspacelen;
    }

    /* convert key to string at first location */
    char *p = cmd + indices[0] + placeholder_len - 1;
    for (size_t j = 0; j < placeholder_len; j++) {
        *p = '0' + key % 10;
        key /= 10;
        p--;
    }

    /* copy the first instance to the other locations */
    for (size_t i = 1; i < count; i++) {
        char *placeholder = cmd + indices[i];
        memcpy(placeholder, cmd + indices[0], placeholder_len);
    }
}


static void replacePlaceholderVector(const size_t *indices, const size_t count, 
                                    char *cmd, _Atomic uint64_t *key_counter) {
    if (!config.use_search || count == 0) return;
    
    /* Self-check: ensure placeholder is exactly 8 bytes */
    assert(VECTOR_PLACEHOLDER_LEN == 8);
    
    /* Get key for randomization */
    uint64_t key = 0;
    if (config.keyspacelen != 0) {
        if (config.sequential_replacement) {
            key = atomic_load_explicit(key_counter, memory_order_relaxed);
        } else {
            key = random();
        }
        key %= config.keyspacelen;
    }
    
    /* Generate exactly 2 floats (8 bytes) */
    float vector[2];
    uint64_t state = key ? key : 0x123456789ABCDEF0ULL;
    
    for (int i = 0; i < 2; i++) {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        state *= 0x2545F4914F6CDD1DULL;
        
        uint32_t bits = (uint32_t)(state >> 32);
        vector[i] = ((float)(int32_t)bits) / 2147483648.0f;
    }
    
    /* Normalize if using COSINE metric */
    if (config.search.metric && strcmp(config.search.metric, "COSINE") == 0) {
        float norm = sqrtf(vector[0] * vector[0] + vector[1] * vector[1]);
        if (norm > 0.0f) {
            vector[0] /= norm;
            vector[1] /= norm;
        }
    }
    
    /* Replace all occurrences in-place (exactly 8 bytes) */
    for (size_t j = 0; j < count; j++) {
        char *placeholder = cmd + indices[j];        
        memcpy(placeholder, vector, VECTOR_PLACEHOLDER_LEN);  // Exactly 8 bytes replacement
    }
}

static void replacePlaceholders(client c, char *cmd_data, int cmd_count) {
    static _Atomic uint64_t seq_key[PLACEHOLDER_NUM_OF] = {0};

    for (int cmd_index = 0; cmd_index < cmd_count; cmd_index++) {
        char *cmd = cmd_data + cmd_index * placeholders.cmd_len;
        
        /* Handle __rand_int__ separately (multiple different values) */
        size_t *indices = placeholders.indices[0];
        _Atomic uint64_t *key_counter = &seq_key[0];
        for (size_t i = 0; i < placeholders.count[0]; i++) {
            replacePlaceholder(indices + i, 1, cmd, key_counter, placeholders.len[0]);
        }

        /* Handle other regular placeholders */
        for (size_t placeholder = 1; placeholder < PLACEHOLDER_NORMAL_NUM_OF; placeholder++) {
            indices = placeholders.indices[placeholder];
            size_t count = placeholders.count[placeholder];
            key_counter = &seq_key[placeholder];
            replacePlaceholder(indices, count, cmd, key_counter, placeholders.len[placeholder]);
        }
        if (placeholders.count[CLUSTER_PLACEHOLDER_INDEX] > 0) {
            indices = placeholders.indices[CLUSTER_PLACEHOLDER_INDEX];
            size_t count = placeholders.count[CLUSTER_PLACEHOLDER_INDEX];
            replacePlaceholderClusterTag(c, indices, count, cmd, 
                                   &seq_key[CLUSTER_PLACEHOLDER_INDEX]);
        }
        /* Handle vector placeholder */
        if (config.use_search && placeholders.count[VECTOR_PLACEHOLDER_INDEX] > 0) {
            indices = placeholders.indices[VECTOR_PLACEHOLDER_INDEX];
            size_t count = placeholders.count[VECTOR_PLACEHOLDER_INDEX];
            replacePlaceholderVector(indices, count, cmd, 
                                   &seq_key[VECTOR_PLACEHOLDER_INDEX]);
        }
    }
}

static void releasePausedClient(client c) {
    if (c->thread_id >= 0) {
        benchmarkThread *thread = config.threads[c->thread_id];
        listNode *ln = listSearchKey(thread->paused_clients, c);
        if (ln != NULL) {
            listDelNode(thread->paused_clients, ln);
        }
    } else {
        listNode *ln = listSearchKey(config.paused_clients, c);
        if (ln != NULL) {
            listDelNode(config.paused_clients, ln);
        }
    }
}

static void freeClient(client c) {
    aeEventLoop *el = CLIENT_GET_EVENTLOOP(c);
    listNode *ln;
    aeDeleteFileEvent(el, c->context->fd, AE_WRITABLE);
    aeDeleteFileEvent(el, c->context->fd, AE_READABLE);
    if (c->thread_id >= 0) {
        int requests_finished = atomic_load_explicit(&config.requests_finished, memory_order_relaxed);
        if (requests_finished >= config.requests) {
            aeStop(el);
        }
    }
    valkeyFree(c->context);
    if (c->paused) releasePausedClient(c);
    sdsfree(c->obuf);
    zfree(c->stagptr);
    zfree(c);
    if (config.num_threads) pthread_mutex_lock(&(config.liveclients_mutex));
    config.liveclients--;
    ln = listSearchKey(config.clients, c);
    assert(ln != NULL);
    listDelNode(config.clients, ln);
    if (config.num_threads) pthread_mutex_unlock(&(config.liveclients_mutex));
}

static void freeAllClients(void) {
    listNode *ln = config.clients->head, *next;

    while (ln) {
        next = ln->next;
        freeClient(ln->value);
        ln = next;
    }
}

static void resetClient(client c) {
    aeEventLoop *el = CLIENT_GET_EVENTLOOP(c);
    aeDeleteFileEvent(el, c->context->fd, AE_WRITABLE);
    aeDeleteFileEvent(el, c->context->fd, AE_READABLE);
    if (config.ct == VALKEY_CONN_RDMA) {
        writeHandler(el, c->context->fd, c, 0); /* RDMA context always writable, but it can't be invoked by AE_WRITABLE */
    } else {
        aeCreateFileEvent(el, c->context->fd, AE_WRITABLE, writeHandler, c);
    }
    c->written = 0;
    c->pending = config.pipeline * c->seqlen;
}

/* Acquires the specified number of tokens from the token bucket or calculates the wait time if tokens are not available.
 * This function implements a token bucket rate limiting algorithm to control access to a resource.
 *
 * The tokens parameter is the number of tokens to acquire.
 *
 * Returns the delay time in milliseconds that the caller should wait before proceeding, or 0 if tokens are immediately available.
 *
 * Token Bucket Algorithm Explanation:
 * - The token bucket algorithm allows a certain number of tokens to be accumulated over time, which can then be used to control the rate of requests.
 * - Due to the time event only allowing a delay of 1ms, a request for the next 1ms is issued.
 *
 * The function is thread-safe. */
static long long acquireTokenOrWait(int tokens) {
    uint64_t time_per_token = config.time_per_token;
    uint64_t time_per_burst = config.time_per_burst;
    uint64_t new_time = 0;
    uint64_t now_epoch, next_epoch, min_time, delay_time;
    uint64_t last_time_ns, old_last_time_ns;

    while (1) {
        old_last_time_ns = atomic_load_explicit(&config.last_time_ns, memory_order_relaxed);
        last_time_ns = old_last_time_ns;
        now_epoch = nstime();

        // If the last_time_ns is 0, it means this is the first request, so we set it to now_epoch.
        if (last_time_ns == 0) {
            last_time_ns = now_epoch;
        }

        next_epoch = now_epoch + 1000000;
        min_time = next_epoch - time_per_burst;

        if (min_time > last_time_ns) { // if the last time is too old, reset it
            new_time = min_time + (time_per_token * tokens);
        } else {
            new_time = last_time_ns + (time_per_token * tokens);
        }

        delay_time = 0;
        if (new_time > next_epoch) { // if the new time is in the next epoch, we need to wait
            delay_time = new_time - now_epoch;
        } else {
            last_time_ns = new_time;
        }

        if (atomic_compare_exchange_weak_explicit(
                &config.last_time_ns,
                &old_last_time_ns,
                last_time_ns,
                memory_order_release,
                memory_order_relaxed)) {
            break;
        }
    }

    return delay_time / 1000000;
}

static void clientDone(client c) {
    int requests_finished = atomic_load_explicit(&config.requests_finished, memory_order_relaxed);
    if (requests_finished >= config.requests) {
        freeClient(c);
        if (!config.num_threads && config.el) aeStop(config.el);
        return;
    }
    if (config.keepalive) {
        resetClient(c);
    } else {
        if (config.num_threads) pthread_mutex_lock(&(config.liveclients_mutex));
        config.liveclients--;
        createMissingClients(c);
        config.liveclients++;
        if (config.num_threads) pthread_mutex_unlock(&(config.liveclients_mutex));
        freeClient(c);
    }
}

static void readHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    client c = privdata;
    void *reply = NULL;
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);

    /* Calculate latency only for the first read event. This means that the
     * server already sent the reply and we need to parse it. Parsing overhead
     * is not part of the latency, so calculate it only once, here. */
    if (c->latency < 0) c->latency = ustime() - (c->start);

    if (valkeyBufferRead(c->context) != VALKEY_OK) {
        fprintf(stderr, "Error: %s\n", c->context->errstr);
        exit(1);
    } else {
        while (c->pending) {
            if (valkeyGetReply(c->context, &reply) != VALKEY_OK) {
                fprintf(stderr, "Error: %s\n", c->context->errstr);
                exit(1);
            }
            if (reply != NULL) {
                if (reply == (void *)VALKEY_REPLY_ERROR) {
                    fprintf(stderr, "Unexpected error reply, exiting...\n");
                    exit(1);
                }
                valkeyReply *r = reply;
                if (r->type == VALKEY_REPLY_ERROR) {
                    /* Try to update slots configuration if reply error is
                     * MOVED/ASK/CLUSTERDOWN and the key(s) used by the command
                     * contain(s) the slot hash tag.
                     * If the error is not topology-update related then we
                     * immediately exit to avoid false results. */
                    if (c->cluster_node && c->staglen) {
                        int fetch_slots = 0, do_wait = 0;
                        if (!strncmp(r->str, "MOVED", 5) || !strncmp(r->str, "ASK", 3))
                            fetch_slots = 1;
                        else if (!strncmp(r->str, "CLUSTERDOWN", 11)) {
                            /* Usually the cluster is able to recover itself after
                             * a CLUSTERDOWN error, so try to sleep one second
                             * before requesting the new configuration. */
                            fetch_slots = 1;
                            do_wait = 1;
                            fprintf(stderr, "Error from server %s:%d: %s.\n", c->cluster_node->ip,
                                    c->cluster_node->port, r->str);
                        }
                        if (do_wait) sleep(1);
                        if (fetch_slots && !fetchClusterSlotsConfiguration(c)) exit(1);
                    } else {
                        if (c->cluster_node) {
                            fprintf(stderr, "Error from server %s:%d: %s\n", c->cluster_node->ip, c->cluster_node->port,
                                    r->str);
                        } else
                            fprintf(stderr, "Error from server: %s\n", r->str);
                        exit(1);
                    }
                }
                if (config.print_search_results) {
                    printSearchResults(reply);
                }
                freeReplyObject(reply);
                /* This is an OK for prefix commands such as auth and select.*/
                if (c->prefix_pending > 0) {
                    c->prefix_pending--;
                    c->pending--;
                    /* Discard prefix commands on first response.*/
                    if (c->prefixlen > 0) {
                        size_t j;
                        sdsrange(c->obuf, c->prefixlen, -1);
                        /* Fix the pointers to the slot hash tags */
                        for (j = 0; j < c->staglen; j++) c->stagptr[j] -= c->prefixlen;
                        c->prefixlen = 0;
                    }
                    continue;
                }
                int requests_finished = atomic_fetch_add_explicit(&config.requests_finished, 1, memory_order_relaxed);
                if (requests_finished < config.requests) {
                    if (config.num_threads == 0) {
                        hdr_record_value(config.latency_histogram, // Histogram to record to
                                         (long)c->latency <= CONFIG_LATENCY_HISTOGRAM_MAX_VALUE
                                             ? (long)c->latency
                                             : CONFIG_LATENCY_HISTOGRAM_MAX_VALUE); // Value to record
                        hdr_record_value(config.current_sec_latency_histogram,      // Histogram to record to
                                         (long)c->latency <= CONFIG_LATENCY_HISTOGRAM_INSTANT_MAX_VALUE
                                             ? (long)c->latency
                                             : CONFIG_LATENCY_HISTOGRAM_INSTANT_MAX_VALUE); // Value to record
                    } else {
                        hdr_record_value_atomic(config.latency_histogram, // Histogram to record to
                                                (long)c->latency <= CONFIG_LATENCY_HISTOGRAM_MAX_VALUE
                                                    ? (long)c->latency
                                                    : CONFIG_LATENCY_HISTOGRAM_MAX_VALUE); // Value to record
                        hdr_record_value_atomic(config.current_sec_latency_histogram,      // Histogram to record to
                                                (long)c->latency <= CONFIG_LATENCY_HISTOGRAM_INSTANT_MAX_VALUE
                                                    ? (long)c->latency
                                                    : CONFIG_LATENCY_HISTOGRAM_INSTANT_MAX_VALUE); // Value to record
                    }
                }
                c->pending--;
                if (c->pending == 0) {
                    clientDone(c);
                    break;
                }
            } else {
                break;
            }
        }
    }
}

/*
 * When a client is paused, the function is called by the event loop to
 * awaken the client.
 *
 * Return the number of milliseconds to wait before calling the function again.
 *
 * If the function returns AE_NOMORE, the event is removed.
 */
static long long awakenPausedClient(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    UNUSED(id);
    benchmarkThread *thread = (benchmarkThread *)clientData;

    list *paused_clients = NULL;
    if (thread == NULL) {
        paused_clients = config.paused_clients;
    } else {
        paused_clients = thread->paused_clients;
    }

    listIter li;
    listNode *ln;
    long long delay = 0;
    listRewind(paused_clients, &li);
    while ((ln = listNext(&li)) != NULL) {
        client c = ln->value;
        delay = acquireTokenOrWait(config.pipeline);
        if (delay) {
            break;
        }
        // When client acquires a token, try to write with `reuse`.
        c->paused = 0;
        c->reuse = 1;
        writeHandler(eventLoop, c->context->fd, c, AE_WRITABLE);
        listDelNode(paused_clients, ln);
    }

    // If there are no more paused clients, remove the event.
    if (delay == 0) {
        return AE_NOMORE;
    }
    return delay;
}

static void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    client c = privdata;
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);

    // When benchmark with rps control, and client is not reuse, try to acquire a token.
    if (config.rps > 0 && c->reuse == 0) {
        /* Acquire a token from the token bucket. */
        long long delay = acquireTokenOrWait(config.pipeline);

        if (delay) {
            int thread_id = c->thread_id;
            int paused_clients_count = 0;

            c->paused = 1;
            aeDeleteFileEvent(el, c->context->fd, AE_WRITABLE);

            benchmarkThread *thread = NULL;
            if (thread_id < 0) {
                paused_clients_count = listLength(config.paused_clients);
                listAddNodeTail(config.paused_clients, c);
            } else {
                thread = config.threads[thread_id];
                paused_clients_count = listLength(thread->paused_clients);
                listAddNodeTail(thread->paused_clients, c);
            }
            if (paused_clients_count == 0) {
                /* Create a time event to awaken the client. */
                aeCreateTimeEvent(el, delay, awakenPausedClient, (void *)thread, NULL);
            }
            return;
        }
    }
    c->reuse = 0;

    /* Initialize request when nothing was written. */
    if (c->written == 0) {
        /* Enforce upper bound to number of requests. */
        int requests_issued = atomic_fetch_add_explicit(&config.requests_issued,
                                                        config.pipeline * c->seqlen,
                                                        memory_order_relaxed);
        if (requests_issued >= config.requests) {
            return;
        }

        /* Really initialize: replace keys and set start time. */
        replacePlaceholders(c, c->obuf + c->prefixlen, config.pipeline);
        c->slots_last_update = atomic_load_explicit(&config.slots_last_update, memory_order_relaxed);
        c->start = ustime();
        c->latency = -1;
    }
    const ssize_t buflen = sdslen(c->obuf);
    const ssize_t writeLen = buflen - c->written;
    if (writeLen > 0) {
        void *ptr = c->obuf + c->written;
        while (1) {
            /* Optimistically try to write before checking if the file descriptor
             * is actually writable. At worst we get EAGAIN. */
            const ssize_t nwritten = cliWriteConn(c->context, ptr, writeLen);
            if (nwritten != writeLen) {
                if (nwritten == -1 && errno != EAGAIN) {
                    if (errno != EPIPE) fprintf(stderr, "Error writing to the server: %s\n", strerror(errno));
                    freeClient(c);
                    return;
                } else if (nwritten > 0) {
                    c->written += nwritten;
                    return;
                }
            } else {
                aeDeleteFileEvent(el, c->context->fd, AE_WRITABLE);
                aeCreateFileEvent(el, c->context->fd, AE_READABLE, readHandler, c);
                return;
            }
        }
    }
}

/* Create a benchmark client, configured to send the command passed as 'cmd' of
 * 'len' bytes.
 *
 * The command is copied N times in the client output buffer (that is reused
 * again and again to send the request to the server) accordingly to the configured
 * pipeline size.
 *
 * Also an initial SELECT command is prepended in order to make sure the right
 * database is selected, if needed. The initial SELECT will be discarded as soon
 * as the first reply is received.
 *
 * To create a client from scratch, the 'from' pointer is set to NULL. If instead
 * we want to create a client using another client as reference, the 'from' pointer
 * points to the client to use as reference. In such a case the following
 * information is take from the 'from' client:
 *
 * 1) The command line to use.
 * 2) The offsets of the __rand_int__ elements inside the command line, used
 *    for arguments randomization.
 *
 * Even when cloning another client, prefix commands are applied if needed.*/
static client createClient(char *cmd, int len, int seqlen, client from, int thread_id) {
    int is_cluster_client = (config.cluster_mode && thread_id >= 0);
    client c = zcalloc(sizeof(struct _client));

    const char *ip = config.conn_info.hostip;
    int port = config.conn_info.hostport;
    struct timeval tv = {0};
    if (config.selected_node_count > 0) {
        /* If the user specified a list of nodes, use them in a round-robin
         * fashion. */
        int node_idx = 0;
        if (config.num_threads < config.selected_node_count)
            node_idx = (config.liveclients + 10007) % config.selected_node_count;
        else
            node_idx = (ustime() + thread_id) % config.selected_node_count;
        clusterNode *node = config.selected_nodes[node_idx];
        assert(node != NULL);
        ip = node->ip;
        port = node->port;
        c->cluster_node = node;
    } 

    c->context = valkeyConnectWrapper(config.ct, ip, port, tv, 1, config.mptcp);
    if (c->context->err) {
        fprintf(stderr, "Could not connect to server at ");
        if (config.ct != VALKEY_CONN_UNIX || is_cluster_client)
            fprintf(stderr, "%s:%d: %s\n", ip, port, c->context->errstr);
        else
            fprintf(stderr, "%s: %s\n", ip, c->context->errstr);
        exit(1);
    }
    if (config.tls == 1) {
        const char *err = NULL;
        if (cliSecureConnection(c->context, config.sslconfig, &err) == VALKEY_ERR && err) {
            fprintf(stderr, "Could not negotiate a TLS connection: %s\n", err);
            exit(1);
        }
    }
    c->paused = 0;
    c->reuse = 0;
    c->thread_id = thread_id;
    /* Suppress libvalkey cleanup of unused buffers for max speed. */
    c->context->reader->maxbuf = 0;

    /* Build the request buffer:
     * Queue N requests accordingly to the pipeline size, or simply clone
     * the example client buffer. */
    c->obuf = sdsempty();
    /* Prefix the request buffer with AUTH and/or SELECT commands, if applicable.
     * These commands are discarded after the first response, so if the client is
     * reused the commands will not be used again. */
    c->prefix_pending = 0;
    if (config.conn_info.auth) {
        char *buf = NULL;
        int len;
        if (config.conn_info.user == NULL)
            len = valkeyFormatCommand(&buf, "AUTH %s", config.conn_info.auth);
        else
            len = valkeyFormatCommand(&buf, "AUTH %s %s", config.conn_info.user, config.conn_info.auth);
        c->obuf = sdscatlen(c->obuf, buf, len);
        free(buf);
        c->prefix_pending++;
    }

    if (config.enable_tracking) {
        char *buf = NULL;
        int len = valkeyFormatCommand(&buf, "CLIENT TRACKING on");
        c->obuf = sdscatlen(c->obuf, buf, len);
        free(buf);
        c->prefix_pending++;
    }

    /* If a DB number different than zero is selected, prefix our request
     * buffer with the SELECT command, that will be discarded the first
     * time the replies are received, so if the client is reused the
     * SELECT command will not be used again. */
    if (config.conn_info.input_dbnum) {
        c->obuf = sdscatprintf(c->obuf, "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n", (int)sdslen(config.input_dbnumstr),
                               config.input_dbnumstr);
        c->prefix_pending++;
    }

    if (config.resp3) {
        char *buf = NULL;
        int len = valkeyFormatCommand(&buf, "HELLO 3");
        c->obuf = sdscatlen(c->obuf, buf, len);
        free(buf);
        c->prefix_pending++;
    }

    if (config.cluster_mode && (config.read_from_replica == FROM_REPLICA_ONLY || config.read_from_replica == FROM_ALL)) {
        char *buf = NULL;
        int len;
        len = valkeyFormatCommand(&buf, "READONLY");
        c->obuf = sdscatlen(c->obuf, buf, len);
        free(buf);
        c->prefix_pending++;
    }

    c->prefixlen = sdslen(c->obuf);
    /* Append the request itself. */
    if (from) {
        c->obuf = sdscatlen(c->obuf, from->obuf + from->prefixlen, sdslen(from->obuf) - from->prefixlen);
        seqlen = from->seqlen;
    } else {
        for (int j = 0; j < config.pipeline; j++) c->obuf = sdscatlen(c->obuf, cmd, len);
    }

    c->written = 0;
    c->seqlen = seqlen;
    c->pending = config.pipeline * seqlen + c->prefix_pending;
    c->stagptr = NULL;
    c->staglen = 0;

    /* If cluster mode is enabled, set slot hashtags pointers. */
    if (config.cluster_mode) {
        if (from) {
            c->staglen = from->staglen;
            c->stagfree = 0;
            c->stagptr = zcalloc(sizeof(char *) * c->staglen);
            /* copy the offsets. */
            for (size_t j = 0; j < c->staglen; j++) {
                c->stagptr[j] = c->obuf + (from->stagptr[j] - from->obuf);
                /* Adjust for the different select prefix length. */
                c->stagptr[j] += c->prefixlen - from->prefixlen;
            }
        } else {
            char *p = c->obuf;

            c->staglen = 0;
            c->stagfree = RANDPTR_INITIAL_SIZE;
            c->stagptr = zcalloc(sizeof(char *) * c->stagfree);
            while ((p = strstr(p, "{tag}")) != NULL) {
                if (c->stagfree == 0) {
                    c->stagptr = zrealloc(c->stagptr, sizeof(char *) * c->staglen * 2);
                    c->stagfree += c->staglen;
                }
                c->stagptr[c->staglen++] = p;
                c->stagfree--;
                p += 5; /* 5 is strlen("{tag}"). */
            }
        }
    }
    aeEventLoop *el = NULL;
    if (thread_id < 0)
        el = config.el;
    else {
        benchmarkThread *thread = config.threads[thread_id];
        el = thread->el;
    }
    if (config.idlemode == 0) {
        if (config.ct == VALKEY_CONN_RDMA) {
            writeHandler(el, c->context->fd, c, 0);
        } else {
            aeCreateFileEvent(el, c->context->fd, AE_WRITABLE, writeHandler, c);
        }
    } else
        /* In idle mode, clients still need to register readHandler for catching errors */
        aeCreateFileEvent(el, c->context->fd, AE_READABLE, readHandler, c);

    listAddNodeTail(config.clients, c);
    atomic_fetch_add_explicit(&config.liveclients, 1, memory_order_relaxed);

    c->slots_last_update = atomic_load_explicit(&config.slots_last_update, memory_order_relaxed);
    return c;
}

static void createMissingClients(client c) {
    int n = 0;
    while (config.liveclients < config.numclients) {
        int thread_id = -1;
        if (config.num_threads) thread_id = config.liveclients % config.num_threads;
        createClient(NULL, 0, 0, c, thread_id);

        /* Listen backlog is quite limited on most systems */
        if (++n > 64) {
            usleep(50000);
            n = 0;
        }
    }
}

static void showLatencyReport(void) {
    const float reqpersec = (float)config.requests_finished / ((float)config.totlatency / 1000.0f);
    const float p0 = ((float)hdr_min(config.latency_histogram)) / 1000.0f;
    const float p50 = hdr_value_at_percentile(config.latency_histogram, 50.0) / 1000.0f;
    const float p95 = hdr_value_at_percentile(config.latency_histogram, 95.0) / 1000.0f;
    const float p99 = hdr_value_at_percentile(config.latency_histogram, 99.0) / 1000.0f;
    const float p100 = ((float)hdr_max(config.latency_histogram)) / 1000.0f;
    const float avg = hdr_mean(config.latency_histogram) / 1000.0f;

    if (!config.quiet && !config.csv) {
        printf("%*s\r", config.last_printed_bytes, " "); // ensure there is a clean line
        printf("====== %s ======\n", config.title);      
        printf("  %d requests completed in %.2f seconds\n", config.requests_finished, (float)config.totlatency / 1000);
        printf("  %d parallel clients\n", config.numclients);
        printf("  %d bytes payload\n", config.datasize);
        printf("  keep alive: %d\n", config.keepalive);
        if (config.cluster_mode) {
            const char *node_roles = NULL;
            if (config.read_from_replica == FROM_ALL) {
                node_roles = "cluster";
            } else if (config.read_from_replica == FROM_REPLICA_ONLY) {
                node_roles = "replica";
            } else {
                node_roles = "primary";
            }
            printf("  cluster mode: yes (%d %s)\n", config.cluster_node_count, node_roles);
            int m;
            for (m = 0; m < config.cluster_node_count; m++) {
                clusterNode *node = config.cluster_nodes[m];
                serverConfig *cfg = node->server_config;
                if (cfg == NULL) continue;
                printf("  node [%d] configuration:\n", m);
                printf("    save: %s\n", sdslen(cfg->save) ? cfg->save : "NONE");
                printf("    appendonly: %s\n", cfg->appendonly);
            }
        } else {
            if (config.server_config) {
                printf("  host configuration \"save\": %s\n", config.server_config->save);
                printf("  host configuration \"appendonly\": %s\n", config.server_config->appendonly);
            }
        }
        printf("  multi-thread: %s\n", (config.num_threads ? "yes" : "no"));
        if (config.num_threads) printf("  threads: %d\n", config.num_threads);

        printf("\n");
        printf("Latency by percentile distribution:\n");
        struct hdr_iter iter;
        long long previous_cumulative_count = -1;
        const long long total_count = config.latency_histogram->total_count;
        hdr_iter_percentile_init(&iter, config.latency_histogram, 1);
        struct hdr_iter_percentiles *percentiles = &iter.specifics.percentiles;
        while (hdr_iter_next(&iter)) {
            const double value = iter.highest_equivalent_value / 1000.0f;
            const double percentile = percentiles->percentile;
            const long long cumulative_count = iter.cumulative_count;
            if (previous_cumulative_count != cumulative_count || cumulative_count == total_count) {
                printf("%3.3f%% <= %.3f milliseconds (cumulative count %lld)\n", percentile, value, cumulative_count);
            }
            previous_cumulative_count = cumulative_count;
        }
        printf("\n");
        printf("Cumulative distribution of latencies:\n");
        previous_cumulative_count = -1;
        hdr_iter_linear_init(&iter, config.latency_histogram, 100);
        while (hdr_iter_next(&iter)) {
            const double value = iter.highest_equivalent_value / 1000.0f;
            const long long cumulative_count = iter.cumulative_count;
            const double percentile = ((double)cumulative_count / (double)total_count) * 100.0;
            if (previous_cumulative_count != cumulative_count || cumulative_count == total_count) {
                printf("%3.3f%% <= %.3f milliseconds (cumulative count %lld)\n", percentile, value, cumulative_count);
            }
            /* After the 2 milliseconds latency to have percentages split
             * by decimals will just add a lot of noise to the output. */
            if (iter.highest_equivalent_value > 2000) {
                hdr_iter_linear_set_value_units_per_bucket(&iter, 1000);
            }
            previous_cumulative_count = cumulative_count;
        }
        printf("\n");
        printf("Summary:\n");
        printf("  throughput summary: %.2f requests per second\n", reqpersec);
        printf("  latency summary (msec):\n");
        printf("    %9s %9s %9s %9s %9s %9s\n", "avg", "min", "p50", "p95", "p99", "max");
        printf("    %9.3f %9.3f %9.3f %9.3f %9.3f %9.3f\n", avg, p0, p50, p95, p99, p100);
    } else if (config.csv) {
        printf("\"%s\",\"%.2f\",\"%.3f\",\"%.3f\",\"%.3f\",\"%.3f\",\"%.3f\",\"%.3f\"\n", config.title, reqpersec, avg,
               p0, p50, p95, p99, p100);
    } else {
        printf("%*s\r", config.last_printed_bytes, " "); // ensure there is a clean line
        printf("%s: %.2f requests per second, p50=%.3f msec\n", config.title, reqpersec, p50);
    }
}

static void initBenchmarkThreads(void) {
    int i;
    if (config.threads) freeBenchmarkThreads();
    config.threads = zcalloc(config.num_threads * sizeof(benchmarkThread *));
    for (i = 0; i < config.num_threads; i++) {
        benchmarkThread *thread = createBenchmarkThread(i);
        config.threads[i] = thread;
    }
}

static void startBenchmarkThreads(void) {
    int i;
    for (i = 0; i < config.num_threads; i++) {
        benchmarkThread *t = config.threads[i];
        if (pthread_create(&(t->thread), NULL, execBenchmarkThread, t)) {
            fprintf(stderr, "FATAL: Failed to start thread %d.\n", i);
            exit(1);
        }
    }
    for (i = 0; i < config.num_threads; i++) pthread_join(config.threads[i]->thread, NULL);
}

/* Benchmark a sequence of commands. The cmd is RESP encoded of length len and
 * seqlen is the number of commands included in cmd. */
static void benchmarkSequence(const char *title, char *cmd, int len, int seqlen) {
    client c;

    config.title = title;
    config.requests_issued = 0;
    config.requests_finished = 0;
    config.previous_requests_finished = 0;
    config.last_printed_bytes = 0;
    hdr_init(CONFIG_LATENCY_HISTOGRAM_MIN_VALUE,         // Minimum value
             CONFIG_LATENCY_HISTOGRAM_MAX_VALUE,         // Maximum value
             config.precision,                           // Number of significant figures
             &config.latency_histogram);                 // Pointer to initialise
    hdr_init(CONFIG_LATENCY_HISTOGRAM_MIN_VALUE,         // Minimum value
             CONFIG_LATENCY_HISTOGRAM_INSTANT_MAX_VALUE, // Maximum value
             config.precision,                           // Number of significant figures
             &config.current_sec_latency_histogram);     // Pointer to initialise

    initPlaceholders(cmd, len);
    if (config.num_threads) initBenchmarkThreads();

    long long search_memory = 0;
    long long search_reclaimable = 0;
    long long search_total_docs = 0;
    long long search_ingest_field_vector = 0;
    long long search_background_indexing_status = 0;
    long long after_search_memory = 0;
    long long after_search_reclaimable = 0;
    long long after_search_total_docs = 0;
    long long after_search_ingest_field_vector = 0;
    long long after_search_background_indexing_status = 0;
    clusterSnapshot* before_search_info = NULL;
    clusterSnapshot* before_ftinfo = NULL;
    clusterSnapshot* before_info_all = NULL;
    clusterSnapshot* after_search_info = NULL;
    clusterSnapshot* after_ftinfo = NULL;
    clusterSnapshot* after_info_all = NULL;
    if (config.use_search) {
        before_search_info = getSearchInfo(&search_memory, &search_reclaimable, &search_total_docs,
                            &search_ingest_field_vector, &search_background_indexing_status);
        before_ftinfo = getFtInfoStatistics(config.search.name);
        before_info_all = getInfoCluster();
        // printf("  Search index memory: %lld MB\n", search_memory/ (1024 * 1024));
        // printf("  Search index reclaimable: %lld MB\n", search_reclaimable/ (1024 * 1024));
        // printf("  Search total documents: %lld\n", search_total_docs);
        // printf("  Search ingest field vector: %lld\n", search_ingest_field_vector);
        // printf("  Search background indexing status: %lld\n", search_background_indexing_status);

    }
    if (config.rps > 0) {
        config.time_per_token = 1000000000 / config.rps;
        config.time_per_burst = config.time_per_token * config.rps;
        config.last_time_ns = 0;
    }

    int thread_id = config.num_threads > 0 ? 0 : -1;
    c = createClient(cmd, len, seqlen, NULL, thread_id);
    createMissingClients(c);

    config.start = mstime();
    if (!config.num_threads)
        aeMain(config.el);
    else
        startBenchmarkThreads();
    config.totlatency = mstime() - config.start;
    if (config.use_search) {
        after_search_info = getSearchInfo(&after_search_memory, &after_search_reclaimable, &after_search_total_docs,
                            &after_search_ingest_field_vector, &after_search_background_indexing_status);
        after_ftinfo = getFtInfoStatistics(config.search.name);
        after_info_all = getInfoCluster();
        compareInfoSnapshots(before_info_all, after_info_all, before_ftinfo, after_ftinfo, before_search_info, after_search_info);
        if (after_search_memory != search_memory)
            printf("search_memory: %lld MB -> %lld MB\n", search_memory / (1024 * 1024), after_search_memory / (1024 * 1024));
        if (after_search_reclaimable != search_reclaimable)
            printf("search_reclaimable: %lld MB -> %lld MB\n", search_reclaimable / (1024 * 1024), after_search_reclaimable / (1024 * 1024));
        if (after_search_total_docs != search_total_docs)
            printf("search_total_docs: %lld -> %lld\n", search_total_docs, after_search_total_docs);
        if (after_search_ingest_field_vector != search_ingest_field_vector)
            printf("search_ingest_field_vector: %lld -> %lld\n", search_ingest_field_vector, after_search_ingest_field_vector);
        if (after_search_background_indexing_status != search_background_indexing_status)
            printf("search_background_indexing_status: %lld -> %lld\n", search_background_indexing_status, after_search_background_indexing_status);

    }
    showLatencyReport();
    freeAllClients();
    if (config.threads) freeBenchmarkThreads();
    if (config.current_sec_latency_histogram) hdr_close(config.current_sec_latency_histogram);
    if (config.latency_histogram) hdr_close(config.latency_histogram);
}

/* Benchmark a single RESP-encoded command of length len. */
static void benchmark(const char *title, char *cmd, int len) {
    benchmarkSequence(title, cmd, len, 1);
}

/* Thread functions. */

static benchmarkThread *createBenchmarkThread(int index) {
    benchmarkThread *thread = zcalloc(sizeof(*thread));
    if (thread == NULL) return NULL;
    thread->index = index;
    thread->el = aeCreateEventLoop(1024 * 10);
    thread->paused_clients = listCreate();
    aeCreateTimeEvent(thread->el, 1, showThroughput, (void *)thread, NULL);
    return thread;
}

static void freeBenchmarkThread(benchmarkThread *thread) {
    if (thread->el) aeDeleteEventLoop(thread->el);
    listRelease(thread->paused_clients);
    zfree(thread);
}

static void freeBenchmarkThreads(void) {
    int i = 0;
    for (; i < config.num_threads; i++) {
        benchmarkThread *thread = config.threads[i];
        if (thread) freeBenchmarkThread(thread);
    }
    zfree(config.threads);
    config.threads = NULL;
}

static void *execBenchmarkThread(void *ptr) {
    benchmarkThread *thread = (benchmarkThread *)ptr;
    aeMain(thread->el);
    return NULL;
}

/* Cluster helper functions. */

static clusterNode *createClusterNode(char *ip, int port) {
    clusterNode *node = zcalloc(sizeof(*node));
    if (!node) return NULL;
    node->ip = ip;
    node->port = port;
    node->name = NULL;
    node->flags = 0;
    node->replicate = NULL;
    node->replicas_count = 0;
    node->slots = zcalloc(CLUSTER_SLOTS * sizeof(int));
    node->slots_count = 0;
    node->updated_slots = NULL;
    node->updated_slots_count = 0;
    node->server_config = NULL;
    return node;
}

static void freeClusterNode(clusterNode *node) {
    if (node->name) sdsfree(node->name);
    if (node->replicate) sdsfree(node->replicate);
    if (node->ctx) valkeyFree(node->ctx);
    /* If the node is not the reference node, that uses the address from
     * config.conn_info.hostip and config.conn_info.hostport, then the node ip has been
     * allocated by fetchClusterConfiguration, so it must be freed. */
    if (node->ip && strcmp(node->ip, config.conn_info.hostip) != 0) sdsfree(node->ip);
    if (node->server_config != NULL) freeServerConfig(node->server_config);
    zfree(node->slots);
    zfree(node);
}

static void freeClusterNodes(void) {
    int i = 0;
    for (; i < config.cluster_node_count; i++) {
        clusterNode *n = config.cluster_nodes[i];
        if (n) freeClusterNode(n);
    }
    zfree(config.cluster_nodes);
    zfree(config.cluster_primary_nodes);
    config.cluster_nodes = NULL;
    config.cluster_primary_nodes = NULL;
}

static clusterNode **addClusterNode(clusterNode *node, int selected) {
    printf("Adding cluster node %s %s:%d\n", node->name, node->ip, node->port);    
    // verify node ip + port is unique
    for (int i = 0; i < config.cluster_node_count; i++) {
        clusterNode *n = config.cluster_nodes[i];
        if (strcmp(n->ip, node->ip) == 0 && n->port == node->port) {
            printf("Node %s:%d already exists, skipping name=%s, replicate=%s <==> n_name=%s, n_replicate=%s\n", node->ip, node->port, node->name, node->replicate, n->name, n->replicate);
            freeClusterNode(node);
            return config.cluster_nodes;
        }
    }
    int count = config.cluster_node_count + 1;
    config.cluster_nodes = zrealloc(config.cluster_nodes, count * sizeof(clusterNode *));
    assert(config.cluster_nodes != NULL);
    node->ctx = getValkeyContext(config.ct, node->ip, node->port);
    config.cluster_nodes[config.cluster_node_count++] = node;
    
    if (node->replicate == NULL) {
        printf("Adding cluster primary node %s:%d\n", node->ip, node->port);
        config.cluster_primary_nodes = zrealloc(config.cluster_primary_nodes, (config.cluster_primary_node_count + 1) * sizeof(clusterNode *));
        config.cluster_primary_nodes[config.cluster_primary_node_count++] = node;
    }
    if (selected) {
        config.selected_nodes = zrealloc(config.selected_nodes, (config.selected_node_count + 1) * sizeof(clusterNode *));
        config.selected_nodes[config.selected_node_count++] = node;
    }
    return config.cluster_nodes;
}

int isElastiCacheEndpoint(const char *hostname) {
    /* Must contain ElastiCache domain markers */
    if (strstr(hostname, ".cache.amazonaws.com") == NULL) {
        return 0;
    }
    return 1;
}
/* CMD (Cluster Mode Disabled) configuration prototypes */
static int fetchCMDNodesConfiguration(void);
static int setupElastiCacheCMDNodes(void);
static sds constructElastiCacheReaderEndpoint(const char *hostname);
static int setupOpenSourceCMDPrimary(valkeyReply *role_reply);
static int setupOpenSourceCMDReplica(valkeyReply *role_reply);
// static serverConfig *getServerConfigSafe(enum valkeyConnectionType ct, const char *host, int port);

/**
 * Fetch nodes configuration for Cluster Mode Disabled (CMD) setup.
 * Handles open-source Valkey (via ROLE) and AWS ElastiCache (reader endpoint synthesis).
 * 
 * For ElastiCache CMD: creates primary + single reader endpoint node (load-balanced across replicas).
 * For open-source: creates primary + individual replica nodes from ROLE output.
 * 
 * Returns 1 on success, 0 on failure.
 */
static int fetchCMDNodesConfiguration(void) {
    int success = 1;
    valkeyContext *ctx = NULL;
    valkeyReply *reply = NULL;
    assert(!isElastiCacheEndpoint(config.conn_info.hostip));
    ctx = config.conn_ctx;
    if (ctx == NULL) {
        fprintf(stderr, "No existing connection context, creating new\n");
        ctx = getValkeyContext(config.ct, config.conn_info.hostip, config.conn_info.hostport);
        if (ctx == NULL) {
            exit(1);
        }
    }

    /* Detect ElastiCache by trying ROLE first */
    reply = valkeyCommand(ctx, "ROLE");
    if (reply == NULL || ctx->err || reply->type != VALKEY_REPLY_ARRAY || reply->elements < 1) {        
        success = 0;
        /* ROLE failed - try ElastiCache reader endpoint synthesis */
        printf("ROLE command failed, assuming ElastiCache endpoint and trying reader endpoint synthesis\n");
        goto cleanup;
    }
    printf("Detected open-source Valkey endpoint, using ROLE output for replicas\n");    
    char *role = reply->element[0]->str;
    if (strcmp(role, "master") == 0) {
        success = setupOpenSourceCMDPrimary(reply);
    } else if (strcmp(role, "slave") == 0) {
        success = setupOpenSourceCMDReplica(reply);
    } else {
        success = 0;
    }

cleanup:
    if (reply) freeReplyObject(reply);
    
    if (!success && config.cluster_nodes) {
        freeClusterNodes();
    }
    
    return success;
}

/**
 * Setup nodes for ElastiCache CMD by synthesizing reader endpoint.
 * Pattern: <primary-host> → <primary-host with -ro inserted before .ng. or domain>
 * 
 * ElastiCache exposes replicas via DNS load-balanced reader endpoint, not individual IPs.
 */
static int setupElastiCacheCMDNodes(void) {
    /* Add primary node */
    clusterNode *primary = createClusterNode((char *)config.conn_info.hostip, 
                                              config.conn_info.hostport);
    if (!primary) return 0;
    
    primary->name = sdsnew("primary");
    for (int slot = 0; slot < CLUSTER_SLOTS; slot++) {
        primary->slots[primary->slots_count++] = slot;
    }

    if (!addClusterNode(primary, isSelected(1))) {
        freeClusterNode(primary);
        return 0;
    }
    
    /* Synthesize reader endpoint if ElastiCache pattern detected */
    sds reader_hostname = constructElastiCacheReaderEndpoint(config.conn_info.hostip);
    if (reader_hostname == NULL) {
        printf("No ElastiCache reader endpoint pattern detected in hostname %s, skipping\n",
                config.conn_info.hostip);
        /* Not an ElastiCache endpoint pattern - no reader to add */
        return 1;
    }
    
    /* Verify reader endpoint is reachable */
    valkeyContext *test_ctx = valkeyConnect(reader_hostname, config.conn_info.hostport);
    if (test_ctx == NULL || test_ctx->err) {
        fprintf(stderr, "WARNING: Synthesized reader endpoint %s:%d unreachable, skipping\n",
                reader_hostname, config.conn_info.hostport);
        sdsfree(reader_hostname);
        if (test_ctx) valkeyFree(test_ctx);
        exit(1);
        // return 1; /* Non-fatal - primary still usable */
    }
    valkeyFree(test_ctx);
    
    /* Add reader node */
    clusterNode *reader = createClusterNode(reader_hostname, config.conn_info.hostport);
    if (!reader) {
        fprintf(stderr, "ERROR: Failed to create reader node for %s:%d\n",
                reader_hostname, config.conn_info.hostport);
        sdsfree(reader_hostname);
        fflush(stderr);
        exit(1);
        // sdsfree(reader_hostname);
        // return 0;
    }
    
    reader->name = sdsnew("reader-endpoint");
    reader->flags = 1; /* Mark as replica endpoint */
    reader->replicate = sdsnew(primary->name);
    
    for (int slot = 0; slot < CLUSTER_SLOTS; slot++) {
        reader->slots[reader->slots_count++] = slot;
    }

    if (!addClusterNode(reader, isSelected(0))) {
        freeClusterNode(reader);
        return 0;
    }
    
    primary->replicas_count = 1; /* Logical count - reader represents N replicas */
    return 1;
}

/**
 * Construct ElastiCache reader endpoint from primary hostname.
 * 
 * Patterns:
 *   CMD: xxx.ng.0001.region.cache.amazonaws.com → xxx-ro.ng.0001.region.cache.amazonaws.com
 *   CMD: xxx.ajfdds.ng.0001.region.cache.amazonaws.com → xxx-ro.ajfdds.ng.0001.region.cache.amazonaws.com
 * 
 * Returns allocated sds with reader hostname, or NULL if pattern not detected.
 */
static sds constructElastiCacheReaderEndpoint(const char *hostname) {
    /* Must contain ElastiCache domain markers */
    
    if (!isElastiCacheEndpoint(hostname)) {
        return NULL;
    }
    /* Already a -ro endpoint */
    if (strstr(hostname, "-ro.") != NULL) {
        return NULL;
    }
    
    /* Find the first dot - this is after the cluster name prefix */
    const char *first_dot = strchr(hostname, '.');
    if (first_dot == NULL) {
        return NULL;
    }
    
    /* Insert -ro before the first dot:
     * ec-search-ec-cmd.ajfdds.ng... → ec-search-ec-cmd-ro.ajfdds.ng... */
    size_t prefix_len = first_dot - hostname;
    sds reader = sdsnewlen(hostname, prefix_len);
    reader = sdscat(reader, "-ro");
    reader = sdscat(reader, first_dot);
    
    return reader;
}
/**
 * Setup nodes for open-source Valkey CMD when connected to primary.
 * Parses ROLE response to enumerate individual replica endpoints.
 */
static int setupOpenSourceCMDPrimary(valkeyReply *role_reply) {
    /* Add primary node */
    clusterNode *primary = createClusterNode((char *)config.conn_info.hostip, 
                                              config.conn_info.hostport);
    if (!primary) return 0;
    
    primary->name = sdsnew("primary");
    for (int slot = 0; slot < CLUSTER_SLOTS; slot++) {
        primary->slots[primary->slots_count++] = slot;
    }
    
    if (!addClusterNode(primary, isSelected(1))) {
        freeClusterNode(primary);
        return 0;
    }
    
    /* Parse replicas from ROLE response: [role, repl_offset, [[ip, port, offset], ...]] */
    if (role_reply->elements >= 3 && role_reply->element[2]->type == VALKEY_REPLY_ARRAY) {
        size_t replica_count = role_reply->element[2]->elements;
        
        for (size_t i = 0; i < replica_count; i++) {
            valkeyReply *replica_info = role_reply->element[2]->element[i];
            if (replica_info->type != VALKEY_REPLY_ARRAY || replica_info->elements < 2) {
                continue;
            }
            
            char *replica_ip = replica_info->element[0]->str;
            int replica_port = (int)replica_info->element[1]->integer;
            
            clusterNode *replica = createClusterNode(sdsnew(replica_ip), replica_port);
            if (!replica) return 0;
            
            replica->name = sdscatprintf(sdsempty(), "replica-%zu", i);
            replica->flags = 1;
            replica->replicate = sdsnew(primary->name);
            
            for (int slot = 0; slot < CLUSTER_SLOTS; slot++) {
                replica->slots[replica->slots_count++] = slot;
            }
            
            if (!addClusterNode(replica, isSelected(0))) {
                freeClusterNode(replica);
                return 0;
            }
            
            primary->replicas_count++;
        }
    }
    
    return 1;
}

/**
 * Setup nodes for open-source Valkey CMD when connected to replica.
 * Parses ROLE response to find primary, then adds current replica.
 */
static int setupOpenSourceCMDReplica(valkeyReply *role_reply) {
    if (role_reply->elements < 3) return 0;
    
    char *primary_ip = role_reply->element[1]->str;
    int primary_port = (int)role_reply->element[2]->integer;
    
    /* Add primary node */
    clusterNode *primary = createClusterNode(sdsnew(primary_ip), primary_port);
    if (!primary) return 0;
    
    primary->name = sdsnew("primary");
    for (int slot = 0; slot < CLUSTER_SLOTS; slot++) {
        primary->slots[primary->slots_count++] = slot;
    }

    if (!addClusterNode(primary, isSelected(1))) {
        freeClusterNode(primary);
        return 0;
    }
    
    /* Add current replica node */
    clusterNode *replica = createClusterNode((char *)config.conn_info.hostip,
                                              config.conn_info.hostport);
    if (!replica) return 0;
    
    replica->name = sdsnew("replica-0");
    replica->flags = 1;
    replica->replicate = sdsnew(primary->name);
    
    for (int slot = 0; slot < CLUSTER_SLOTS; slot++) {
        replica->slots[replica->slots_count++] = slot;
    }
    
    if (!addClusterNode(replica, isSelected(0))) {
        freeClusterNode(replica);
        return 0;
    }
    
    primary->replicas_count = 1;
    return 1;
}

/* Fetch the cluster configuration by calling CLUSTER SLOTS and update
 * the internal representation of the cluster nodes accordingly. */
static int fetchClusterConfiguration(void) {
    int success = 1;
    valkeyContext *ctx = NULL;
    valkeyReply *reply = NULL;
    dict *nodes = NULL;
    const char *errmsg = "Failed to fetch cluster configuration";
    size_t i, j;
    ctx = config.conn_ctx;
    if (ctx == NULL) {
        fprintf(stderr, "No existing connection context, creating new\n");
        ctx = getValkeyContext(config.ct, config.conn_info.hostip, config.conn_info.hostport);
        if (ctx == NULL) {
            exit(1);
        }
    }

    reply = valkeyCommand(ctx, "CLUSTER SLOTS");
    if (reply == NULL || reply->type == VALKEY_REPLY_ERROR) {
        success = 0;
        if (reply) fprintf(stderr, "%s\nCLUSTER SLOTS ERROR: %s\n", errmsg, reply->str);
        goto cleanup;
    }
    assert(reply->type == VALKEY_REPLY_ARRAY);
    nodes = dictCreate(&dtype);
    for (i = 0; i < reply->elements; i++) {
        valkeyReply *r = reply->element[i];
        assert(r->type == VALKEY_REPLY_ARRAY);
        assert(r->elements >= 3);
        int from = r->element[0]->integer;
        int to = r->element[1]->integer;
        sds primary = NULL;
        for (j = 2; j < r->elements; j++) {
            valkeyReply *nr = r->element[j];
            assert(nr->type == VALKEY_REPLY_ARRAY && nr->elements >= 3);
            assert(nr->element[0]->str != NULL);
            assert(nr->element[2]->str != NULL);

            int is_primary = (j == 2);
            if (is_primary) primary = sdsnew(nr->element[2]->str);
            printf("Node %s:%lld is %s\n", nr->element[0]->str, nr->element[1]->integer, is_primary ? "primary" : "replica");

            sds ip = sdsnew(nr->element[0]->str);
            sds name = sdsnew(nr->element[2]->str);
            int port = nr->element[1]->integer;
            int slot_start = from;
            int slot_end = to;

            clusterNode *node = NULL;
            dictEntry *entry = dictFind(nodes, name);
            if (entry == NULL) {
                node = createClusterNode(sdsnew(ip), port);
                if (node == NULL) {
                    success = 0;
                    goto cleanup;
                } else {
                    node->name = name;
                    if (!is_primary) node->replicate = sdsdup(primary);
                }
            } else {
                node = dictGetVal(entry);
            }
            if (slot_start == slot_end) {
                node->slots[node->slots_count++] = slot_start;
            } else {
                while (slot_start <= slot_end) {
                    int slot = slot_start++;
                    node->slots[node->slots_count++] = slot;
                }
            }
            if (node->slots_count == 0) {
                fprintf(stderr, "WARNING: Node %s:%d has no slots, skipping...\n", node->ip, node->port);
                continue;
            }
            if (entry == NULL) {
                dictReplace(nodes, node->name, node);
                if (!addClusterNode(node, isSelected(is_primary))) {
                    success = 0;
                    goto cleanup;
                }
            }
        }
        sdsfree(primary);
    }
cleanup:
    if (!success) {
        if (config.cluster_nodes) freeClusterNodes();
    }
    if (reply) freeReplyObject(reply);
    if (nodes) dictRelease(nodes);
    return success;
}

/* Request the current cluster slots configuration by calling CLUSTER SLOTS
 * and atomically update the slots after a successful reply. */
static int fetchClusterSlotsConfiguration(client c) {
    UNUSED(c);
    int success = 1, is_fetching_slots = 0, last_update = 0;
    size_t i, j;

    last_update = atomic_load_explicit(&config.slots_last_update, memory_order_relaxed);
    if (c->slots_last_update < last_update) {
        c->slots_last_update = last_update;
        return -1;
    }
    valkeyReply *reply = NULL;

    is_fetching_slots = atomic_fetch_add_explicit(&config.is_fetching_slots, 1, memory_order_relaxed);
    if (is_fetching_slots) return -1; // TODO: use other codes || errno ?
    atomic_store_explicit(&config.is_fetching_slots, 1, memory_order_relaxed);
    fprintf(stderr, "WARNING: Cluster slots configuration changed, fetching new one...\n");
    const char *errmsg = "Failed to update cluster slots configuration";

    /* printf("[%d] fetchClusterSlotsConfiguration\n", c->thread_id); */
    dict *nodes = dictCreate(&dtype);
    // valkeyContext *ctx = NULL;
    for (i = 0; i < (size_t)config.cluster_node_count; i++) {
        clusterNode *node = config.cluster_nodes[i];
        assert(node->ip != NULL);
        assert(node->name != NULL);
        assert(node->port);
        /* Use first node as entry point to connect to. */
        if (node->ctx == NULL) {
            node->ctx = getValkeyContext(config.ct, node->ip, node->port);
            if (!node->ctx) {
                success = 0;
                goto cleanup;
            }
        }
        if (node->updated_slots != NULL) zfree(node->updated_slots);
        node->updated_slots = NULL;
        node->updated_slots_count = 0;
        dictReplace(nodes, node->name, node);
    }
    reply = valkeyCommand(config.cluster_nodes[0]->ctx, "CLUSTER SLOTS");
    if (reply == NULL || reply->type == VALKEY_REPLY_ERROR) {
        success = 0;
        if (reply) fprintf(stderr, "%s\nCLUSTER SLOTS ERROR: %s\n", errmsg, reply->str);
        goto cleanup;
    }
    assert(reply->type == VALKEY_REPLY_ARRAY);
    for (i = 0; i < reply->elements; i++) {
        valkeyReply *r = reply->element[i];
        assert(r->type == VALKEY_REPLY_ARRAY);
        assert(r->elements >= 3);
        int from, to, slot;
        from = r->element[0]->integer;
        to = r->element[1]->integer;
        size_t start, end;
        if (config.read_from_replica == FROM_ALL) {
            start = 2;
            end = r->elements;
        } else if (config.read_from_replica == FROM_REPLICA_ONLY) {
            start = 3;
            end = r->elements;
        } else {
            start = 2;
            end = 3;
        }

        for (j = start; j < end; j++) {
            valkeyReply *nr = r->element[j];
            assert(nr->type == VALKEY_REPLY_ARRAY && nr->elements >= 3);
            assert(nr->element[2]->str != NULL);
            sds name = sdsnew(nr->element[2]->str);
            dictEntry *entry = dictFind(nodes, name);
            if (entry == NULL) {
                success = 0;
                fprintf(stderr,
                        "%s: could not find node with ID %s in current "
                        "configuration.\n",
                        errmsg, name);
                if (name) sdsfree(name);
                goto cleanup;
            }
            sdsfree(name);
            clusterNode *node = dictGetVal(entry);
            if (node->updated_slots == NULL) node->updated_slots = zcalloc(CLUSTER_SLOTS * sizeof(int));
            for (slot = from; slot <= to; slot++) node->updated_slots[node->updated_slots_count++] = slot;
        }
    }
    updateClusterSlotsConfiguration();
cleanup:
    freeReplyObject(reply);
    // valkeyFree(ctx);
    dictRelease(nodes);
    atomic_store_explicit(&config.is_fetching_slots, 0, memory_order_relaxed);
    return success;
}

/* Atomically update the new slots configuration. */
static void updateClusterSlotsConfiguration(void) {
    pthread_mutex_lock(&config.is_updating_slots_mutex);
    atomic_store_explicit(&config.is_updating_slots, 1, memory_order_relaxed);

    int i;
    for (i = 0; i < config.cluster_node_count; i++) {
        clusterNode *node = config.cluster_nodes[i];
        if (node->updated_slots != NULL) {
            int *oldslots = node->slots;
            node->slots = node->updated_slots;
            node->slots_count = node->updated_slots_count;
            node->updated_slots = NULL;
            node->updated_slots_count = 0;
            zfree(oldslots);
        }
    }
    atomic_store_explicit(&config.is_updating_slots, 0, memory_order_relaxed);
    atomic_fetch_add_explicit(&config.slots_last_update, 1, memory_order_relaxed);
    pthread_mutex_unlock(&config.is_updating_slots_mutex);
}

/* Generate random data for the benchmark. See #7196. */
static void genBenchmarkRandomData(char *data, int count) {
    static uint32_t state = 1234;
    int i = 0;

    while (count--) {
        state = (state * 1103515245 + 12345);
        data[i++] = '0' + ((state >> 16) & 63);
    }
}

/* Parse tag distributions from command line */
static void parseTagDistributions(const char *distributions_str) {
    sds str = sdsnew(distributions_str);
    int count = 0;
    char *token;
    double cumulative = 0.0;
    
    /* First pass: count distributions */
    sds temp = sdsdup(str);
    char *saveptr;
    token = strtok_r(temp, ",", &saveptr);
    while (token) {
        count++;
        token = strtok_r(NULL, ",", &saveptr);
    }
    sdsfree(temp);
    
    /* Allocate array */
    config.search.curr_conf.tag_dists = zcalloc(sizeof(tagDistribution) * count);
    config.search.curr_conf.n_dists = count;
    
    /* Second pass: parse distributions */
    int i = 0;
    token = strtok_r(str, ",", &saveptr);
    while (token) {
        char *colon = strchr(token, ':');
        if (!colon) {
            fprintf(stderr, "Invalid tag distribution format: %s\n", token);
            exit(1);
        }
        
        *colon = '\0';
        char *tag = token;
        double percentage = atof(colon + 1);
        
        cumulative += percentage;
        config.search.curr_conf.tag_dists[i].pattern = sdsnew(tag);
        config.search.curr_conf.tag_dists[i].percentage = percentage;
        config.search.curr_conf.tag_dists[i].cumulative = cumulative;
        
        i++;
        token = strtok_r(NULL, ",", &saveptr);
    }
    
    sdsfree(str);
    
    /* Note: Percentages are independent probabilities, they don't need to sum to 100% */
    /* Each percentage represents the probability that tag will be included */
}

/* Select tags based on independent probabilities - each tag has its own probability */
static sds selectTagByDistribution(void) {
    if (!config.search.curr_conf.tag_dists || config.search.curr_conf.n_dists == 0) {
        return NULL;
    }
    
    sds tags = sdsempty();
    int first = 1;
    
    /* Each tag has an independent probability of being included */
    for (int i = 0; i < config.search.curr_conf.n_dists; i++) {
        double random_percent = ((double)rand() / RAND_MAX) * 100.0;
        
        /* Include this tag if random falls within its percentage */
        if (random_percent <= config.search.curr_conf.tag_dists[i].percentage) {
            /* Process pattern with placeholders */
            sds tag = sdsdup(config.search.curr_conf.tag_dists[i].pattern);
            
            /* Replace __rand_int__ placeholder if present */
            if (strstr(tag, "__rand_int__")) {
                char rand_str[32];
                snprintf(rand_str, sizeof(rand_str), "%d", rand() % 1000000);
                char *pos = strstr(tag, "__rand_int__");
                if (pos) {
                    sds prefix = sdsnewlen(tag, pos - tag);
                    sds suffix = sdsnew(pos + strlen("__rand_int__"));
                    sdsfree(tag);
                    tag = sdscatprintf(prefix, "%s%s", rand_str, suffix);
                    sdsfree(suffix);
                }
            }
            
            /* Add tag to list with comma separator */
            if (!first) {
                tags = sdscat(tags, ",");
            }
            tags = sdscat(tags, tag);
            sdsfree(tag);
            first = 0;
        }
    }
    
    /* Return NULL if no tags selected, otherwise return tag list */
    if (sdslen(tags) == 0) {
        sdsfree(tags);
        return NULL;
    }
    
    return tags;
}

void setDefaultSearchConfig(void) {
    config.search.name = sdsnew("test_vector_index");
    config.search.prefix = sdsnew("vec:");
    config.search.vector_field = sdsnew("vector_field");
    config.search.vector_dim = 128; // Default vector dimension
    config.search.ef_construction = 200; // Default EF Construction
    config.search.ef_search = 200; // Default EF Search
    config.search.m = 16; // Default HNSW M parameter
    config.search.tag_field = NULL; // No tag field by default
    config.search.numeric_field = NULL; // No numeric field by default
    config.search.k = 10; // Default K for KNN queries
    config.search.curr_conf.tag_dists = NULL;
    config.search.curr_conf.n_dists = 0;
    config.search.curr_conf.tag_filter = NULL;
    config.search.metric = sdsnew("L2");
    config.search.algorithm = sdsnew("hnsw"); // Default algorithm
    config.search.nocontent = 1; // exclude content by default
}
/* Returns number of consumed options. */
int parseOptions(int argc, char **argv) {
    int i;
    int lastarg;
    int exit_status = 1;
    char *tls_usage;
    char *rdma_usage;

    for (i = 1; i < argc; i++) {
        lastarg = (i == (argc - 1));

        if (!strcmp(argv[i], "-c")) {
            if (lastarg) goto invalid;
            config.numclients = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "-v") || !strcmp(argv[i], "--version")) {
            sds version = cliVersion();
            printf("valkey-benchmark %s\n", version);
            sdsfree(version);
            exit(0);
        } else if (!strcmp(argv[i], "-n")) {
            if (lastarg) goto invalid;
            config.requests = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "-k")) {
            if (lastarg) goto invalid;
            config.keepalive = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "-h")) {
            if (lastarg) goto invalid;
            sdsfree(config.conn_info.hostip);
            config.conn_info.hostip = sdsnew(argv[++i]);
        } else if (!strcmp(argv[i], "-p")) {
            if (lastarg) goto invalid;
            config.conn_info.hostport = atoi(argv[++i]);
            if (config.conn_info.hostport < 0 || config.conn_info.hostport > 65535) {
                fprintf(stderr, "Invalid server port.\n");
                exit(1);
            }
        } else if (!strcmp(argv[i], "-s")) {
            if (lastarg) goto invalid;
            sdsfree(config.conn_info.hostip);
            config.conn_info.hostip = sdsnew(argv[++i]);
            config.ct = VALKEY_CONN_UNIX;
        } else if (!strcmp(argv[i], "-x")) {
            config.stdinarg = 1;
        } else if (!strcmp(argv[i], "-a")) {
            if (lastarg) goto invalid;
            config.conn_info.auth = sdsnew(argv[++i]);
        } else if (!strcmp(argv[i], "--user")) {
            if (lastarg) goto invalid;
            config.conn_info.user = sdsnew(argv[++i]);
        } else if (!strcmp(argv[i], "--rps")) {
            if (lastarg) goto invalid;
            config.rps = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "-u") && !lastarg) {
            parseUri(argv[++i], "valkey-benchmark", &config.conn_info, &config.tls);
            if (config.conn_info.hostport < 0 || config.conn_info.hostport > 65535) {
                fprintf(stderr, "Invalid server port.\n");
                exit(1);
            }
            config.input_dbnumstr = sdsfromlonglong(config.conn_info.input_dbnum);
        } else if (!strcmp(argv[i], "-3")) {
            config.resp3 = 1;
        } else if (!strcmp(argv[i], "-d")) {
            if (lastarg) goto invalid;
            config.datasize = atoi(argv[++i]);
            if (config.datasize < 1) config.datasize = 1;
            if (config.datasize > 1024 * 1024 * 1024) config.datasize = 1024 * 1024 * 1024;
        } else if (!strcmp(argv[i], "-P")) {
            if (lastarg) goto invalid;
            config.pipeline = atoi(argv[++i]);
            if (config.pipeline <= 0) config.pipeline = 1;
        } else if (!strcmp(argv[i], "-r")) {
            if (lastarg) goto invalid;
            const char *next = argv[++i], *p = next;
            if (*p == '-') {
                p++;
                if (*p < '0' || *p > '9') goto invalid;
            }
            config.replace_placeholders = 1;
            config.keyspacelen = atoi(next);
            if (config.keyspacelen < 0) config.keyspacelen = 0;
        } else if (!strcmp(argv[i], "--sequential")) {
            config.sequential_replacement = 1;
        } else if (!strcmp(argv[i], "-q")) {
            config.quiet = 1;
        } else if (!strcmp(argv[i], "--csv")) {
            config.csv = 1;
        } else if (!strcmp(argv[i], "-l")) {
            config.loop = 1;
        } else if (!strcmp(argv[i], "-I")) {
            config.idlemode = 1;
        } else if (!strcmp(argv[i], "-e")) {
            fprintf(stderr, "WARNING: -e option has no effect. "
                            "We now immediately exit on error to avoid false results.\n");
        } else if (!strcmp(argv[i], "--seed")) {
            if (lastarg) goto invalid;
            int rand_seed = atoi(argv[++i]);
            srandom(rand_seed);
            init_genrand64(rand_seed);
        } else if (!strcmp(argv[i], "-t")) {
            if (lastarg) goto invalid;
            /* We get the list of tests to run as a string in the form
             * get,set,lrange,...,test_N. Then we add a comma before and
             * after the string in order to make sure that searching
             * for ",testname," will always get a match if the test is
             * enabled. */
            config.tests = sdsnew(",");
            config.tests = sdscat(config.tests, (char *)argv[++i]);
            config.tests = sdscat(config.tests, ",");
            sdstolower(config.tests);
        } else if (!strcmp(argv[i], "--dbnum")) {
            if (lastarg) goto invalid;
            config.conn_info.input_dbnum = atoi(argv[++i]);
            config.input_dbnumstr = sdsfromlonglong(config.conn_info.input_dbnum);
        } else if (!strcmp(argv[i], "--precision")) {
            if (lastarg) goto invalid;
            config.precision = atoi(argv[++i]);
            if (config.precision < 0) config.precision = DEFAULT_LATENCY_PRECISION;
            if (config.precision > MAX_LATENCY_PRECISION) config.precision = MAX_LATENCY_PRECISION;
        } else if (!strcmp(argv[i], "--threads")) {
            if (lastarg) goto invalid;
            config.num_threads = atoi(argv[++i]);
            if (config.num_threads > MAX_THREADS) {
                fprintf(stderr, "WARNING: Too many threads, limiting threads to %d.\n", MAX_THREADS);
                config.num_threads = MAX_THREADS;
            } else if (config.num_threads < 0)
                config.num_threads = 0;
        } else if (!strcmp(argv[i], "--cluster")) {
            config.cluster_mode = 1;
        } else if (!strcmp(argv[i], "--rfr")) {
            if (argv[++i]) {
                if (!strcmp(argv[i], "all")) {
                    config.read_from_replica = FROM_ALL;
                } else if (!strcmp(argv[i], "yes")) {
                    config.read_from_replica = FROM_REPLICA_ONLY;
                } else if (!strcmp(argv[i], "no")) {
                    config.read_from_replica = FROM_PRIMARY_ONLY;
                } else {
                    goto invalid;
                }
            } else
                goto invalid;
        } else if (!strcmp(argv[i], "--enable-tracking")) {
            config.enable_tracking = 1;
        } else if (!strcmp(argv[i], "--search")) {
            // TODO: Is search is enabled and -t is not, do not run default tests
            config.use_search = 1;
        } else if (!strcmp(argv[i], "--search-print-results")) {
            config.print_search_results = 1;          
        } else if (!strcmp(argv[i], "--search-prefix")) {
            if (lastarg) goto invalid;
            if (config.search.prefix) sdsfree(config.search.prefix);
            config.search.prefix = sdsnew(argv[++i]);
        } else if (!strcmp(argv[i], "--vector-field")) {
            if (lastarg) goto invalid;
            if (config.search.vector_field) sdsfree(config.search.vector_field);
            config.search.vector_field = sdsnew(argv[++i]);
        } else if (!strcmp(argv[i], "--search-name")) {
            if (lastarg) goto invalid;
            if (config.search.name) sdsfree(config.search.name);
            config.search.name = sdsnew(argv[++i]);
        } else if (!strcmp(argv[i], "--vector-dim")) {
            if (lastarg) goto invalid;
            config.search.vector_dim = atoi(argv[++i]);
            if (config.search.vector_dim <= VECTOR_NUM_RAND_DIM) {
                fprintf(stderr, "Invalid vector dimension: %d\n", config.search.vector_dim);
                goto invalid;
            }
        } else if (!strcmp(argv[i], "--ef-search")) {
            if (lastarg) goto invalid;
            config.search.ef_search = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "--ef-construction")) {
            if (lastarg) goto invalid;
            config.search.ef_construction = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "--m")) {
            if (lastarg) goto invalid;
            config.search.m = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "--tag-field")) {
            if (lastarg) goto invalid;
            if (config.search.tag_field) sdsfree(config.search.tag_field);
            config.search.tag_field = sdsnew(argv[++i]);
        } else if (!strcmp(argv[i], "--tag-filter")) {
            if (lastarg) goto invalid;
            if (config.search.curr_conf.tag_filter) sdsfree(config.search.curr_conf.tag_filter);
            config.search.curr_conf.tag_filter = sdsnew(argv[++i]);
        } else if (!strcmp(argv[i], "--search-tags")) {
            if (lastarg) goto invalid;
            parseTagDistributions(argv[++i]);
        } else if (!strcmp(argv[i], "--numeric-field")) {
            if (lastarg) goto invalid;
            if (config.search.numeric_field) sdsfree(config.search.numeric_field);
            config.search.numeric_field = sdsnew(argv[++i]);
        } else if (!strcmp(argv[i], "--search-alg")) {
            if (lastarg) goto invalid;
            if (strcmp(argv[i + 1], "hnsw") && strcmp(argv[i + 1], "flat")) {
                goto invalid;
            }
            if (config.search.algorithm) sdsfree(config.search.algorithm);
            config.search.algorithm = sdsnew(argv[++i]);
        } else if (!strcmp(argv[i], "--metric")) {            
            if (lastarg) goto invalid;
            if (strcmp(argv[i + 1], "L2") && strcmp(argv[i + 1], "IP") && strcmp(argv[i + 1], "COSINE")) {
                goto invalid;
            }
            if (config.search.metric) sdsfree(config.search.metric);
            config.search.metric = sdsnew(argv[++i]);
        } else if (!strcmp(argv[i], "--k")) {
            if (lastarg) goto invalid;
            config.search.k = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "--num-functions")) {
            config.num_functions = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "--num-keys-in-fcall")) {
            config.num_keys_in_fcall = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "--help")) {
            exit_status = 0;
            goto usage;
#ifdef USE_OPENSSL
        } else if (!strcmp(argv[i], "--tls")) {
            config.tls = 1;
        } else if (!strcmp(argv[i], "--sni")) {
            if (lastarg) goto invalid;
            config.sslconfig.sni = strdup(argv[++i]);
        } else if (!strcmp(argv[i], "--cacertdir")) {
            if (lastarg) goto invalid;
            config.sslconfig.cacertdir = strdup(argv[++i]);
        } else if (!strcmp(argv[i], "--cacert")) {
            if (lastarg) goto invalid;
            config.sslconfig.cacert = strdup(argv[++i]);
        } else if (!strcmp(argv[i], "--insecure")) {
            config.sslconfig.skip_cert_verify = 1;
        } else if (!strcmp(argv[i], "--cert")) {
            if (lastarg) goto invalid;
            config.sslconfig.cert = strdup(argv[++i]);
        } else if (!strcmp(argv[i], "--key")) {
            if (lastarg) goto invalid;
            config.sslconfig.key = strdup(argv[++i]);
        } else if (!strcmp(argv[i], "--tls-ciphers")) {
            if (lastarg) goto invalid;
            config.sslconfig.ciphers = strdup(argv[++i]);
#ifdef TLS1_3_VERSION
        } else if (!strcmp(argv[i], "--tls-ciphersuites")) {
            if (lastarg) goto invalid;
            config.sslconfig.ciphersuites = strdup(argv[++i]);
#endif
#endif
#ifdef USE_RDMA
        } else if (!strcmp(argv[i], "--rdma")) {
            if (valkeyInitiateRdma() != VALKEY_OK) {
                fprintf(stderr, "Failed to initialize RDMA support from libvalkey\n");
                exit(1);
            }
            config.ct = VALKEY_CONN_RDMA;
#endif
        } else if (!strcmp(argv[i], "--mptcp")) {
            config.mptcp = 1;
        } else if (!strcmp(argv[i], "--")) {
            /* End of options. */
            return i + 1;
        } else {
            /* Assume the user meant to provide an option when the arg starts
             * with a dash. We're done otherwise and should use the remainder
             * as the command and arguments for running the benchmark. */
            if (argv[i][0] == '-') goto invalid;
            return i;
        }
    }

    return i;

invalid:
    printf("Invalid option \"%s\" or option argument missing\n\n", argv[i]);

usage:
    tls_usage =
#ifdef USE_OPENSSL
        " --tls              Establish a secure TLS connection.\n"
        " --sni <host>       Server name indication for TLS.\n"
        " --cacert <file>    CA Certificate file to verify with.\n"
        " --cacertdir <dir>  Directory where trusted CA certificates are stored.\n"
        "                    If neither cacert nor cacertdir are specified, the default\n"
        "                    system-wide trusted root certs configuration will apply.\n"
        " --insecure         Allow insecure TLS connection by skipping cert validation.\n"
        " --cert <file>      Client certificate to authenticate with.\n"
        " --key <file>       Private key file to authenticate with.\n"
        " --tls-ciphers <list> Sets the list of preferred ciphers (TLSv1.2 and below)\n"
        "                    in order of preference from highest to lowest separated by colon (\":\").\n"
        "                    See the ciphers(1ssl) manpage for more information about the syntax of this string.\n"
#ifdef TLS1_3_VERSION
        " --tls-ciphersuites <list> Sets the list of preferred ciphersuites (TLSv1.3)\n"
        "                    in order of preference from highest to lowest separated by colon (\":\").\n"
        "                    See the ciphers(1ssl) manpage for more information about the syntax of this string,\n"
        "                    and specifically for TLSv1.3 ciphersuites.\n"
#endif
#endif
        "";

    rdma_usage =
#ifdef USE_RDMA
        " --rdma             Establish a RDMA connection.\n"
#endif
        "";


    printf(
        "%s%s%s%s%s%s", /* Split to avoid strings longer than 4095 (-Woverlength-strings). */
        "Usage: valkey-benchmark [OPTIONS] [--] [COMMAND ARGS...]\n\n"
        "Simulates sending commands using multiple clients. The utility provides a\n"
        "default set of tests. You can run a subset of the tests using the -t option or\n"
        "supply one or more custom commands on the command line.\n\n"
        "To supply multiple commands on the command line, separate them with ';' as in\n"
        "`SET foo bar ';' GET foo`. You can also prefix a command in the sequence with\n"
        "a number N to repeat the command N times. In command arguments, the following\n"
        "placeholders are substituted:\n\n"
        " __rand_int__       Replaced with a zero-padded random integer in the range\n"
        "                    selected using the -r option. Multiple occurrences within the\n"
        "                    command will have different values.\n"
        "__rand_1st__        Like __rand_int__ but multiple occurrences will have the same\n"
        "                    value. __rand_2nd__ through __rand_9th__ are also available.\n"
        " __data__           Replaced with data of the size specified by the -d option.\n"
        " {tag}              Replaced with a tag that routes the command to each node in\n"
        "                    a cluster. Include this in key names when running in cluster\n"
        "                    mode.\n"
        "\n",
        "Options:\n"
        "\n"
        " -h <hostname>      Server hostname (default 127.0.0.1)\n"
        " -p <port>          Server port (default 6379)\n"
        " -s <socket>        Server socket (overrides host and port)\n"
        " -a <password>      Password for Valkey Auth\n"
        " --user <username>  Used to send ACL style 'AUTH username pass'. Needs -a.\n"
        " -u <uri>           Server URI on format valkey://user:password@host:port/dbnum\n"
        "                    User, password and dbnum are optional. For authentication\n"
        "                    without a username, use username 'default'. For TLS, use\n"
        "                    the scheme 'valkeys'.\n"
        " -c <clients>       Number of parallel connections (default 50).\n"
        "                    Note: If --cluster is used then number of clients has to be\n"
        "                    the same or higher than the number of nodes.\n"
        " -n <requests>      Total number of requests (default 100000)\n"
        " -d <size>          Data size of SET/GET value in bytes (default 3)\n"
        " --dbnum <db>       SELECT the specified db number (default 0)\n"
        " -3                 Start session in RESP3 protocol mode.\n"
        " --threads <num>    Enable multi-thread mode.\n"
        " --cluster          Enable cluster mode.\n"
        "                    If the command is supplied on the command line in cluster\n"
        "                    mode, the key must contain \"{tag}\". Otherwise, the\n"
        "                    command will not be sent to the right cluster node.\n"
        " --rfr <mode>       Enable read from replicas in cluster mode.\n"
        "                    This command must be used with the --cluster option.\n"
        "                    There are three modes for reading from replicas:\n"
        "                    'no' - sends read requests to primaries only (default) \n"
        "                    'yes' - sends read requests to replicas only.\n"
        "                    'all' - sends read requests to all nodes.\n"
        "                    Since write commands will not be accepted by replicas,\n"
        "                    it is recommended to enable read from replicas only for read\n"
        "                    command tests.\n"
        " --enable-tracking  Send CLIENT TRACKING on before starting benchmark.\n"
        " -k <boolean>       1=keep alive 0=reconnect (default 1)\n"
        " -r <keyspacelen>   Use random keys for SET/GET/INCR, random values for SADD,\n"
        "                    random members and scores for ZADD.\n"
        "                    Using this option the benchmark will replace the string\n"
        "                    __rand_int__ inside an argument with a random 12 digit\n"
        "                    number in the specified range from 0 to keyspacelen-1. The\n"
        "                    substitution changes every time a command is executed.\n"
        "                    Default tests use this to hit random keys in the specified\n"
        "                    range.\n"
        "                    Note: If -r is omitted, all commands in a benchmark will\n"
        "                    use the same key.\n"
        " --sequential       Modifies the -r argument to replace the string __rand_int__\n"
        "                    with 12 digit numbers sequentially instead of randomly.\n"
        "                    __rand_1st__ through __rand_9th__ are available with independent\n"
        "                    counters. Used to create expected number of elements with multiple\n"
        "                    replacements.\n"
        "                    example: ZADD myzset __rand_int__ element:__rand_1st__\n"
        " -P <numreq>        Pipeline <numreq> requests. That is, send multiple requests\n"
        "                    before waiting for the replies. Default 1 (no pipeline).\n"
        "                    When multiple commands are specified on the command line,\n"
        "                    then the full command sequence counts as one and -P controls\n"
        "                    the number of times the command sequence is sent in each\n"
        "                    pipeline.\n",
        " -q                 Quiet. Just show query/sec values\n"
        " --precision        Number of decimal places to display in latency output (default 0)\n"
        " --csv              Output in CSV format\n"
        " -l                 Loop. Run the tests forever\n"
        " -t <tests>         Only run the comma separated list of tests. The test\n"
        "                    names are the same as the ones produced as output.\n"
        "                    The -t option is ignored if a specific command is supplied\n"
        "                    on the command line.\n"
        " -I                 Idle mode. Just open N idle connections and wait.\n"
        " -x                 Read last argument from STDIN.\n"
        " --rps <requests>   Limit the total number of requests per second. Default 0 (no limit)\n"
        " --seed <num>       Set the seed for random number generator. Default seed is based on time.\n"
        " --num-functions <num>\n"
        "                    Sets the number of functions present in the Lua lib that is\n"
        "                    loaded when running the 'function_load' test. (default 10).\n"
        " --num-keys-in-fcall <num>\n"
        "                    Sets the number of keys passed to FCALL command when running\n"
        "                    the 'fcall' test. (default 1)\n"
        " --search           Enable search indexes for vec-insert, vec-query, and vec-del tests.\n"
        "                    Creates a vector index when starting benchmarks.\n"
        " --search-print-results Print the search results returned by FT.SEARCH queries.\n"
        " --ef-search <value> Set the EF_RUNTIME parameter for KNN queries. (default 200)\n"
        " --vector-dim <dim> Set the dimension of the vector index. Dim must be > 16. (default 128)\n"
        " --ef-construction <value> Set the EF_CONSTRUCTION parameter for KNN queries. (default 200)\n"
        " --m <value>        Set the HNSW M parameter for KNN queries. (default 16)\n"
        " --search-alg <name> Set the search algorithm to use for KNN queries. (default 'hnsw')\n"
        "                    Supported algorithms: 'hnsw', 'flat'.\n"
        " --metric <name>    Set the metric for KNN queries. (default 'L2')\n"
        "                    Supported metrics: 'L2', 'IP', 'COSINE'\n"
        " --k <value>       Set the number of nearest neighbors to return in KNN queries. (default 10)\n"
        " --search-name <name> Set the name of the search index to use for vec-query and vec-del tests.\n"
        "                    If not set, the default index name 'test_vector_index' is used.\n"
        " --search-prefix <prefix>\n"
        "                    Set the prefix for vector keys. (default 'vec:')\n"
        " --vector-field <name>\n"
        "                    Set the name for vector values. (default 'vector_field')\n"
        " --tag-field <name> Set the tag field name for the index.\n"
        " --tag-filter <pattern>\n"
        "                    Set tag filter pattern for vec-query operations (e.g., 'category_*').\n"
        " --search-tags <distribution>\n"
        "                    Comma-separated tag:percentage pairs for vec-insert operations.\n"
        "                    Example: 'fruits:8.5,vegetables:7.2,dairy:32.1,meat:52.2'\n",
        tls_usage,
        rdma_usage,        
        " --mptcp            Enable an MPTCP connection.\n"
        " --help             Output this help and exit.\n"
        " --version          Output version and exit.\n\n"
        "Examples:\n\n"
        " Run the benchmark with the default configuration against 127.0.0.1:6379:\n"
        "   $ valkey-benchmark\n\n"
        " Use 20 parallel clients, for a total of 100k requests, against 192.168.1.1:\n"
        "   $ valkey-benchmark -h 192.168.1.1 -p 6379 -n 100000 -c 20\n\n"
        " Fill 127.0.0.1:6379 with about 1 million keys only using the SET test:\n"
        "   $ valkey-benchmark -t set -n 1000000 -r 100000000\n\n"
        " Benchmark 127.0.0.1:6379 for a few commands producing CSV output:\n"
        "   $ valkey-benchmark -t ping,set,get -n 100000 --csv\n\n"
        " Benchmark a specific command line:\n"
        "   $ valkey-benchmark -r 10000 -n 10000 eval 'return server.call(\"ping\")' 0\n\n"
        " Fill a list with 10000 random elements:\n"
        "   $ valkey-benchmark -r 10000 -n 10000 lpush mylist __rand_int__\n\n"
        " Benchmark a specific transaction:\n"
        "   $ valkey-benchmark -- multi ';' set key:__rand_int__ __data__ ';' \\\n"
        "                         incr counter ';' exec\n\n"
        " Search index tests:\n"
        "   $ valkey-benchmark --search  --search-name grocery_products --vector-dim 768 "
        "--tag-field \"category\" --search-tags 'fruits:100,vegetables:100,dairy:100,meat:52.2,fruitsppo:99,fruitsppod:99' -t vec-insert -n 100 -r 1000\n"
        " Query and filter vector data:\n"
        "   $ valkey-benchmark --search  --search-name grocery_products     --vector-dim 768     --tag-field \"category\"\n"
         "--search-tags 'fruits:5.7,vegetables:0.3,dairy:10.1,meat:52.2,fruitsppo:99,fruitsppod:99' --tag-filter 'fruits*'\n"
         "   -t vec-query  --search-print-results   -n 1 -r 10000000\n\n"
        " For more information, see the Valkey documentation at https://valkey.io.\n");
    exit(exit_status);
}

long long showThroughput(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    UNUSED(eventLoop);
    UNUSED(id);
    benchmarkThread *thread = (benchmarkThread *)clientData;
    int liveclients = atomic_load_explicit(&config.liveclients, memory_order_relaxed);
    int requests_finished = atomic_load_explicit(&config.requests_finished, memory_order_relaxed);
    int previous_requests_finished = atomic_load_explicit(&config.previous_requests_finished, memory_order_relaxed);
    long long current_tick = mstime();

    if (liveclients == 0 && requests_finished != config.requests) {
        fprintf(stderr, "All clients disconnected... aborting.\n");
        exit(1);
    }
    if (config.num_threads && requests_finished >= config.requests) {
        aeStop(eventLoop);
        return AE_NOMORE;
    }
    if (config.csv) return SHOW_THROUGHPUT_INTERVAL;
    /* only first thread output throughput */
    if (thread != NULL && thread->index != 0) {
        return SHOW_THROUGHPUT_INTERVAL;
    }
    if (config.idlemode == 1) {
        printf("clients: %d\r", config.liveclients);
        fflush(stdout);
        return SHOW_THROUGHPUT_INTERVAL;
    }
    const float dt = (float)(current_tick - config.start) / 1000.0;
    const float rps = (float)requests_finished / dt;
    const float instantaneous_dt = (float)(current_tick - config.previous_tick) / 1000.0;
    const float instantaneous_rps = (float)(requests_finished - previous_requests_finished) / instantaneous_dt;
    config.previous_tick = current_tick;
    atomic_store_explicit(&config.previous_requests_finished, requests_finished, memory_order_relaxed);
    printf("%*s\r", config.last_printed_bytes, " "); /* ensure there is a clean line */
    int printed_bytes =
        printf("%s: rps=%.1f (overall: %.1f) avg_msec=%.3f (overall: %.3f)\r", config.title, instantaneous_rps, rps,
               hdr_mean(config.current_sec_latency_histogram) / 1000.0f, hdr_mean(config.latency_histogram) / 1000.0f);
    config.last_printed_bytes = printed_bytes;
    hdr_reset(config.current_sec_latency_histogram);
    fflush(stdout);
    return SHOW_THROUGHPUT_INTERVAL;
}

char *generateFunctionScript(uint32_t num_functions, int with_keys) {
    /* 64K buffer to hold script code */
    const size_t buffer_len = 64 * 1024;
    char *buffer = zcalloc(buffer_len);
    memset(buffer, 0, buffer_len);

    int written = snprintf(buffer, buffer_len, "#!lua name=benchlib\n");
    while (num_functions > 0 && (buffer_len - written) > 0) {
        assert(buffer_len - written > 0);
        int n = 0;
        if (with_keys) {
            n = snprintf(buffer + written, buffer_len - written,
                         "local function foo%u(keys, args)\nreturn keys[0]\nend\n",
                         num_functions);
        } else {
            n = snprintf(buffer + written, buffer_len - written,
                         "local function foo%u()\nreturn 0\nend\n",
                         num_functions);
        }

        if (n < 0 || (size_t)n >= buffer_len - written) {
            break;
        }
        written += n;

        n = snprintf(buffer + written, buffer_len - written,
                     "server.register_function('foo%u', foo%u)\n",
                     num_functions,
                     num_functions);
        written += n;

        num_functions--;
    }

    return buffer;
}

/* Return true if the named test was selected using the -t command line
 * switch, or if all the tests are selected (no -t passed by user). */
int test_is_selected(const char *name) {
    char buf[256];
    int l = strlen(name);

    if (config.tests == NULL) return 1;
    buf[0] = ',';
    memcpy(buf + 1, name, l);
    buf[l + 1] = ',';
    buf[l + 2] = '\0';
    return strstr(config.tests, buf) != NULL;
}

int main(int argc, char **argv) {
    int i;
    char *data, *cmd, *tag;
    int len;
    memset(&config, 0, sizeof(config));
    client c;

    srandom(time(NULL) ^ getpid());
    init_genrand64(ustime() ^ getpid());
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);

    config.ct = VALKEY_CONN_TCP;
    config.numclients = 50;
    config.requests = 100000;
    config.liveclients = 0;
    config.el = aeCreateEventLoop(1024 * 10);
    aeCreateTimeEvent(config.el, 1, showThroughput, NULL, NULL);
    config.keepalive = 1;
    config.datasize = 3;
    config.pipeline = 1;
    config.replace_placeholders = 1;
    config.keyspacelen = 0;
    config.sequential_replacement = 0;
    config.quiet = 0;
    config.csv = 0;
    config.loop = 0;
    config.idlemode = 0;
    config.clients = listCreate();
    config.paused_clients = listCreate();
    config.conn_info.hostip = sdsnew("127.0.0.1");
    config.conn_info.hostport = 6379;
    config.use_search = 0;
    config.print_search_results = 0;
    config.tests = NULL;
    config.conn_info.input_dbnum = 0;
    config.stdinarg = 0;
    config.conn_info.auth = NULL;
    config.precision = DEFAULT_LATENCY_PRECISION;
    config.num_threads = 0;
    config.threads = NULL;
    config.cluster_mode = 0;
    config.rps = 0;
    config.read_from_replica = FROM_ALL;
    config.cluster_node_count = 0;
    config.cluster_nodes = NULL;
    config.server_config = NULL;
    config.is_fetching_slots = 0;
    config.is_updating_slots = 0;
    config.slots_last_update = 0;
    config.enable_tracking = 0;
    config.num_functions = 10;
    config.num_keys_in_fcall = 1;
    config.resp3 = 0;
    resetPlaceholders();
    setDefaultSearchConfig();
    i = parseOptions(argc, argv);
    argc -= i;
    argv += i;

    tag = "";

#ifdef USE_OPENSSL
    if (config.tls) {
        cliSecureInit();
    }
#endif
  
    /* Initialize base vector */
    initBaseVector(config.search.vector_dim);
    
    if (config.mptcp && (config.ct != VALKEY_CONN_TCP)) {
        fprintf(stderr, "Options --mptcp is only supported by TCP\n");
        exit(1);
    }

    if (config.cluster_mode) {
        // We only include the slot placeholder {tag} if cluster mode is enabled
        tag = ":{tag}";

        /* Fetch cluster configuration. */
        if (!fetchClusterConfiguration() || !config.cluster_nodes) {
            if (config.ct != VALKEY_CONN_UNIX) {
                fprintf(stderr,
                        "Failed to fetch cluster configuration from "
                        "%s:%d\n",
                        config.conn_info.hostip, config.conn_info.hostport);
            } else {
                fprintf(stderr,
                        "Failed to fetch cluster configuration from "
                        "%s\n",
                        config.conn_info.hostip);
            }
            exit(1);
        }
        if (config.cluster_node_count == 0) {
            fprintf(stderr, "Invalid cluster: %d node(s).\n", config.cluster_node_count);
            exit(1);
        }       
    } else if (isElastiCacheEndpoint(config.conn_info.hostip)) {
        int res = setupElastiCacheCMDNodes();
        if (!res) {
            fprintf(stderr,
                    "Failed to fetch cluster configuration from "
                    "%s:%d\n",
                    config.conn_info.hostip, config.conn_info.hostport);
            exit(1);
        }
    } else {
        int res = fetchCMDNodesConfiguration();
        if (!res) {
            if (config.ct != VALKEY_CONN_UNIX) {
                fprintf(stderr,
                        "Failed to fetch cluster configuration from "
                        "%s:%d\n",
                        config.conn_info.hostip, config.conn_info.hostport);
            } else {
                fprintf(stderr,
                        "Failed to fetch cluster configuration from "
                        "%s\n",
                        config.conn_info.hostip);
            }
            exit(1);
        }
        // config.server_config = getServerConfig(config.ct, config.conn_info.hostip, config.conn_info.hostport);
        // if (config.server_config == NULL) {
        //     fprintf(stderr, "WARNING: Could not fetch server CONFIG\n");
        // }
    }
    const char *node_roles = NULL;
    if (config.read_from_replica == FROM_ALL) {
        node_roles = "cluster";
    } else if (config.read_from_replica == FROM_REPLICA_ONLY) {
        node_roles = "replica";
    } else {
        node_roles = "primary";
    }
    printf("Cluster has %d %s nodes:\n\n", config.cluster_node_count, node_roles);
    i = 0;
    for (; i < config.cluster_node_count; i++) {
        clusterNode *node = config.cluster_nodes[i];
        if (!node) {
            fprintf(stderr, "Invalid cluster node #%d\n", i);
            exit(1);
        }
        const char *node_type = (node->replicate == NULL ? "Primary" : "Replica");
        printf("Node %d(%s): ", i, node_type);
        if (node->name) printf("%s ", node->name);
        printf("%s:%d\n", node->ip, node->port);
        safeGetServerConfig(config.ct, node->ip, node->port, node->server_config);
        if (node->server_config == NULL) {
            fprintf(stderr, "WARNING: Could not fetch node CONFIG %s:%d\n", node->ip, node->port);
        }
    }
    printf("\n");
    /* Automatically set thread number to node count if not specified
        * by the user. */
    if (config.num_threads == 0) config.num_threads = config.cluster_node_count;
    if (config.num_threads > 0) {
        pthread_mutex_init(&(config.liveclients_mutex), NULL);
        pthread_mutex_init(&(config.is_updating_slots_mutex), NULL);
    }

    if (config.keepalive == 0) {
        fprintf(stderr, "WARNING: Keepalive disabled. You probably need "
                        "'echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse' for Linux and "
                        "'sudo sysctl -w net.inet.tcp.msl=1000' for Mac OS X in order "
                        "to use a lot of clients/requests\n");
    }
    if (argc > 0 && config.tests != NULL) {
        fprintf(stderr, "WARNING: Option -t is ignored.\n");
    }

    if (config.idlemode) {
        printf("Creating %d idle connections and waiting forever (Ctrl+C when done)\n", config.numclients);
        int thread_id = -1, use_threads = (config.num_threads > 0);
        if (use_threads) {
            thread_id = 0;
            initBenchmarkThreads();
        }
        c = createClient("", 0, 1, NULL, thread_id); /* will never receive a reply */
        createMissingClients(c);
        if (use_threads)
            startBenchmarkThreads();
        else
            aeMain(config.el);
        /* and will wait for every */
    }
    if (config.csv) {
        printf("\"test\",\"rps\",\"avg_latency_ms\",\"min_latency_ms\",\"p50_latency_ms\",\"p95_latency_ms\",\"p99_"
               "latency_ms\",\"max_latency_ms\"\n");
    }
    /* Run benchmark with command in the remainder of the arguments. */
    if (argc) {
        sds title = sdsnew(argv[0]);
        for (i = 1; i < argc; i++) {
            title = sdscatlen(title, " ", 1);
            title = sdscatlen(title, (char *)argv[i], strlen(argv[i]));
        }
        sds *sds_args = getSdsArrayFromArgv(argc, argv, 0);
        if (!sds_args) {
            fprintf(stderr, "Invalid quoted string\n");
            return 1;
        }
        if (config.stdinarg) {
            sds_args = sds_realloc(sds_args, (argc + 1) * sizeof(sds));
            sds_args[argc] = readArgFromStdin();
            argc++;
        }
        /* Setup argument length */
        size_t *argvlen = zcalloc(argc * sizeof(size_t));
        for (i = 0; i < argc; i++) argvlen[i] = sdslen(sds_args[i]);
        /* RESP-encode the command(s) given on the syntax
         *
         *     [N] command args [ ";" [N] command args [...] ]
         */
        int start = 0;   /* Argument index where the current command starts. */
        int repeat = 1;  /* Number of times to repeat the current command. */
        int seq_len = 0; /* Total number of commands in the sequence. */
        sds cmd_seq = sdsempty();
        for (i = 0; i <= argc; i++) {
            if (i == start && sds_args[i][0] >= '1' && sds_args[i][0] <= '9') {
                /* Command prefixed by number means repeat command N times. */
                repeat = atoi(sds_args[i]);
                start++;
            } else if (i == argc || strcmp(";", sds_args[i]) == 0) {
                cmd = NULL;
                if (i == start) continue;
                /* End of command. RESP-encode and append to sequence. */
                len = valkeyFormatCommandArgv(&cmd, i - start,
                                              (const char **)sds_args + start,
                                              argvlen + start);
                for (int j = 0; j < repeat; j++) {
                    cmd_seq = sdscatlen(cmd_seq, cmd, len);
                }
                seq_len += repeat;
                free(cmd);
                start = i + 1;
                repeat = 1;
            } else if (strstr(sds_args[i], "__data__")) {
                /* Replace data placeholders with data of length given by -d. */
                int num_parts;
                sds *parts = sdssplitlen(sds_args[i], sdslen(sds_args[i]),
                                         "__data__", strlen("__data__"),
                                         &num_parts);
                sds newarg = parts[0];
                parts[0] = NULL; /* prevent it from being freed below */
                for (int j = 1; j < num_parts; j++) {
                    char data[config.datasize];
                    genBenchmarkRandomData(data, config.datasize);
                    newarg = sdscatlen(newarg, data, config.datasize);
                    newarg = sdscatlen(newarg, parts[j], sdslen(parts[j]));
                }
                sdsfreesplitres(parts, num_parts);
                sdsfree(sds_args[i]);
                sds_args[i] = newarg;
                argvlen[i] = sdslen(sds_args[i]);
            }
        }
        len = sdslen(cmd_seq);
        /* adjust the datasize to the parsed command */
        config.datasize = len;
        do {
            benchmarkSequence(title, cmd_seq, len, seq_len);
        } while (config.loop);
        sdsfree(cmd_seq);
        sdsfreesplitres(sds_args, argc);

        sdsfree(title);
        if (config.server_config != NULL) freeServerConfig(config.server_config);
        zfree(argvlen);
        return 0;
    }
    if (config.use_search) {
        printf("Using search indexes for the benchmark.\n");
        createDefaultSearchIndexes();
    }
    /* Run default benchmark suite. */
    data = zcalloc(config.datasize + 1);
    do {
        genBenchmarkRandomData(data, config.datasize);
        data[config.datasize] = '\0';

        if (test_is_selected("ping_inline") || test_is_selected("ping")) benchmark("PING_INLINE", "PING\r\n", 6);

        if (test_is_selected("ping_mbulk") || test_is_selected("ping")) {
            len = valkeyFormatCommand(&cmd, "PING");
            benchmark("PING_MBULK", cmd, len);
            free(cmd);
        }

        if (test_is_selected("set")) {
            len = valkeyFormatCommand(&cmd, "SET key%s:__rand_int__ %s", tag, data);
            benchmark("SET", cmd, len);
            free(cmd);
        }

        if (test_is_selected("get")) {
            len = valkeyFormatCommand(&cmd, "GET key%s:__rand_int__", tag);
            benchmark("GET", cmd, len);
            free(cmd);
        }

        if (test_is_selected("incr")) {
            len = valkeyFormatCommand(&cmd, "INCR counter%s:__rand_int__", tag);
            benchmark("INCR", cmd, len);
            free(cmd);
        }

        if (test_is_selected("lpush")) {
            len = valkeyFormatCommand(&cmd, "LPUSH mylist%s %s", tag, data);
            benchmark("LPUSH", cmd, len);
            free(cmd);
        }

        if (test_is_selected("rpush")) {
            len = valkeyFormatCommand(&cmd, "RPUSH mylist%s %s", tag, data);
            benchmark("RPUSH", cmd, len);
            free(cmd);
        }

        if (test_is_selected("lpop")) {
            len = valkeyFormatCommand(&cmd, "LPOP mylist%s", tag);
            benchmark("LPOP", cmd, len);
            free(cmd);
        }

        if (test_is_selected("rpop")) {
            len = valkeyFormatCommand(&cmd, "RPOP mylist%s", tag);
            benchmark("RPOP", cmd, len);
            free(cmd);
        }

        if (test_is_selected("sadd")) {
            len = valkeyFormatCommand(&cmd, "SADD myset%s element:__rand_int__", tag);
            benchmark("SADD", cmd, len);
            free(cmd);
        }

        if (test_is_selected("hset")) {
            len = valkeyFormatCommand(&cmd, "HSET myhash%s element:__rand_int__ %s", tag, data);
            benchmark("HSET", cmd, len);
            free(cmd);
        }
        if (config.use_search) {
            if (test_is_selected("vec-insert")) {
                /* Use custom vector benchmark function */
                len = createVectorInsertCmdTemplate(&cmd);
                benchmark("VEC-INSERT", cmd, len);
                free(cmd);
            }

            if (test_is_selected("vec-query")) {                
                /* Use custom vector benchmark function */
                len = createSearchCmdTemplate(&cmd);
                benchmark("VEC-QUERY", cmd, len);
                free(cmd);
            }

            if (test_is_selected("vec-del")) {
                sds key = getVectorKey();
                len = valkeyFormatCommand(&cmd, "DEL %s", key);
                benchmark("VEC-DEL", cmd, len);
                sdsfree(key);
                free(cmd);
            }
        }
        if (test_is_selected("spop")) {
            len = valkeyFormatCommand(&cmd, "SPOP myset%s", tag);
            benchmark("SPOP", cmd, len);
            free(cmd);
        }

        if (test_is_selected("zadd")) {
            char *score = "0";
            if (config.replace_placeholders) score = "__rand_int__";
            len = valkeyFormatCommand(&cmd, "ZADD myzset%s %s element:__rand_1st__", tag, score);
            benchmark("ZADD", cmd, len);
            free(cmd);
        }

        if (test_is_selected("zpopmin")) {
            len = valkeyFormatCommand(&cmd, "ZPOPMIN myzset%s", tag);
            benchmark("ZPOPMIN", cmd, len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_100") || test_is_selected("lrange_300") ||
            test_is_selected("lrange_500") || test_is_selected("lrange_600")) {
            len = valkeyFormatCommand(&cmd, "LPUSH mylist%s %s", tag, data);
            benchmark("LPUSH (needed to benchmark LRANGE)", cmd, len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_100")) {
            len = valkeyFormatCommand(&cmd, "LRANGE mylist%s 0 99", tag);
            benchmark("LRANGE_100 (first 100 elements)", cmd, len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_300")) {
            len = valkeyFormatCommand(&cmd, "LRANGE mylist%s 0 299", tag);
            benchmark("LRANGE_300 (first 300 elements)", cmd, len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_500")) {
            len = valkeyFormatCommand(&cmd, "LRANGE mylist%s 0 499", tag);
            benchmark("LRANGE_500 (first 500 elements)", cmd, len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_600")) {
            len = valkeyFormatCommand(&cmd, "LRANGE mylist%s 0 599", tag);
            benchmark("LRANGE_600 (first 600 elements)", cmd, len);
            free(cmd);
        }

        if (test_is_selected("mset")) {
            const char *cmd_argv[21];
            cmd_argv[0] = "MSET";
            sds key_placeholder = sdscatprintf(sdsnew(""), "key%s:__rand_int__", tag);
            for (i = 1; i < 21; i += 2) {
                cmd_argv[i] = key_placeholder;
                cmd_argv[i + 1] = data;
            }
            len = valkeyFormatCommandArgv(&cmd, 21, cmd_argv, NULL);
            benchmark("MSET (10 keys)", cmd, len);
            free(cmd);
            sdsfree(key_placeholder);
        }

        if (test_is_selected("mget")) {
            const char *cmd_argv[11];
            cmd_argv[0] = "MGET";
            sds key_placeholder = sdscatprintf(sdsnew(""), "key%s:__rand_int__", tag);
            for (i = 1; i < 11; i++) {
                cmd_argv[i] = key_placeholder;
            }
            len = valkeyFormatCommandArgv(&cmd, 11, cmd_argv, NULL);
            benchmark("MGET (10 keys)", cmd, len);
            free(cmd);
            sdsfree(key_placeholder);
        }

        if (test_is_selected("xadd")) {
            len = valkeyFormatCommand(&cmd, "XADD mystream%s * myfield %s", tag, data);
            benchmark("XADD", cmd, len);
            free(cmd);
        }

        if (test_is_selected("function_load")) {
            char *script = generateFunctionScript(config.num_functions, 0);
            len = valkeyFormatCommand(&cmd, "function load replace %s", script);
            benchmark("FUNCTION LOAD", cmd, len);
            zfree(script);
            free(cmd);
        }

        if (test_is_selected("fcall")) {
            char *script = generateFunctionScript(1, config.num_keys_in_fcall > 0);

            valkeyContext* ctx = config.conn_ctx;
            if (ctx == NULL) {
                fprintf(stderr, "No existing connection context, creating new\n");
                ctx = getValkeyContext(config.ct, config.conn_info.hostip, config.conn_info.hostport);
                if (ctx == NULL) {
                    exit(1);
                }
            }

            assert(ctx != NULL && ctx->err == 0);
            void *reply = valkeyCommand(ctx, "FUNCTION LOAD REPLACE %s", script);

            assert(reply != NULL);
            freeReplyObject(reply);
            zfree(script);

            char **cmd_argv = zcalloc(sizeof(char *) * (config.num_keys_in_fcall + 3));
            int ret = asprintf(&(cmd_argv[0]), "fcall");
            UNUSED(ret);
            ret = asprintf(&(cmd_argv[1]), "foo1");
            UNUSED(ret);
            ret = asprintf(&(cmd_argv[2]), "%d", config.num_keys_in_fcall);
            UNUSED(ret);
            for (int i = 0; i < config.num_keys_in_fcall; i++) {
                ret = asprintf(&(cmd_argv[3 + i]), "key%d", i + 1);
                UNUSED(ret);
            }
            len = valkeyFormatCommandArgv(&cmd, config.num_keys_in_fcall + 3, (const char **)cmd_argv, NULL);
            for (int i = 0; i < config.num_keys_in_fcall + 3; i++) {
                free(cmd_argv[i]);
            }
            zfree(cmd_argv);

            benchmark("FCALL", cmd, len);
            free(cmd);
        }

        if (!config.csv) printf("\n");
    } while (config.loop);

    zfree(data);
    freeCliConnInfo(config.conn_info);
    if (config.server_config != NULL) freeServerConfig(config.server_config);
    if (base_vector != NULL) zfree(base_vector);
    resetPlaceholders();

    return 0;
}
