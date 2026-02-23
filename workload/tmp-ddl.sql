BEGIN;

-- CREATE VECTOR INDEX partsupp_wiki_hnsw ON partsupp_vector (ps_text_embedding) WITH (distance=l2, type=hnsw, lib=vsag) PARALLEL 32;

CREATE VECTOR INDEX part_wiki_hnsw ON part_vector (text_embedding) WITH (distance=l2, type=hnsw, lib=vsag) PARALLEL 32;

COMMIT;

